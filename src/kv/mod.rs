mod entry;

use crate::KvStoreErr;
use crate::Result;
use entry::IndexEntry;
use std::ffi::OsStr;
use std::fs;
use std::fs::remove_file;
use std::fs::rename;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::{collections::HashMap, path::PathBuf};

use self::entry::HintEntry;
use self::entry::LogEntry;
use self::entry::SerializeToBytes;

const DELETED_CODE: u8 = 255;
const DEFAULT_LOG_FILE_MAX_BYTES: u64 = 1024;
const DEFAULT_MERGE_TRIGGER_THRESHOLD: u64 = 1024;

pub struct KvStore {
    index: HashMap<String, IndexEntry>,
    base_dir: PathBuf,
    active_file_id: u64,
    active_file_writer: BufWriterWithPos<File>,
    file_reader: HashMap<u64, BufReaderWithPos<File>>,
    useless_value_bytes: u64,
    log_file_max_bytes: u64,
    merge_trigger_threshold: u64,
}

impl KvStore {
    /// The function set a key and value to `KvStore`.
    /// # Example
    /// ``` rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::open();
    /// store.set("lcy".to_string(), "2002".to_string());
    /// assert_eq!(Some("2002".to_owned()), store.get("lcy".to_owned()));
    /// assert_eq!(None, store.get("cmj".to_owned()));
    ///
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();
        let k_size = key_bytes.len() as u64;
        let v_size = value_bytes.len() as u64;
        let log_entry = LogEntry {
            k_size: k_size,
            v_size: v_size,
            key: Vec::from(key_bytes),
            value: Vec::from(value_bytes),
        };
        // serialize to bytes
        let buf: Vec<u8> = log_entry.serialize();
        self.write_and_flush(&buf)?;
        // generate index entry
        let index_entry = IndexEntry {
            file_id: self.active_file_id,
            v_pos: self.active_file_writer.pos,
            v_size: value_bytes.len() as u64,
        };
        if let Some(old_entry) = self.index.insert(key, index_entry) {
            self.useless_value_bytes += old_entry.v_size;
            if self.useless_value_bytes > self.merge_trigger_threshold {
                self.merge()?;
            }
        }
        Ok(())
    }

    /// The function is to get a `value` for a `key`, return none if input `key` is not exists.
    /// # Example
    /// ``` rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// store.set("lcy".to_string(), "2002".to_string());
    /// assert_eq!(Some("2002".to_owned()), store.get("lcy".to_owned()));
    /// assert_eq!(None, store.get("cmj".to_owned()));
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // find in index
        if let Some(index_entry) = self.index.get(&key) {
            if let Some(reader) = self.file_reader.get_mut(&index_entry.file_id) {
                reader.seek(SeekFrom::Start(index_entry.v_pos - index_entry.v_size))?;
                let mut taker = reader.take(index_entry.v_size);
                let mut buf: [u8; 255] = [0; 255];
                taker.read(&mut buf[..])?;
                Ok(Some(String::from_utf8(
                    buf[..(index_entry.v_size as usize)].to_vec(),
                )?))
            } else {
                Err(KvStoreErr::InnerErr("get file reader".to_string()))
            }
        } else {
            // not exists
            Ok(None)
        }
    }

    /// The function remove a key-value pair from `KvStore`.
    /// # Example
    /// ``` rust
    /// use std::collections::HashMap;
    ///
    /// let mut map: HashMap<String, String> = HashMap::new();
    /// map.insert(String::from("a"), String::from("1"));
    /// map.insert(String::from("b"), String::from("2"));
    /// assert_eq!(Some(&String::from("1")), map.get(&String::from("a")));
    /// println!("{}", map.remove(&String::from("a")).unwrap());
    ///
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        // find in index
        if let Some(_) = self.index.get(&key) {
            // write new log entry as remove
            let log_entry = LogEntry {
                k_size: key.as_bytes().len() as u64,
                v_size: 1,
                key: key.as_bytes().to_vec(),
                value: [DELETED_CODE; 1].to_vec(),
            };
            let buf = log_entry.serialize();
            self.write_and_flush(&buf)?;
            if let Some(old_entry) = self.index.remove(&key) {
                self.useless_value_bytes += old_entry.v_size + 1;
                if self.useless_value_bytes > self.merge_trigger_threshold {
                    self.merge()?;
                }
            }

            Ok(())
        } else {
            // not exists
            Err(KvStoreErr::KeyNotFound(key))
        }
    }

    fn write_and_flush(&mut self, buf: &[u8]) -> Result<()> {
        let size = buf.len() as u64;
        if self.active_file_writer.pos + size > self.log_file_max_bytes {
            // check out new active file writer
            self.create_new_log_file()?;
        }
        self.active_file_writer.write(buf)?;
        self.active_file_writer.flush()?;
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path_buf: PathBuf = path.into();
        fs::create_dir_all(path_buf.as_path())?;
        let log_id_list = get_all_sorted_log_file_id(path_buf.as_path())?;
        let mut index: HashMap<String, IndexEntry> = HashMap::new();
        let mut file_reader = HashMap::<u64, BufReaderWithPos<File>>::new();
        let mut useless_value_bytes: u64 = 0;
        for id in &log_id_list {
            let mut reader = gen_file_reader_with_pos(&path_buf, *id, "log", &mut opt_open_r())?;
            let hint_file_path = log_path(&path_buf, *id, "hint");
            if hint_file_path.exists() {
                load_from_hint_file(
                    *id,
                    &mut gen_file_reader_with_pos(&path_buf, *id, "hint", &mut opt_open_r())?,
                    &mut index,
                )?;
            } else {
                useless_value_bytes += load_from_log_file(*id, &mut reader, &mut index)?;
            }
            file_reader.insert(*id, reader);
        }
        let active_file_writer: BufWriterWithPos<File>;
        let active_file_id;
        if log_id_list.len() == 0 {
            // now data is empty
            // create first log file
            active_file_id = 0;
            active_file_writer =
                gen_file_writer_with_pos(&path_buf, active_file_id, "log", &mut opt_create_r_w())?;
            file_reader.insert(
                active_file_id,
                gen_file_reader_with_pos(&path_buf, active_file_id, "log", &mut opt_open_r())?,
            );
        } else {
            let active_id = log_id_list.get(log_id_list.len() - 1).unwrap();
            active_file_id = *active_id;
            active_file_writer =
                gen_file_writer_with_pos(&path_buf, active_file_id, "log", &mut opt_open_r_w())?;
        }

        let kv = KvStore {
            index: index,
            base_dir: path_buf,
            active_file_id: active_file_id,
            active_file_writer: active_file_writer,
            file_reader: file_reader,
            useless_value_bytes,
            log_file_max_bytes: DEFAULT_LOG_FILE_MAX_BYTES,
            merge_trigger_threshold: DEFAULT_MERGE_TRIGGER_THRESHOLD,
        };
        Ok(kv)
    }

    fn create_new_log_file(&mut self) -> Result<()> {
        self.active_file_id += 1;
        self.active_file_writer = gen_file_writer_with_pos(
            &self.base_dir,
            self.active_file_id,
            "log",
            &mut opt_create_r_w(),
        )?;
        self.file_reader.insert(
            self.active_file_id,
            gen_file_reader_with_pos(
                &self.base_dir,
                self.active_file_id,
                "log",
                &mut opt_open_r(),
            )?,
        );
        Ok(())
    }

    pub fn merge(&mut self) -> Result<()> {
        let ids = get_all_sorted_log_file_id(&self.base_dir)?;
        let old_log_file_ids = &ids[..ids.len() - 1];
        let mut merged_log_file_id = 0;
        let (mut log_writer, mut hint_writer) =
            gen_merge_process_writer_pair(&self.base_dir, merged_log_file_id)?;

        // merge old log files and generate merged old log files and hint files
        for id in old_log_file_ids {
            let mut reader =
                gen_file_reader_with_pos(&self.base_dir, *id, "log", &mut opt_open_r())?;
            while let Ok(Some((log_entry, pos))) = read_log_entry(&mut reader) {
                if let Some(value) = self.index.get(&String::from_utf8(log_entry.key.clone())?) {
                    // this log is up to date
                    if value.file_id == *id && value.v_pos == pos {
                        let log_vec = log_entry.serialize();
                        if log_vec.len() as u64 + log_writer.pos > self.log_file_max_bytes {
                            // if log file size reach out log file max bytes
                            // flush
                            log_writer.flush()?;
                            hint_writer.flush()?;
                            merged_log_file_id += 1;
                            (log_writer, hint_writer) =
                                gen_merge_process_writer_pair(&self.base_dir, merged_log_file_id)?;
                        }
                        log_writer.write(&log_entry.serialize())?;
                        // write hint entry into hint file
                        let hint_entry = HintEntry {
                            k_size: log_entry.k_size,
                            v_size: log_entry.v_size,
                            v_pos: log_writer.pos,
                            key: log_entry.key.clone(),
                        };
                        hint_writer.write(&hint_entry.serialize())?;
                    } else {
                        // this log has been expired
                        self.useless_value_bytes -= log_entry.v_size;
                    }
                } else {
                    // this log has been deleted
                    self.useless_value_bytes -= 1;
                }
            }
        }
        log_writer.flush()?;
        hint_writer.flush()?;

        // remove old log files and reader
        for id in old_log_file_ids {
            let path = log_path(&self.base_dir, *id, "log");
            remove_file(&path)?;
            // remove reader
            self.file_reader.remove(id);
        }

        // update
        for id in 0..=merged_log_file_id {
            // rename log file and hint file
            let temp_log_file_path = log_path(&self.base_dir, id, "log.temp");
            let log_file_path = log_path(&self.base_dir, id, "log");
            rename(&temp_log_file_path, &log_file_path)?;
            let temp_hint_file_path = log_path(&self.base_dir, id, "hint.temp");
            let hint_file_path = log_path(&self.base_dir, id, "hint");
            rename(&temp_hint_file_path, &hint_file_path)?;

            // add merged log file reader in mem
            let log_reader =
                gen_file_reader_with_pos(&self.base_dir, id, "log", &mut opt_open_r())?;
            self.file_reader.insert(id, log_reader);

            // update index by loading hint file
            let mut reader =
                gen_file_reader_with_pos(&self.base_dir, id, "hint", &mut opt_open_r())?;
            load_from_hint_file(id, &mut reader, &mut self.index)?;
        }
        Ok(())
    }
}

fn opt_create_r_w() -> OpenOptions {
    OpenOptions::new()
        .append(true)
        .create(true)
        .read(true)
        .write(true)
        .to_owned()
}

fn opt_open_r_w() -> OpenOptions {
    OpenOptions::new().read(true).write(true).to_owned()
}

fn opt_open_r() -> OpenOptions {
    OpenOptions::new().read(true).to_owned()
}

fn gen_merge_process_writer_pair(
    base_path: &Path,
    id: u64,
) -> Result<(BufWriterWithPos<File>, BufWriterWithPos<File>)> {
    let log_writer = gen_file_writer_with_pos(base_path, id, "log.temp", &mut opt_create_r_w())?;
    let hint_writer = gen_file_writer_with_pos(base_path, id, "hint.temp", &mut opt_create_r_w())?;
    Ok((log_writer, hint_writer))
}

fn gen_file_writer_with_pos(
    base_path: &Path,
    id: u64,
    extension: &str,
    opt: &mut OpenOptions,
) -> Result<BufWriterWithPos<File>> {
    Ok(BufWriterWithPos::new(
        opt.open(log_path(base_path, id, extension))?,
    )?)
}

fn gen_file_reader_with_pos(
    base_path: &Path,
    id: u64,
    extension: &str,
    opt: &mut OpenOptions,
) -> Result<BufReaderWithPos<File>> {
    Ok(BufReaderWithPos::new(
        opt.open(log_path(base_path, id, extension))?,
    )?)
}

/// Load index entry and replay it to update index
/// Return useless value bytes
fn load_from_log_file(
    file_id: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut HashMap<String, IndexEntry>,
) -> Result<u64> {
    reader.seek(SeekFrom::Start(0))?;
    let mut useless_value_bytes: u64 = 0;
    while let Ok(Some((log_entry, pos))) = read_log_entry(reader) {
        if log_entry.value.len() == 1 && log_entry.value[0] == DELETED_CODE {
            // this key mark as deleted
            if let Some(old_entry) = index.remove(&String::from_utf8(log_entry.key)?) {
                // entry represents the deleted also occupy 1 bytes in value slot
                useless_value_bytes += old_entry.v_size + 1;
            }
        } else {
            // update it to index
            let key = String::from_utf8(log_entry.key)?;
            if let Some(old_entry) = index.insert(
                key,
                IndexEntry {
                    file_id: file_id,
                    v_pos: pos,
                    v_size: log_entry.v_size,
                },
            ) {
                useless_value_bytes += old_entry.v_size;
            }
        }
    }
    Ok(useless_value_bytes)
}

fn load_from_hint_file(
    file_id: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut HashMap<String, IndexEntry>,
) -> Result<()> {
    reader.seek(SeekFrom::Start(0))?;
    while let Ok(Some(hint_entry)) = read_hint_entry(reader) {
        let key = String::from_utf8(hint_entry.key)?;
        index.insert(
            key,
            IndexEntry {
                file_id: file_id,
                v_pos: hint_entry.v_pos,
                v_size: hint_entry.v_size,
            },
        );
    }
    Ok(())
}

fn read_log_entry(reader: &mut BufReaderWithPos<File>) -> Result<Option<(LogEntry, u64)>> {
    let k_size: u64;
    if let Some(k_s) = reader.read_u64() {
        k_size = k_s;
    } else {
        return Ok(None);
    }
    let v_size = reader.read_u64().unwrap();
    let mut key_buf: [u8; 255] = [0; 255];
    let mut taker = reader.take(k_size);
    taker.read(&mut key_buf)?;
    let mut value_buf: [u8; 255] = [0; 255];
    let mut taker2 = reader.take(v_size);
    taker2.read(&mut value_buf)?;
    Ok(Some((
        LogEntry {
            k_size: k_size,
            v_size: v_size,
            key: key_buf[..(k_size as usize)].to_vec(),
            value: value_buf[..(v_size as usize)].to_vec(),
        },
        reader.pos,
    )))
}

fn read_hint_entry(reader: &mut BufReaderWithPos<File>) -> Result<Option<HintEntry>> {
    let k_size: u64;
    if let Some(k_s) = reader.read_u64() {
        k_size = k_s;
    } else {
        return Ok(None);
    }

    let v_size = reader
        .read_u64()
        .expect(format!("error to read value size").as_str());

    let v_pos = reader
        .read_u64()
        .expect(format!("error to read value position").as_str());

    let mut key_buf: [u8; 255] = [0; 255];
    let mut taker = reader.take(k_size);
    taker.read(&mut key_buf)?;
    Ok(Some(HintEntry {
        k_size: k_size,
        v_size: v_size,
        v_pos: v_pos,
        key: key_buf[..(k_size as usize)].to_vec(),
    }))
}

fn log_path(base_path: &Path, id: u64, extension: &str) -> PathBuf {
    base_path.join(format!("{}.{}", id, extension))
}

fn get_all_sorted_log_file_id(path: &Path) -> Result<Vec<u64>> {
    let mut log_list: Vec<u64> = fs::read_dir(path)?
        .flat_map(|dir_entry| -> Result<_> { Ok(dir_entry?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    log_list.sort();
    Ok(log_list)
}

pub struct BufWriterWithPos<F: Write + Seek> {
    writer: BufWriter<F>,
    pos: u64,
}

impl<F: Write + Seek> BufWriterWithPos<F> {
    fn new(mut f: F) -> Result<Self> {
        let file_end_pos = f.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPos {
            writer: (BufWriter::new(f)),
            pos: file_end_pos,
        })
    }
}

impl<F: Write + Seek> Write for BufWriterWithPos<F> {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<F: Write + Seek> Seek for BufWriterWithPos<F> {
    fn seek(&mut self, pos: SeekFrom) -> std::result::Result<u64, std::io::Error> {
        let len = self.writer.seek(pos)?;
        self.pos = len;
        Ok(len)
    }
}

pub struct BufReaderWithPos<F: Read + Seek> {
    reader: BufReader<F>,
    pos: u64,
}

impl<F: Read + Seek> BufReaderWithPos<F> {
    fn new(mut f: F) -> Result<Self> {
        let offset = f.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: (BufReader::new(f)),
            pos: offset,
        })
    }
}

impl<F: Read + Seek> Read for BufReaderWithPos<F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<F: Read + Seek> Seek for BufReaderWithPos<F> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

impl<F: Read + Seek> BufReaderWithPos<F> {
    fn read_u64(&mut self) -> Option<u64> {
        let mut buf: [u8; 8] = [0; 8];
        if let Ok(len) = self.read(&mut buf) {
            if len == 0 {
                return None;
            }
            return Some(u8_arr_to_u64(&buf));
        }
        None
    }
}

fn u8_arr_to_u64(arr: &[u8; 8]) -> u64 {
    let mut ans: u64 = 0;
    let mut offet = 56;
    for num in arr {
        let real_num: u64 = (*num as u64) << offet;
        offet = offet - 8;
        ans = ans + real_num;
    }
    ans
}

mod entry;

use crate::KvStoreErr;
use crate::Result;
use entry::IndexEntry;
use std::ffi::OsStr;
use std::fs;
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

use self::entry::LogEntry;
use self::entry::SerializeToBytes;

const DELETED_CODE: u8 = 255;

pub struct KvStore {
    index: HashMap<String, IndexEntry>,
    base_dir: PathBuf,
    active_file_id: u64,
    active_file_writer: BufWriterWithPos<File>,
    file_reader: HashMap<u64, BufReaderWithPos<File>>,
}

impl KvStore {
    /// The function set a key and value to `KvStore`.
    /// # Example
    /// ``` rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
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
        self.active_file_writer.write(&buf)?;
        self.active_file_writer.flush()?;
        // generate index entry
        let index_entry = IndexEntry {
            file_id: self.active_file_id,
            v_pos: self.active_file_writer.pos,
            v_size: value_bytes.len() as u64,
        };
        self.index.insert(key, index_entry);
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
            self.active_file_writer.write(&buf)?;
            self.active_file_writer.flush()?;
            self.index.remove(&key);

            Ok(())
        } else {
            // not exists
            Err(KvStoreErr::KeyNotFound(key))
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path_buf: PathBuf = path.into();
        fs::create_dir_all(path_buf.as_path())?;
        let log_id_list = get_all_sorted_log_file_id(path_buf.as_path())?;
        let mut index: HashMap<String, IndexEntry> = HashMap::new();
        let mut file_reader = HashMap::<u64, BufReaderWithPos<File>>::new();
        for id in &log_id_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(path_buf.as_path(), *id))?)?;
            load(*id, &mut reader, &mut index)?;
            file_reader.insert(*id, reader);
        }
        let active_file_writer: BufWriterWithPos<File>;
        let active_file_id;
        if log_id_list.len() == 0 {
            // now data is empty
            // create first log file
            active_file_id = 0;
            let first_log_path = log_path(path_buf.as_path(), active_file_id);
            active_file_writer = BufWriterWithPos::new(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .read(true)
                    .write(true)
                    .open(&first_log_path)?,
            )?;
            file_reader.insert(0, BufReaderWithPos::new(File::open(first_log_path)?)?);
        } else {
            let active_id = log_id_list.get(log_id_list.len() - 1).unwrap();
            active_file_id = *active_id;
            let active_log_file_path = log_path(path_buf.as_path(), active_file_id);
            active_file_writer = BufWriterWithPos::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .append(true)
                    .open(&active_log_file_path)?,
            )?;
        }

        let kv = KvStore {
            index: index,
            base_dir: path_buf,
            active_file_id: active_file_id,
            active_file_writer: active_file_writer,
            file_reader: file_reader,
        };
        Ok(kv)
    }

    pub fn create_new_log_file(&mut self) -> Result<()> {
        let new_log_file_path = log_path(&self.base_dir, self.active_file_id + 1);
        self.active_file_id += 1;
        self.active_file_writer = BufWriterWithPos::new(
            OpenOptions::new()
                .append(true)
                .create(true)
                .read(true)
                .write(true)
                .open(&new_log_file_path)?,
        )?;
        self.file_reader.insert(
            self.active_file_id,
            BufReaderWithPos::new(File::open(&new_log_file_path)?)?,
        );
        Ok(())
    }
}

fn load(
    file_id: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut HashMap<String, IndexEntry>,
) -> Result<()> {
    reader.seek(SeekFrom::Start(0))?;
    while let Ok(Some((log_entry, pos))) = read_log_entry(reader) {
        if log_entry.value.len() == 1 && log_entry.value[0] == DELETED_CODE {
            // this key mark as deleted
            index.remove(&String::from_utf8(log_entry.key)?);
        } else {
            // update it to index
            let key = String::from_utf8(log_entry.key)?;
            index.insert(
                key,
                IndexEntry {
                    file_id: file_id,
                    v_pos: pos,
                    v_size: log_entry.v_size,
                },
            );
        }
    }
    Ok(())
}

fn read_log_entry(reader: &mut BufReaderWithPos<File>) -> Result<Option<(LogEntry, u64)>> {
    let mut k_size_buf: [u8; 8] = [0; 8];
    let len = reader.read(&mut k_size_buf)?;
    if len == 0 {
        return Ok(None);
    }
    let k_size: u64 = u8_arr_to_u64(&k_size_buf);
    let mut v_size_buf: [u8; 8] = [0; 8];
    reader.read(&mut v_size_buf)?;
    let v_size: u64 = u8_arr_to_u64(&v_size_buf);
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

fn log_path(base_path: &Path, id: u64) -> PathBuf {
    base_path.join(format!("{}.log", id))
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

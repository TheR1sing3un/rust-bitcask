use crate::Result;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

pub struct BufWriterWithPos<F: Write + Seek> {
    writer: BufWriter<F>,
    pub pos: u64,
}

impl<F: Write + Seek> BufWriterWithPos<F> {
    pub fn new(mut f: F) -> Result<Self> {
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
    pub pos: u64,
}

impl<F: Read + Seek> BufReaderWithPos<F> {
    pub fn new(mut f: F) -> Result<Self> {
        let offset = f.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: (BufReader::new(f)),
            pos: offset,
        })
    }

    pub fn read_u64(&mut self) -> Option<u64> {
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

pub fn u8_arr_to_u64(arr: &[u8; 8]) -> u64 {
    let mut ans: u64 = 0;
    let mut offet = 56;
    for num in arr {
        let real_num: u64 = (*num as u64) << offet;
        offet = offet - 8;
        ans = ans + real_num;
    }
    ans
}

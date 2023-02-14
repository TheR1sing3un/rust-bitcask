use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexEntry {
    pub file_id: u64,
    pub v_pos: u64,
    pub v_size: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub k_size: u64,
    pub v_size: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HintEntry {
    pub k_size: u64,
    pub v_size: u64,
    pub v_pos: u64,
    pub key: Vec<u8>,
}

pub trait SerializeToBytes {
    fn serialize(&self) -> Vec<u8>;
}

impl SerializeToBytes for LogEntry {
    fn serialize(&self) -> Vec<u8> {
        let mut buf: Vec<u8> =
            Vec::with_capacity(8 + 8 + self.k_size as usize + self.v_size as usize);
        buf.append(&mut self.k_size.to_be_bytes().to_vec());
        buf.append(&mut self.v_size.to_be_bytes().to_vec());
        buf.append(&mut self.key.clone());
        buf.append(&mut self.value.clone());
        buf
    }
}

impl SerializeToBytes for HintEntry {
    fn serialize(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(8 + 8 + 8 + self.k_size as usize);
        buf.append(&mut self.k_size.to_be_bytes().to_vec());
        buf.append(&mut self.v_size.to_be_bytes().to_vec());
        buf.append(&mut self.v_pos.to_be_bytes().to_vec());
        buf.append(&mut self.key.clone());
        buf
    }
}

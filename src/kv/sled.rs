use std::path::PathBuf;

use sled::Db;

use crate::{KvStoreErr, KvsEngine, Result};

struct SledEngine {
    kv: Db,
}
impl SledEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path_buf: PathBuf = path.into();
        if let Ok(kv) = sled::open(&path_buf) {
            Ok(SledEngine { kv })
        } else {
            Err(KvStoreErr::InnerErr(format!(
                "open sled engine fail, path: {:?}",
                path_buf
            )))
        }
    }
}

impl KvsEngine for SledEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.kv.insert(key, value.into_bytes())?;
        self.kv.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Ok(Some(val)) = self.kv.get(key) {
            return Ok(Some(String::from_utf8(val.to_vec())?));
        }
        return Ok(None);
    }

    fn remove(&self, key: String) -> Result<()> {
        if let None = self.kv.remove(&key)? {
            return Err(KvStoreErr::KeyNotFound(key));
        }
        return Ok(());
    }
}

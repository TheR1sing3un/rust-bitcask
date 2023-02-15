pub mod bitcask;
mod entry;
mod sled;
use super::Result;

pub trait KvsEngine: Sync + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&self, key: String) -> Result<()>;
}

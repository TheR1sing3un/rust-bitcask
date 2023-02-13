//!
//! An in-memory key-value store.
//! Use hashmap to implement the get, set and remove menthod to support basic db behavior.
//!
mod command;
mod err;
mod kv;

pub use command::Command;
pub use err::{KvStoreErr, Result};
pub use kv::KvStore;

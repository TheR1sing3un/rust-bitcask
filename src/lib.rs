//!
//! An in-memory key-value store.
//! Use hashmap to implement the get, set and remove menthod to support basic db behavior.
//!
mod client;
mod connection;
mod err;
mod io;
mod kv;
mod protocol;
mod server;

pub use client::Client;
pub use err::{KvStoreErr, Result};
pub use kv::bitcask::BitcaskEngine;
pub use kv::KvsEngine;
pub use protocol::Frame;
pub use server::Server;

use core::result;
use failure::Fail;
use std::{io, string::FromUtf8Error};

#[derive(Fail, Debug)]
pub enum KvStoreErr {
    #[fail(display = "{}", _0)]
    IOErr(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    BincodeErr(#[cause] bincode::Error),
    #[fail(display = "key {} not found", _0)]
    KeyNotFound(String),
    #[fail(display = "inner error: {}", _0)]
    InnerErr(String),
    #[fail(display = "system error: {}", _0)]
    SystemErr(#[cause] FromUtf8Error),
}

impl From<io::Error> for KvStoreErr {
    fn from(value: io::Error) -> Self {
        KvStoreErr::IOErr(value)
    }
}

impl From<bincode::Error> for KvStoreErr {
    fn from(value: bincode::Error) -> Self {
        KvStoreErr::BincodeErr(value)
    }
}

impl From<FromUtf8Error> for KvStoreErr {
    fn from(value: FromUtf8Error) -> Self {
        KvStoreErr::SystemErr(value)
    }
}

pub type Result<T> = result::Result<T, KvStoreErr>;

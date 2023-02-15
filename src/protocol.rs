use bytes::Buf;
use std::io::Cursor;
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{KvStoreErr, Result};

/// Command frame, to request and respond in c/s
///
/// Frame's format in stream: `%command%`
///
#[derive(Debug)]
pub enum Frame {
    /// Set key value command.
    /// Frame's format in stream: `%0key#value%`
    Set(String, String),
    /// Get key command.
    /// Frame's format in stream: `%1key%`
    Get(String),
    /// Remove key command.
    /// Frame's format in stream: `%2key%`
    Remove(String),
    /// Respond to client with value.
    /// Frame's format in stream: `%3value%`
    Value(String),
    /// Respond to client with error message.
    /// Frame's format in stream: `%4error_msg%`
    Error(String),
    /// Respond to client with null.
    /// Frame's format in stream: `%5%`
    Null,
}

impl Frame {
    pub async fn write(&self, writer: &mut BufWriter<TcpStream>) -> Result<()> {
        // write start separtor %
        writer.write_u8(b'%').await?;
        match self {
            Self::Set(key, value) => {
                // write code
                writer.write_u8(0).await?;

                // write key
                writer.write(key.as_bytes()).await?;

                // write #
                writer.write_u8(b'#').await?;

                // write value
                writer.write(value.as_bytes()).await?;
            }
            Self::Get(key) => {
                // write code
                writer.write_u8(1).await?;

                // write key
                writer.write(key.as_bytes()).await?;
            }
            Self::Remove(key) => {
                // write code
                writer.write_u8(2).await?;

                // write key
                writer.write(key.as_bytes()).await?;
            }
            Self::Value(value) => {
                // write code
                writer.write_u8(3).await?;

                // write value
                writer.write(value.as_bytes()).await?;
            }
            Self::Error(msg) => {
                // write code
                writer.write_u8(4).await?;

                // write value
                writer.write(msg.as_bytes()).await?;
            }
            Self::Null => {
                // write code
                writer.write_u8(5).await?;
            }
        }
        // write end separtor %
        writer.write_u8(b'%').await?;
        Ok(())
    }

    pub fn parse(buf: &mut Cursor<&[u8]>) -> Result<Frame> {
        // get start separator
        let _ = get_u8(buf)?;
        let code: u8 = get_u8(buf)?;
        match code {
            0 => {
                let key_buf = get_until_target_char(buf, b'#').unwrap();
                let key = String::from_utf8(key_buf.to_vec())?;
                let value_buf = get_until_target_char(buf, b'%').unwrap();
                let value = String::from_utf8(value_buf.to_vec())?;
                Ok(Self::Set(key, value))
            }
            1 => {
                let key_buf = get_until_target_char(buf, b'%').unwrap();
                let key = String::from_utf8(key_buf.to_vec())?;
                Ok(Self::Get(key))
            }
            2 => {
                let key_buf = get_until_target_char(buf, b'%').unwrap();
                let key = String::from_utf8(key_buf.to_vec())?;
                Ok(Self::Remove(key))
            }
            3 => {
                let key_buf = get_until_target_char(buf, b'%').unwrap();
                let key = String::from_utf8(key_buf.to_vec())?;
                Ok(Self::Value(key))
            }
            4 => {
                let msg_buf = get_until_target_char(buf, b'%').unwrap();
                let msg = String::from_utf8(msg_buf.to_vec())?;
                Ok(Self::Error(msg))
            }
            5 => {
                let _ = get_until_target_char(buf, b'%').unwrap();
                Ok(Self::Null)
            }
            _ => Err(KvStoreErr::UnexceptErr(
                "server receive unkown frame".to_owned(),
            )),
        }
    }

    pub fn check(buf: &mut Cursor<&[u8]>) -> Result<()> {
        // get start separator
        let start_separtor = get_u8(buf)?;
        if start_separtor != b'%' {
            return Err(KvStoreErr::UnexceptErr(
                "server receive wrong format frame".to_owned(),
            ));
        }
        // get end separtor
        if let Some(_) = get_until_target_char(buf, b'%') {
            return Ok(());
        }
        return Err(KvStoreErr::IncompleteErr);
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8> {
    if !src.has_remaining() {
        return Err(KvStoreErr::IncompleteErr);
    }

    Ok(src.get_u8())
}

fn get_until_target_char<'a>(buf: &mut Cursor<&'a [u8]>, char: u8) -> Option<&'a [u8]> {
    let start = buf.position() as usize;
    let end = buf.get_ref().len();
    for index in start..end {
        if buf.get_ref()[index] == char {
            buf.set_position((index + 1) as u64);
            return Some(&buf.get_ref()[start..index]);
        }
    }
    None
}

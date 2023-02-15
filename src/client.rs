use log::info;
use tokio::net::TcpStream;

use crate::{connection::Connection, Frame, KvStoreErr, Result};

pub struct Client {
    conn: Connection,
}

impl Client {
    pub fn new(socket: TcpStream) -> Self {
        Client {
            conn: Connection::new(socket),
        }
    }
}

impl Client {
    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Frame::Set(key, value);
        info!("client start to request to server with frame: {:?}", cmd);
        self.conn.write_frame(cmd).await?;
        info!("client start to read response from server");
        self.conn.read_frame().await?;
        Ok(())
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        let cmd = Frame::Get(key);
        info!("client start to request to server with frame: {:?}", cmd);
        self.conn.write_frame(cmd).await?;
        info!("client start to read response from server");
        if let Some(frame) = self.conn.read_frame().await? {
            match frame {
                Frame::Value(val) => {
                    return Ok(Some(val));
                }
                Frame::Null => {
                    return Ok(None);
                }
                Frame::Error(err) => {
                    return Err(KvStoreErr::UnexceptErr(err));
                }
                _ => {
                    return Err(KvStoreErr::UnexceptErr("invalid frame".to_owned()));
                }
            };
        }
        Ok(None)
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Frame::Remove(key);
        self.conn.write_frame(cmd).await?;
        if let Some(frame) = self.conn.read_frame().await? {
            match frame {
                Frame::Null => {
                    return Ok(());
                }
                Frame::Error(err) => {
                    return Err(KvStoreErr::UnexceptErr(err));
                }
                _ => {
                    return Err(KvStoreErr::UnexceptErr("invalid frame".to_owned()));
                }
            }
        }
        Ok(())
    }
}

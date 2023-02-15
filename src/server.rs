use std::sync::Arc;

use log::{error, info, warn};
use tokio::net::{TcpListener, TcpStream};

use crate::{connection::Connection, Frame, KvStoreErr, KvsEngine, Result};

pub struct Server<D: KvsEngine> {
    tcp: TcpListener,
    kv: Arc<D>,
}

impl<D: KvsEngine> Server<D> {
    pub async fn start(tcp: TcpListener, kv: Arc<D>) -> Result<Self> {
        let mut server = Server { tcp, kv };
        server.run().await?;
        Ok(server)
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("server start to receive connection from client");
        // receive connection
        while let (socket, _) = self.tcp.accept().await? {
            info!("server receive a connection from: {:?}", socket);
            let mut handler = Handler::new(socket, self.kv.clone());
            tokio::spawn(async move {
                if let Err(err) = handler.handle().await {
                    error!("handler handle error: {:?}", err);
                }
            });
        }
        Ok(())
    }
}

pub struct Handler<D: KvsEngine> {
    conn: Connection,
    kv: Arc<D>,
}

impl<D: KvsEngine> Handler<D> {
    pub fn new(socket: TcpStream, kv: Arc<D>) -> Self {
        Handler {
            conn: Connection::new(socket),
            kv,
        }
    }

    pub async fn handle(&mut self) -> Result<()> {
        // keep reading frame from socket, and write response to socket
        info!("handler start to handler requests from client");
        loop {
            if let Some(frame) = self.conn.read_frame().await? {
                // receive a frame
                self.deal(frame).await?;
            }
        }
    }

    pub async fn deal(&mut self, frame: Frame) -> Result<()> {
        info!("handler read a frame: {:?} from socket", frame);
        let resp = match frame {
            Frame::Set(key, value) => {
                if let Err(err) = self.kv.set(key, value) {
                    Frame::Error(err.to_string())
                } else {
                    Frame::Null
                }
            }
            Frame::Get(key) => match self.kv.get(key) {
                Ok(Some(val)) => Frame::Value(val),
                Ok(None) => Frame::Null,
                Err(err) => Frame::Error(err.to_string()),
            },
            Frame::Remove(key) => {
                if let Err(err) = self.kv.remove(key) {
                    Frame::Error(err.to_string())
                } else {
                    Frame::Null
                }
            }
            _ => {
                let msg = format!("unexcept frame received: {:?}", frame);
                warn!("{}", msg);
                return Err(KvStoreErr::UnexceptErr(msg));
            }
        };
        info!("handler write a frame: {:?} to client", resp);
        // write resp
        self.conn.write_frame(resp).await?;
        Ok(())
    }
}

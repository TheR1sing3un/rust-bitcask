use std::io::Cursor;

use log::info;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use bytes::{Buf, BytesMut};

use crate::{Frame, KvStoreErr, Result};

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(socket),
            // default 4kb buffer
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // try to parse frame from buffer
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            info!("parse frame fail, try to read from socket");
            // if parse frame but get none, means that the buffer hasn't at least one completed frame
            // try to read stream from socket
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    info!("socket is empty");
                    return Ok(None);
                }
                return Err(KvStoreErr::UnexceptErr(
                    "read an uncompleted frame".to_owned(),
                ));
            }
        }
    }

    pub async fn write_frame(&mut self, frame: Frame) -> Result<()> {
        frame.write(&mut self.stream).await?;
        self.stream.flush().await?;
        Ok(())
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);
        // check if there are completed frames in buffer
        match Frame::check(&mut buf) {
            Ok(_) => {
                // frame len
                let len = buf.position() as usize;
                buf.set_position(0);
                // parse frame from 0 to len
                let frame = Frame::parse(&mut buf)?;
                // move the cursor forward len units
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(KvStoreErr::IncompleteErr) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

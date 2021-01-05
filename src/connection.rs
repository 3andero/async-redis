use crate::protocol::{self, *};
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::*;
use tokio::net::*;

struct Connection {
    stream: BufWriter<TcpStream>,
    buf: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buf: BytesMut::new(),
        }
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            let mut buf = Bytes::from(self.buf.to_vec());
            let origin_len = buf.len();
            match protocol::Decode(&mut buf) {
                Err(FrameError::Incomplete) => {}
                Err(FrameError::Other(e)) => {
                    return Err(e);
                }
                Ok(frame) => {
                    self.buf.advance(origin_len - buf.len());
                    return Ok(Some(frame));
                }
            }

            if self.stream.read_buf(&mut self.buf).await? == 0 {
                if self.buf.len() == 0 {
                    return Ok(None);
                } else {
                    return Err("Closed Unexpectedly".into());
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> crate::Result<()> {
        let mut frame_byte = Encode(frame)?;
        self.stream
            .write_buf(&mut frame_byte)
            .await
            .map_err(|e| Box::new(e))?;

        Ok(())
    }
}

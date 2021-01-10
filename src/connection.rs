use crate::protocol::*;
use bytes::{Buf, Bytes, BytesMut};
use tokio::io::*;
use tokio::net::*;
use tracing::*;

#[derive(Debug)]
pub struct Connection {
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

    #[instrument(skip(self))]
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            let mut buf = Bytes::from(self.buf.to_vec());
            let origin_len = buf.len();
            debug!("buffer: {:?}, len: {}", &buf, origin_len);
            match Decode(&mut buf) {
                Err(FrameError::Incomplete) => {}
                Err(FrameError::Other(e)) => {
                    return Err(e);
                }
                Err(FrameError::NotImplemented) => {
                    return Err(Box::new(FrameError::NotImplemented));
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
        debug!("encoded frame_byte: {:?}", frame_byte);
        self.stream
            .write_all(&mut frame_byte)
            .await
            .map_err(|e| Box::new(e))?;

        self.stream.flush().await.map_err(|e| Box::new(e))?;
        Ok(())
    }
}

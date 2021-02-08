use crate::protocol::*;
use anyhow::{Error, Result};
use reusable_buf::ReusableBuf;
use tokio::io::*;
use tokio::net::*;
use tracing::*;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buf: ReusableBuf,
    pub id: u64,
}

impl Connection {
    pub fn new(stream: TcpStream, id: u64) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buf: ReusableBuf::new(),
            id,
        }
    }

    pub fn refresh(&mut self, stream: TcpStream, id: u64) {
        self.stream = BufWriter::new(stream);
        self.id = id;
        self.buf.reset();
    }

    pub async fn close_connection(&mut self) {
        debug!("<{}>closing previous connection", self.id);
        let _ = self.stream.shutdown().await;
    }

    // #[instrument(skip(self))]
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        let mut parser = decode::IntermediateParser::new();
        loop {
            trace!("<{}>buffer: {:?}", self.id, &self.buf);
            match parser.parse(&mut self.buf) {
                Err(FrameError::Incomplete) => {}
                Err(FrameError::Other(e)) => {
                    return Err(e);
                }
                Err(FrameError::NotImplemented) => {
                    return Err(Error::new(FrameError::NotImplemented));
                }
                Err(FrameError::Invalid(v)) => {
                    return Err(Error::new(FrameError::Invalid(v)));
                }
                Ok(frame) => {
                    return Ok(Some(frame));
                }
            }

            if self.stream.read_buf(&mut self.buf).await? == 0 {
                if self.buf.len() == 0 {
                    return Ok(None);
                } else {
                    return Err(anyhow::anyhow!("Closed Unexpectedly"));
                }
            }
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let frame_byte = encode::encode(frame)?;
        trace!("<{}>encoded frame_byte: {:?}", self.id, frame_byte);
        self.stream
            .write_all(&frame_byte)
            .await
            .map_err(|e| Box::new(e))?;

        self.stream.flush().await.map_err(|e| Box::new(e))?;
        Ok(())
    }
}

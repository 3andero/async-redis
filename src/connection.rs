use crate::protocol::*;
use anyhow::{Error, Result};
use bytes::BytesMut;
use futures::future;
use std::io::IoSlice;
use std::pin::Pin;
use tokio::io::*;
use tokio::net::*;
use tracing::*;

#[derive(Debug)]
pub struct Connection {
    // stream: BufWriter<TcpStream>,
    stream: TcpStream,
    buf: BytesMut,
    pub id: u64,
}

impl Connection {
    pub fn new(stream: TcpStream, id: u64) -> Self {
        Self {
            // stream: BufWriter::new(stream),
            stream,
            buf: BytesMut::new(),
            id,
        }
    }

    pub async fn close_connection(&mut self) {
        debug!("<{}>closing previous connection", self.id);
        let _ = self.stream.shutdown().await;
    }

    #[instrument(skip(self))]
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        let mut parser = decode::IntermediateParser::new();
        loop {
            debug!("<{}>buffer: {:?}", self.id, &self.buf);
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
        let frame_byte_arr = encode::encode(frame)?;
        debug!("<{}>encoded frame_byte: {:?}", self.id, frame_byte_arr);
        let mut bufs = Vec::with_capacity(frame_byte_arr.len());
        for frame_byte in frame_byte_arr.iter() {
            bufs.push(IoSlice::new(&frame_byte[..]));
        }

        let mut writer = Pin::new(&mut self.stream);
        future::poll_fn(|cx| writer.as_mut().poll_write_vectored(cx, &bufs[..]))
            .await
            .map_err(|e| Box::new(e))?;
        // self.stream
        //     .write_buf(&mut bufs[..])
        //     .await
        //     .map_err(|e| Box::new(e))?;

        self.stream.flush().await.map_err(|e| Box::new(e))?;
        Ok(())
    }
}

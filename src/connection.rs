use crate::protocol::*;
use anyhow::{Error, Result};
use futures::future;
use reusable_buf::ReusableBuf;
use std::io::IoSlice;
use std::pin::Pin;
use tokio::io::*;
use tokio::net::*;
use tracing::*;

#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    buf: ReusableBuf,
    pub id: u64,
}

impl Connection {
    pub fn new(stream: TcpStream, id: u64) -> Self {
        Self {
            stream,
            buf: ReusableBuf::new(),
            id,
        }
    }

    pub fn refresh(&mut self, stream: TcpStream, id: u64) {
        self.stream = stream;
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
        let x = match frame {
            Frame::NullString => Some(NIL_STRING_FRAME),
            Frame::Ok => Some(OK_FRAME),
            Frame::NullArray => Some(NIL_ARRAY_FRAME),
            _ => None,
        };
        if x.is_some() {
            self.stream
                .write_all(x.unwrap())
                .await
                .map_err(|e| Box::new(e))?;

            return Ok(());
        }
        let frame_byte_arr = encode::encode(frame)?;
        debug!("<{}>encoded frame_byte: {:?}", self.id, frame_byte_arr);
        if frame_byte_arr.len() == 1 {}
        let mut bufs = Vec::with_capacity(frame_byte_arr.len());
        for frame_byte in frame_byte_arr.iter() {
            bufs.push(IoSlice::new(&frame_byte[..]));
        }

        let mut writer = Pin::new(&mut self.stream);
        future::poll_fn(|cx| writer.as_mut().poll_write_vectored(cx, &bufs[..]))
            .await
            .map_err(|e| Box::new(e))?;

        self.stream.flush().await.map_err(|e| Box::new(e))?;
        Ok(())
    }
}


use std::vec::IntoIter;
use crate::{utils::get_integer, protocol::Frame, cmd::{ParseError, CommandError}};
use anyhow::{Error, Result};
use bytes::Bytes;

pub struct CommandParser {
    frames: IntoIter<Frame>,
    len: usize,
}

impl CommandParser {
    pub fn new(frame: Frame) -> Result<CommandParser> {
        match frame {
            Frame::Arrays(arr) => {
                let len = arr.len();
                Ok(Self {
                    frames: arr.into_iter(),
                    len,
                })
            }
            _ => Err(Error::new(ParseError::NotArray)),
        }
    }

    pub fn next(&mut self) -> Option<Frame> {
        self.len = if self.len > 0 { self.len - 1 } else { 0 };
        self.frames.next()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn next_bytes(&mut self) -> Result<Option<Bytes>> {
        let next_frame = match self.next() {
            Some(x) => x,
            None => {
                return Ok(None);
            }
        };
        match next_frame {
            Frame::SimpleString(s) | Frame::BulkStrings(s) => Ok(Some(s)),
            _ => Err(Error::new(ParseError::NotString)),
        }
    }

    pub fn next_bytes_pair(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        let p1 = match self.next_bytes()? {
            Some(b) => b,
            None => {
                return Ok(None);
            }
        };

        let p2 = match self.next_bytes()? {
            Some(b) => b,
            None => {
                return Err(Error::new(CommandError::MissingOperand));
            }
        };

        Ok(Some((p1, p2)))
    }

    pub fn next_integer(&mut self) -> Result<Option<i64>> {
        match self.next_bytes()? {
            Some(v) => get_integer(&v).map(|v| Some(v)),
            None => Ok(None),
        }
    }
}
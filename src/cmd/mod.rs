pub mod get;
use get::*;
pub mod set;
use crate::{db::DB, protocol::Frame, BytesToString};
use set::*;

use bytes::*;
use enum_dispatch::*;

#[enum_dispatch]
#[derive(Debug)]
pub enum Command {
    Get,
    Set,
}

#[enum_dispatch(Command)]
pub trait ExecDB {
    fn exec(&self, db: &DB) -> Frame;
}

#[derive(Debug, err_derive::Error)]
pub enum ParseError {
    #[error(display = "NotArray")]
    NotArray,
    #[error(display = "EndOfArr")]
    EndOfArr,
    #[error(display = "NotString")]
    NotString,
    #[error(display = "{}", _0)]
    Other(crate::Error),
}
pub struct Parser<'a> {
    idx: usize,
    frames: &'a Vec<Frame>,
}

impl<'a> Parser<'a> {
    fn new(frame: &'a Frame) -> crate::Result<Parser<'a>> {
        match frame {
            Frame::Arrays(arr) => Ok(Self {
                idx: 0,
                frames: arr,
            }),
            _ => Err(Box::new(ParseError::NotArray)),
        }
    }

    fn next(&mut self) -> Option<&Frame> {
        if self.idx < self.frames.len() {
            self.idx += 1;
            return Some(&self.frames[self.idx - 1]);
        }
        None
    }

    fn next_string(&mut self) -> crate::Result<String> {
        let next_frame = self.next().ok_or(Box::new(ParseError::EndOfArr))?;
        match next_frame {
            Frame::SimpleString(s) | Frame::BulkStrings(s) => {
                Ok(BytesToString!(s, ParseError::Other))
            }
            _ => Err(Box::new(ParseError::NotString)),
        }
    }

    fn next_bytes(&mut self) -> crate::Result<Bytes> {
        let next_frame = self.next().ok_or(ParseError::EndOfArr)?;
        match next_frame {
            Frame::SimpleString(s) | Frame::BulkStrings(s) => Ok(s.clone()),
            _ => Err(Box::new(ParseError::NotString)),
        }
    }
}

impl Command {
    pub fn new(frame: &Frame) -> crate::Result<Self> {
        let mut parser = Parser::new(frame)?;
        let cmd_string = parser.next_string()?;
        match &cmd_string.to_lowercase()[..] {
            "get" => Ok(Get::new(&mut parser)?.into()),
            "set" => Ok(Set::new(&mut parser)?.into()),
            _ => unimplemented!(),
        }
    }
}

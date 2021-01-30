pub mod get;
use std::str::FromStr;

use anyhow::{anyhow, Error, Result};

use get::*;
pub mod set;
use crate::{db::DB, protocol::Frame, BytesToString};
use set::*;

pub mod debug;
use bytes::*;
use debug::*;
use enum_dispatch::*;

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum Command {
    Get,
    Set,
    Debug,
}

#[enum_dispatch(Command)]
pub trait ExecDB {
    fn exec(&self, db: &mut DB) -> Frame;
    fn get_key(&self) -> &Bytes;
    fn set_nounce(&mut self, nounce: u64);
}

#[derive(Debug, err_derive::Error)]
pub enum ParseError {
    #[error(display = "NotArray")]
    NotArray,
    #[error(display = "NotString")]
    NotString,
    #[error(display = "{}", _0)]
    Other(Error),
}

#[derive(Debug, err_derive::Error)]
pub enum CommandError {
    #[error(display = "MissingOperation")]
    MissingOperation,
    #[error(display = "MissingOperand")]
    MissingOperand,
    #[error(display = "NotImplemented")]
    NotImplemented,
}

fn missing_operand() -> Error {
    Error::new(CommandError::MissingOperand)
}

fn missing_operation() -> Error {
    Error::new(CommandError::MissingOperation)
}

pub struct Parser<'a> {
    idx: usize,
    frames: &'a Vec<Frame>,
}

impl<'a> Parser<'a> {
    fn new(frame: &'a Frame) -> Result<Parser<'a>> {
        match frame {
            Frame::Arrays(arr) => Ok(Self {
                idx: 0,
                frames: &arr.val,
            }),
            _ => Err(Error::new(ParseError::NotArray)),
        }
    }

    fn next(&mut self) -> Option<&Frame> {
        if self.idx < self.frames.len() {
            self.idx += 1;
            return Some(&self.frames[self.idx - 1]);
        }
        None
    }

    fn next_string(&mut self) -> Result<Option<String>> {
        let next_frame = match self.next() {
            Some(x) => x,
            None => {
                return Ok(None);
            }
        };
        match next_frame {
            Frame::SimpleString(s) | Frame::BulkStrings(s) => {
                Ok(Some(BytesToString!(s, ParseError::Other)))
            }
            _ => Err(Error::new(ParseError::NotString)),
        }
    }

    fn next_bytes(&mut self) -> Result<Option<Bytes>> {
        let next_frame = match self.next() {
            Some(x) => x,
            None => {
                return Ok(None);
            }
        };
        match next_frame {
            Frame::SimpleString(s) | Frame::BulkStrings(s) => Ok(Some(s.clone())),
            _ => Err(Error::new(ParseError::NotString)),
        }
    }

    fn next_number<T>(&mut self) -> Result<Option<T>>
    where
        T: FromStr,
    {
        let next_string = self.next_string()?;
        if next_string.is_none() {
            return Ok(None);
        }
        let maybe_num = next_string.unwrap();
        match maybe_num.parse::<T>() {
            Ok(num) => Ok(Some(num)),
            Err(_) => Err(anyhow!("not a number: {}", maybe_num)),
        }
    }
}

impl Command {
    pub fn new(frame: &Frame) -> Result<Self> {
        let mut parser = Parser::new(frame)?;
        let cmd_string = parser.next_string()?.ok_or_else(missing_operation)?;
        match &cmd_string.to_lowercase()[..] {
            "get" => Ok(Get::new(&mut parser)?.into()),
            "set" => Ok(Set::new(&mut parser)?.into()),
            "debug" => Ok(Debug::new(&mut parser)?.into()),
            _ => Err(Error::new(CommandError::NotImplemented)),
        }
    }
}

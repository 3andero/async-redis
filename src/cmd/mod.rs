pub mod get;
use std::vec::IntoIter;

use anyhow::{Error, Result};

use get::*;
pub mod set;
use crate::{db::DB, protocol::Frame, utils::get_integer};
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
#[allow(dead_code)]
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
    #[error(display = "InvalidOperand")]
    InvalidOperand,
}

fn missing_operand() -> Error {
    Error::new(CommandError::MissingOperand)
}

fn missing_operation() -> Error {
    Error::new(CommandError::MissingOperation)
}

pub struct CommandParser {
    frames: IntoIter<Frame>,
}

impl CommandParser {
    fn new(frame: Frame) -> Result<CommandParser> {
        match frame {
            Frame::Arrays(arr) => Ok(Self {
                frames: arr.val.into_iter(),
            }),
            _ => Err(Error::new(ParseError::NotArray)),
        }
    }

    fn next(&mut self) -> Option<Frame> {
        self.frames.next()
    }

    fn next_bytes(&mut self) -> Result<Option<Bytes>> {
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

    fn next_integer(&mut self) -> Result<Option<i64>> {
        match self.next_bytes()? {
            Some(v) => get_integer(&v).map(|v| Some(v)),
            None => Ok(None),
        }
    }
}

impl Command {
    pub fn new(frame: Frame) -> Result<Self> {
        let mut parser = CommandParser::new(frame)?;
        let cmd_string = parser.next_bytes()?.ok_or_else(missing_operation)?;
        match &cmd_string.to_ascii_lowercase()[..] {
            b"get" => Ok(Get::new(&mut parser)?.into()),
            b"set" => Ok(Set::new(&mut parser)?.into()),
            b"debug" => Ok(Debug::new(&mut parser)?.into()),
            _ => Err(Error::new(CommandError::NotImplemented)),
        }
    }
}

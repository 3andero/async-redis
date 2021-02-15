pub mod diagnose;
pub mod get;
pub mod mget;
pub mod mset;
pub mod set;

use diagnose::*;
use get::*;
use mget::*;
use mset::*;
use set::*;

use anyhow::{Error, Result};
use std::vec::IntoIter;

use crate::{db::DB, protocol::Frame, utils::get_integer};

use bytes::*;
use enum_dispatch::*;

#[allow(dead_code)]
pub enum Command {
    Oneshot(OneshotCommand),
    Traverse(TraverseCommand),
    HoldOn(HoldOnCommand),
}

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum OneshotCommand {
    Get,
    Set,
    MGet,
    MSet,
    Dx,
}

#[enum_dispatch(OneshotCommand)]
pub trait OneshotExecDB {
    fn exec(self, db: &mut DB) -> Frame;
    fn get_key(&self) -> &[u8];
}

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum TraverseCommand {
    MSet(MSetDispatcher),
    MGet(MGetDispatcher),
    Dx(DxDispatcher),
}

type IDCommandPair = (usize, Option<OneshotCommand>);

#[enum_dispatch(TraverseCommand)]
pub trait TraverseExecDB {
    fn len(&self) -> usize;
    fn next_command(&mut self) -> IDCommandPair;
    fn next_key(&self) -> Option<&Bytes>;
    fn init(&mut self, db_amount: usize);
    fn move_to(&mut self, db_id: usize);
    fn dispatch(&mut self, dispatch_fn: impl Fn(&[u8]) -> usize) {
        while let Some(k) = self.next_key() {
            let id = dispatch_fn(k);
            // println!("key: {:?}, id: {}", k, id);
            self.move_to(id);
        }
    }
}

pub enum HoldOnCommand {}

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
    len: usize,
}

impl CommandParser {
    fn new(frame: Frame) -> Result<CommandParser> {
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

    fn next(&mut self) -> Option<Frame> {
        self.len = if self.len > 0 { self.len - 1 } else { 0 };
        self.frames.next()
    }

    fn len(&self) -> usize {
        self.len
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

    fn next_bytes_pair(&mut self) -> Result<Option<(Bytes, Bytes)>> {
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
            b"get" => Ok(Command::Oneshot(Get::new(&mut parser)?.into())),
            b"set" => Ok(Command::Oneshot(Set::new(&mut parser)?.into())),
            b"mset" => Ok(Command::Traverse(MSetDispatcher::new(&mut parser)?.into())),
            b"mget" => Ok(Command::Traverse(MGetDispatcher::new(&mut parser)?.into())),
            b"dx" => Ok(Command::Traverse(DxDispatcher::new(&mut parser)?.into())),
            _ => Err(Error::new(CommandError::NotImplemented)),
        }
    }
}

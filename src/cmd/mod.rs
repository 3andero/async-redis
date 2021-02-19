pub mod command_parser;
pub mod command_table;
pub mod diagnose;
pub mod get;
pub mod incr;
pub mod mget;
pub mod mset;
pub mod set;

use command_parser::*;
use command_table::*;
use diagnose::*;
use get::*;
use incr::*;
use mget::*;
use mset::*;
use set::*;

use anyhow::{Error, Result};
use std::slice::Iter;
use utils::rolling_hash_const;

use crate::{db::DB, protocol::Frame, utils};

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
    Incr,
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

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum MiniCommand {
    Pair(_Pair),
    Single(_Single),
}

#[enum_dispatch(MiniCommand)]
pub trait MiniCommandTrait {
    fn get_key(&self) -> &[u8];
}

pub type _Pair = (Bytes, Frame);

impl MiniCommandTrait for _Pair {
    fn get_key(&self) -> &[u8] {
        return self.0.as_ref();
    }
}

pub type _Single = Bytes;

impl MiniCommandTrait for _Single {
    fn get_key(&self) -> &[u8] {
        return self.as_ref();
    }
}

type IDCommandPair = (usize, Option<(OneshotCommand, MergeStrategy)>);

#[enum_dispatch(TraverseCommand)]
pub trait TraverseExecDB {
    fn len(&self) -> usize;
    fn next_command(&mut self) -> IDCommandPair;
    fn iter_data(&self) -> Iter<MiniCommand>;
    fn move_last_to(&mut self, db_id: usize, original_idx: usize);
    fn init_tbls(&mut self, vec: &Vec<usize>);
    fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize) {
        let mut tbl_len = vec![0; db_amount];
        let db_ids: Vec<usize> = self
            .iter_data()
            .map(|v| {
                let id = dispatch_fn(v.get_key());
                tbl_len[id] += 1 as usize;
                id
            })
            .collect();

        self.init_tbls(&tbl_len);

        let mut order = db_ids.len();
        for _ in 0..db_ids.len() {
            order -= 1;
            self.move_last_to(db_ids[order], order);
        }
    }
}

pub enum HoldOnCommand {}

pub enum MergeStrategy {
    Reorder(Vec<usize>),
    Drop,
    Insert(usize),
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
    #[error(display = "InvalidOperation")]
    InvalidOperation,
}

fn missing_operand() -> Error {
    Error::new(CommandError::MissingOperand)
}

fn missing_operation() -> Error {
    Error::new(CommandError::MissingOperation)
}

fn invalid_operand() -> Error {
    Error::new(CommandError::InvalidOperand)
}

fn invalid_operation() -> Error {
    Error::new(CommandError::InvalidOperation)
}

fn rolling_hash(arr: &[u8]) -> Result<usize> {
    let mut res = 0;
    for &b in arr {
        if b <= b'z' && b >= b'a' {
            res = (res * 26 + (b - b'a') as usize) % utils::PRIME;
        } else if b <= b'Z' && b >= b'A' {
            res = (res * 26 + (b - b'A') as usize) % utils::PRIME;
        } else {
            return Err(Error::new(CommandError::InvalidOperation));
        }
    }
    Ok(res)
}

fn binary_lookup(token: usize) -> CommandTable {
    let (mut start, mut end) = (0, COMMAND_NUM);
    let mut mi;
    while start < end {
        mi = (start + end) / 2;
        if COMMAND_LOOKUP[mi].0 < token {
            start = mi + 1;
        } else {
            end = mi;
        }
    }
    if start == COMMAND_NUM {
        return CommandTable::UNIMPLEMENTED;
    }
    if COMMAND_LOOKUP[start].0 != token {
        return CommandTable::UNIMPLEMENTED;
    } else {
        return COMMAND_LOOKUP[start].1;
    }
}

impl Command {
    pub fn new(frame: Frame) -> Result<Self> {
        let mut parser = CommandParser::new(frame)?;
        let cmd_string = parser.next_bytes()?.ok_or_else(missing_operation)?;
        match binary_lookup(rolling_hash(cmd_string.as_ref())?) {
            CommandTable::GET(v) => Ok(Command::Oneshot(Get::new(&mut parser, v)?.into())),
            CommandTable::SET(v) => Ok(Command::Oneshot(Set::new(&mut parser, v)?.into())),
            CommandTable::MSET => Ok(Command::Traverse(MSetDispatcher::new(&mut parser)?.into())),
            CommandTable::MGET => Ok(Command::Traverse(MGetDispatcher::new(&mut parser)?.into())),
            CommandTable::INCR(v) => Ok(Command::Oneshot(Incr::new(&mut parser, v)?.into())),
            CommandTable::DX => Ok(Command::Traverse(DxDispatcher::new(&mut parser)?.into())),
            CommandTable::SHUTDOWN => Ok(Command::Oneshot(Dx::new(DxCommand::Shutdown).into())),
            CommandTable::UNIMPLEMENTED => Err(Error::new(CommandError::NotImplemented)),
        }
    }
}

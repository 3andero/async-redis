pub mod diagnose;
pub mod get;
pub mod mget;
pub mod mset;
pub mod set;
pub mod command_parser;

use diagnose::*;
use get::*;
use mget::*;
use mset::*;
use set::*;
use command_parser::*;

use anyhow::{Error, Result};
use std::slice::Iter;

use crate::{db::DB, protocol::Frame};

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

pub type _Pair = (Bytes, Bytes);

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
        let db_ids: Vec<usize> = self.iter_data().map(|v| {
            let id = dispatch_fn(v.get_key());
            tbl_len[id] += 1 as usize;
            id
        }).collect();
        
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
}

fn missing_operand() -> Error {
    Error::new(CommandError::MissingOperand)
}

fn missing_operation() -> Error {
    Error::new(CommandError::MissingOperation)
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

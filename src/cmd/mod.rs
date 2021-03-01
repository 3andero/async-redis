pub mod command_parser;
pub mod command_table;
pub mod diagnose;
pub mod get;
pub mod incr;
pub mod mget;
pub mod mset;
pub mod publish;
pub mod set;
pub mod subscribe;
pub mod traverse_command;
pub mod unsubscribe;

use command_parser::*;
use command_table::*;
use diagnose::*;
use get::*;
use incr::*;
use mget::*;
use mset::*;
use publish::*;
use set::*;
use subscribe::*;
use traverse_command::*;
use unsubscribe::*;

use anyhow::{Error, Result};
use std::slice::Iter;
use tokio::sync::{mpsc, oneshot};
use utils::{rolling_hash, rolling_hash_const};

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

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum PubSubCommand {
    Subscribe,
    Publish,
    Unsubscribe,
}

#[enum_dispatch(PubSubCommand)]
pub trait PubSubExecDB {
    fn set_extra_info(&mut self, _extra: ExtraInfo) {}
    fn need_extra_info(&self) -> bool {
        false
    }
}

pub enum ExtraInfo {
    SubscribeInfo((u64, Option<mpsc::Sender<Frame>>)),
}

#[enum_dispatch(OneshotCommand)]
pub trait OneshotExecDB {
    fn exec(self, db: &mut DB) -> Frame;
    fn get_key(&self) -> &[u8];
}

#[enum_dispatch(InitSubscription, DispatchToMultipleDB)]
#[derive(Debug)]
pub enum HoldOnCommand {
    Subscribe(SubscribeDispatcher),
    Publish(PublishDispatcher),
}

crate::impl_enum_is_branch!(HoldOnCommand, need_subscribe, (Subscribe, x));

#[enum_dispatch]
pub trait InitSubscription {
    fn set_subscription(
        &mut self,
        _sub_state: &mut Vec<bool>,
        _ret_tx: &mpsc::Sender<Frame>,
        _handler_id: u64,
    ) {
    }
}

pub enum ResultCollector {
    Reorder(Vec<Vec<usize>>),
    KeepFirst(usize),
}

impl ResultCollector {
    pub async fn merge(
        &mut self,
        ret: &mut Vec<Frame>,
        ret_rx: oneshot::Receiver<Frame>,
    ) -> Result<()> {
        match self {
            ResultCollector::KeepFirst(x) => {
                if *x == 0 {
                    return Ok(());
                }
                *x -= 1;
                let f = ret_rx.await.map_err(|e| Error::new(e))?;
                unsafe {
                    ret.as_mut_ptr().add(*x).write(f);
                }
                Ok(())
            }
            ResultCollector::Reorder(tbl) => {
                while tbl.len() > 0 && tbl[tbl.len() - 1].len() == 0 {
                    tbl.pop();
                }
                if tbl.len() == 0 {
                    panic!("expecting something left");
                }
                let order = tbl.pop().unwrap();
                if let Frame::Arrays(arr) = ret_rx.await.map_err(|e| Error::new(e))? {
                    for (f, o) in arr.into_iter().zip(order) {
                        unsafe {
                            ret.as_mut_ptr().add(o).write(f);
                        }
                    }
                } else {
                    panic!("Only Frame::Array can be reordered.");
                }
                Ok(())
            }
        }
    }
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

impl Command {
    pub fn new(frame: Frame) -> Result<Self> {
        let mut parser = CommandParser::new(frame)?;
        let cmd_string = parser.next_bytes()?.ok_or_else(missing_operation)?;
        use Command::*;
        use CommandTable::*;
        match binary_lookup(rolling_hash(cmd_string.as_ref())?) {
            GET(v) => Ok(Oneshot(Get::new(&mut parser, v)?.into())),
            SET(v) => Ok(Oneshot(Set::new(&mut parser, v)?.into())),
            MSET => Ok(Traverse(MSetDispatcher::new(&mut parser)?.into())),
            MGET => Ok(Traverse(MGetDispatcher::new(&mut parser)?.into())),
            INCR(v) => Ok(Oneshot(Incr::new(&mut parser, v)?.into())),
            DX => Ok(Traverse(DxDispatcher::new(&mut parser)?.into())),
            SHUTDOWN => Ok(Oneshot(Dx::new(DxCommand::Shutdown).into())),
            SUBSCRIBE => Ok(HoldOn(SubscribeDispatcher::new(&mut parser)?.into())),
            PUBLISH => Ok(HoldOn(PublishDispatcher::new(&mut parser)?.into())),
            UNIMPLEMENTED => Err(Error::new(CommandError::NotImplemented)),
        }
    }
}

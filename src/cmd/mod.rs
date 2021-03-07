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
    Dx,
    Incr,
}

impl Into<AtomicCMD> for OneshotCommand {
    fn into(self) -> AtomicCMD {
        use OneshotCommand::*;
        match self {
            Get(c) => AtomicCMD::Get(c),
            Set(c) => AtomicCMD::Set(c),
            Incr(c) => AtomicCMD::Incr(c),
            Dx(c) => AtomicCMD::Dx(c),
        }
    }
}

#[enum_dispatch]
#[derive(Debug)]
pub enum AtomicCMD {
    Get,
    Set,
    MGet,
    MSet,
    Dx,
    Incr,
    Subscribe,
    Publish,
    Unsubscribe,
}

#[enum_dispatch(AtomicCMD)]
pub trait AtomicCMDMarker {}

#[enum_dispatch(OneshotCommand)]
pub trait OneshotExecDB {
    fn get_key(&self) -> &[u8];
}

#[enum_dispatch(InitSubscription, DispatchToMultipleDB)]
#[derive(Debug)]
pub enum HoldOnCommand {
    Subscribe(SubscribeDispatcher),
    Publish(PublishDispatcher),
    Unsubscribe(UnsubDispatcher),
}

crate::impl_enum_is_branch!(
    HoldOnCommand,
    need_subscribe,
    (Subscribe, x) | (Unsubscribe, x) => True,
    (Publish, x) => False
);

crate::impl_enum_is_branch!(
    HoldOnCommand,
    is_unsubscribe,
    (Unsubscribe, x) => True,
    (Subscribe, x) | (Publish, x) => False
);

#[enum_dispatch]
pub trait InitSubscription {
    fn set_subscription(
        &mut self,
        sub_state: &mut Vec<bool>,
        ret_tx: &mpsc::Sender<Frame>,
        handler_id: u64,
    );
}

pub struct ResultCollector {
    pub(in crate::cmd) result_type: ResultCollectorType,
    pub(in crate::cmd) ret: Vec<Frame>,
}

enum ResultCollectorType {
    Reorder(Vec<Vec<usize>>),
    KeepFirst(usize),
    SumFirst((usize, i64)),
}

impl ResultCollector {
    pub fn get_ret(self) -> Vec<Frame> {
        use ResultCollectorType::*;
        assert!(
            match &self.result_type {
                KeepFirst(x) => *x == 0,
                Reorder(tbl) => {
                    let mut idx = tbl.len();
                    while idx > 0 && tbl[idx - 1].len() == 0 {
                        idx -= 1;
                    }
                    idx == 0
                }
                SumFirst(_) => true,
            },
            "result_collector should be exhausted before we can use the result"
        );
        self.ret
    }

    pub async fn merge(&mut self, ret_rx: oneshot::Receiver<Frame>) -> Result<()> {
        use ResultCollectorType::*;
        match &mut self.result_type {
            KeepFirst(x) => {
                if *x == 0 {
                    return Ok(());
                }
                *x -= 1;
                let f = ret_rx.await.map_err(|e| Error::new(e))?;
                unsafe {
                    self.ret.as_mut_ptr().add(*x).write(f);
                }
                Ok(())
            }
            Reorder(tbl) => {
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
                            self.ret.as_mut_ptr().add(o).write(f);
                        }
                    }
                } else {
                    panic!("Only Frame::Array can be reordered.");
                }
                Ok(())
            }
            SumFirst((x, res)) => {
                if *x == 0 {
                    return Ok(());
                }
                *x -= 1;
                let f = ret_rx.await.map_err(|e| Error::new(e))?;
                *res += match f {
                    Frame::Integers(v) => v,
                    _ => {
                        panic!("SumFirst can only be applied to integers");
                    }
                };
                if *x == 0 {
                    self.ret.push(Frame::Integers(res.clone()));
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
            UNSUBSCRIBE => Ok(HoldOn(UnsubDispatcher::new(&mut parser)?.into())),
            UNIMPLEMENTED => Err(Error::new(CommandError::NotImplemented)),
        }
    }
}

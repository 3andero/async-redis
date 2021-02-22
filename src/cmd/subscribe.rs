use super::MiniCommand;
use crate::{cmd::*, protocol::Frame, *};
use async_redis::*;
use tokio::sync::mpsc;
use traverse_command::*;

#[derive(Debug, Clone)]
pub struct Subscribe {
    pairs: Vec<MiniCommand>,
    handler_id: usize,
    ret_tx: Option<mpsc::Sender<Frame>>,
}

impl OneshotExecDB for Subscribe {
    fn exec(mut self, db: &mut DB) -> Frame {
        db.subscribe(&mut self.pairs, self.handler_id, self.ret_tx);
        Frame::Ok
    }

    fn get_key(&self) -> &[u8] {
        &self.pairs[0].get_key()
    }

    fn set_extra_info(&mut self, extra: ExtraInfo) {
        use ExtraInfo::*;
        match extra {
            SubscribeInfo((id, ret_tx)) => {
                self.handler_id = id;
                self.ret_tx = ret_tx;
            }
        }
    }
}

impl Subscribe {
    pub fn new(pairs: Vec<MiniCommand>) -> Self {
        Self {
            pairs,
            handler_id: 0,
            ret_tx: None,
        }
    }
}

impl DB {
    pub fn subscribe(
        &mut self,
        keys: &mut Vec<MiniCommand>,
        handler_id: usize,
        ret_tx: Option<mpsc::Sender<Frame>>,
    ) -> Frame {
        for cmd in keys.drain(..) {
            match cmd {
                MiniCommand::Single(key) => self
                    .subscription
                    .entry(key)
                    .or_insert_with(|| Vec::with_capacity(1))
                    .push(handler_id),
                _ => panic!(),
            }
        }

        match ret_tx {
            Some(s) => {
                self.subscriber.insert(handler_id, s);
            }
            None => (),
        }
        Frame::Ok
    }
}
#[define_traverse_command("N:1")]
#[derive(Debug, Clone)]
pub struct SubscribeDispatcher {}

impl_traverse_command!(SendNReturn1, KeyOnly, SubscribeDispatcher, Subscribe);

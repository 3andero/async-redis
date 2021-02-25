use super::MiniCommand;
use crate::{cmd::*, protocol::Frame, *};
use async_redis::*;
use tokio::sync::mpsc;
use traverse_command::*;

#[derive(Debug, Clone)]
pub struct Subscribe {
    pairs: Vec<MiniCommand>,
    handler_id: u64,
    ret_tx: Option<mpsc::Sender<Frame>>,
}

impl PubSubExecDB for Subscribe {
    fn set_extra_info(&mut self, extra: ExtraInfo) {
        use ExtraInfo::*;
        match extra {
            SubscribeInfo((id, ret_tx)) => {
                self.handler_id = id;
                self.ret_tx = ret_tx;
            }
        }
    }

    fn need_extra_info(&self) -> bool {
        true
    }
}

impl Subscribe {
    pub fn new(value: (Vec<MiniCommand>, Option<mpsc::Sender<Frame>>, u64)) -> Self {
        Self {
            pairs: value.0,
            handler_id: value.2,
            ret_tx: value.1,
        }
    }

    pub fn exec(mut self, db: &mut DB) -> Frame {
        db.subscribe(&mut self.pairs, self.handler_id, self.ret_tx);
        Frame::Ok
    }
}

impl DB {
    pub fn subscribe(
        &mut self,
        keys: &mut Vec<MiniCommand>,
        handler_id: u64,
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
#[derive(Debug, Clone, Default)]
pub struct SubscribeDispatcher {
    ret_txs: Vec<Option<mpsc::Sender<Frame>>>,
    handler_id: u64,
}

#[macro_export]
macro_rules! pop_ret_id {
    ($self:ident) => {{
        let ret_tx = $self.ret_txs.pop().unwrap();
        $self
            .cmds_tbl
            .pop()
            .filter(|v| v.len() > 0)
            .map(|v| (v, ret_tx, $self.handler_id))
    }};
}

impl_traverse_command!(
    SendNReturn1,
    KeyOnly,
    SubscribeDispatcher,
    Subscribe,
    PubSubCommand,
    pop_ret_id
);

impl InitSubscription for SubscribeDispatcher {
    fn set_subscription(
        &mut self,
        sub_state: &mut Vec<bool>,
        ret_tx: &mpsc::Sender<Frame>,
        handler_id: u64,
    ) {
        self.ret_txs = self.cmds_tbl
            .iter()
            .enumerate()
            .map(|(id, cmds)| {
                if cmds.len() > 0 && !sub_state[id] {
                    sub_state[id] = true;
                    Some(ret_tx.clone())
                } else {
                    None
                }
            })
            .collect();
        self.handler_id = handler_id;
    }
}

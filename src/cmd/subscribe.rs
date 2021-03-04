use super::MiniCommand;
use crate::{cmd::*, db::SubscriptionSubModule, protocol::Frame, utils::VecMap, *};
use async_redis::*;
use tokio::sync::mpsc;
use traverse_command::*;

#[derive(Debug, Clone)]
pub struct Subscribe {
    cmds: Vec<MiniCommand>,
    handler_id: u64,
    ret_tx: Option<mpsc::Sender<Frame>>,
}

impl PubSubExecDB for Subscribe {}

impl Subscribe {
    pub fn new(value: (Vec<MiniCommand>, Option<mpsc::Sender<Frame>>, u64)) -> Self {
        Self {
            cmds: value.0,
            handler_id: value.2,
            ret_tx: value.1,
        }
    }

    pub fn exec(mut self, db: &mut DB) -> Frame {
        db.subscribe
            .subscribe(&mut self.cmds, self.handler_id, self.ret_tx);
        Frame::Ok
    }
}

impl SubscriptionSubModule {
    pub fn subscribe(
        &mut self,
        keys: &mut Vec<MiniCommand>,
        handler_id: u64,
        ret_tx: Option<mpsc::Sender<Frame>>,
    ) -> Frame {
        let (_, listening_inner) = self
            .subscriber
            .entry(handler_id)
            .and_modify(|handler_info| {
                handler_info.1.reserve(keys.len());
            })
            .or_insert_with(|| (ret_tx.unwrap(), VecMap::with_capacity(keys.len())));
        let mut listening = VecMap::new();
        std::mem::swap(&mut listening, listening_inner);

        for cmd in keys.drain(..) {
            match cmd {
                MiniCommand::Single(key) => {
                    let mut is_new = false;
                    let channel_id = match self.channels.get(&key) {
                        Some(id) => *id,
                        None => {
                            self.counter += 1;
                            let cid = self.counter;
                            is_new = true;
                            self.channels.insert(key.clone(), cid);
                            self.channel_info.insert(cid, key);
                            cid
                        }
                    };

                    is_new = if is_new {
                        let mut new_channel_subscriber = VecMap::with_capacity(1);
                        new_channel_subscriber.push(&handler_id);
                        self.subscription.insert(channel_id, new_channel_subscriber);
                        true
                    } else {
                        match self.subscription.get_mut(&channel_id) {
                            Some(vm) => vm.push(&handler_id),
                            None => panic!(),
                        }
                    };

                    if is_new {
                        listening.push(&channel_id);
                    }
                }
                _ => panic!(),
            }
        }

        match self.subscriber.get_mut(&handler_id) {
            Some((_, listening_inner)) => {
                std::mem::swap(listening_inner, &mut listening);
            }
            _ => panic!(),
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
        assert!($self.ret_txs.len() > 0, "set_subscription was not executed");
        assert!(
            $self.cmds_tbl.len() > 0,
            "self.cmds_tbl was not properly initialized"
        );
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
    (Key)+,
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
        assert!(
            self.cmds_tbl.len() > 0,
            "self.cmds_tbl was not properly initialized"
        );
        self.ret_txs = self
            .cmds_tbl
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

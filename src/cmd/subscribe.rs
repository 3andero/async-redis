use super::MiniCommand;
use crate::{cmd::*, db::SubscriptionSubModule, protocol::Frame, utils::VecMap, *};
use async_redis::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use traverse_command::*;

#[derive(Debug, Clone)]
pub struct Subscribe {
    cmds: Vec<MiniCommand>,
    handler_id: u64,
    ret_tx: Option<mpsc::Sender<Frame>>,
    total_chn_amount: Arc<AtomicUsize>,
}

impl Subscribe {
    pub fn new(
        value: (
            Vec<MiniCommand>,
            Option<mpsc::Sender<Frame>>,
            u64,
            Arc<AtomicUsize>,
        ),
    ) -> Self {
        Self {
            cmds: value.0,
            handler_id: value.2,
            ret_tx: value.1,
            total_chn_amount: value.3,
        }
    }

    pub fn exec(self, db: &mut DB) -> Frame {
        db.subscribe
            .subscribe(self.cmds, self.handler_id, self.ret_tx, self.total_chn_amount)
    }
}

impl SubscriptionSubModule {
    pub fn subscribe(
        &mut self,
        keys: Vec<MiniCommand>,
        handler_id: u64,
        ret_tx: Option<mpsc::Sender<Frame>>,
        total_chn_amount: Arc<AtomicUsize>,
    ) -> Frame {
        let (_, chn_listening_inner) = self
            .subscriber_info
            .entry(handler_id)
            .and_modify(|handler_info| {
                handler_info.1.reserve(keys.len());
            })
            .or_insert_with(|| (ret_tx.unwrap(), VecMap::with_capacity(keys.len())));
        let mut chn_listening = VecMap::new();
        std::mem::swap(&mut chn_listening, chn_listening_inner);

        let mut is_key_new_subs = vec![false; keys.len()];
        let mut new_subs_amount = 0;
        // let mut curr_chn_amount = chn_listening.len();

        for (idx, cmd) in keys.iter().enumerate() {
            let key = cmd.ref_single();

            let (channel_id, is_new_chn) = match self.channels.get(key) {
                Some(id) => (*id, false),
                None => {
                    self.chn_id_gen += 1;
                    let cid = self.chn_id_gen;
                    self.channels.insert(key.clone(), cid);
                    self.channel_info.insert(cid, key.clone());
                    (cid, true)
                }
            };

            let is_new_subscription = if is_new_chn {
                let mut new_channel_subscriber = VecMap::with_capacity(1);
                new_channel_subscriber.push(&handler_id);
                self.subscriber.insert(channel_id, new_channel_subscriber);
                true
            } else {
                match self.subscriber.get_mut(&channel_id) {
                    Some(vm) => vm.push(&handler_id),
                    None => panic!(),
                }
            };

            if is_new_subscription {
                new_subs_amount += 1;
                is_key_new_subs[idx] = true;
                chn_listening.push(&channel_id);
            }
        }

        match self.subscriber_info.get_mut(&handler_id) {
            Some((_, listening_inner)) => {
                std::mem::swap(listening_inner, &mut chn_listening);
            }
            _ => panic!(),
        }

        let mut prev_amount = total_chn_amount.fetch_add(new_subs_amount, Ordering::AcqRel);
        is_key_new_subs
            .iter()
            .zip(keys)
            .map(|(is_new, cmd)| {
                if *is_new {
                    prev_amount += 1;
                }

                Frame::Arrays(vec![
                    Frame::SimpleString(Bytes::from_static(b"Subscribe")),
                    Frame::BulkStrings(cmd.unwrap_single()),
                    Frame::Integers(prev_amount as i64),
                ])
            })
            .collect::<Vec<Frame>>()
            .into()
    }
}
#[define_traverse_command("N:N")]
#[derive(Debug, Clone, Default)]
pub struct SubscribeDispatcher {
    ret_txs: Vec<Option<mpsc::Sender<Frame>>>,
    handler_id: u64,
    total_chn_amount: Arc<AtomicUsize>,
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
            .map(|v| (v, ret_tx, $self.handler_id, $self.total_chn_amount.clone()))
    }};
}

impl_traverse_command!(
    for cmd: Subscribe = SubscribeDispatcher((Key)+).pop_ret_id!() {
        cmd >> DB
    },
    DB >> N Frame(s) >> AsIs
);

impl InitSubscription for SubscribeDispatcher {
    fn set_subscription(
        &mut self,
        sub_state: &mut Vec<bool>,
        ret_tx: &mpsc::Sender<Frame>,
        handler_id: u64,
        total_chn_amount: Arc<AtomicUsize>,
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
        self.total_chn_amount = total_chn_amount;
    }
}

impl AtomicCMDMarker for Subscribe {}

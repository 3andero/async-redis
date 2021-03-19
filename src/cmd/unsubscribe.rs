use super::MiniCommand;
use crate::{cmd::*, db::SubscriptionSubModule, protocol::Frame, *};
use async_redis::*;
use tokio::sync::mpsc;
use traverse_command::*;

#[derive(Debug, Clone)]
pub struct Unsubscribe {
    cmds: Option<Vec<MiniCommand>>, // `None` stands for `unsunscribe all`
    handler_id: u64,
    total_chn_amount: Arc<AtomicUsize>,
}

impl Unsubscribe {
    pub async fn exec(self, db: &mut DB) -> Frame {
        db.subscribe
            .unsubscribe(self.cmds, self.handler_id, db.id, self.total_chn_amount)
            .await
    }
}

impl SubscriptionSubModule {
    pub fn remove_subscriber(&mut self, channel_id: &usize, handler_id: &u64) -> Option<Bytes> {
        let listener = self.subscriber.get_mut(channel_id).unwrap();
        let ret = if listener.remove(handler_id) {
            self.channel_info.get(channel_id).cloned()
        } else {
            None
        };
        if listener.len() == 0 {
            // no listeners left, remove this channel
            self.subscriber.remove(channel_id);
            self.channels
                .remove(&self.channel_info.remove(channel_id).unwrap());
        }
        ret
    }

    pub async fn unsubscribe(
        &mut self,
        keys: Option<Vec<MiniCommand>>,
        handler_id: u64,
        db_id: usize,
        total_chn_amount: Arc<AtomicUsize>,
    ) -> Frame {
        if keys.is_none() {
            let (subscriber_ret_tx, subscribed_channel) =
                self.subscriber_info.remove(&handler_id).unwrap();

            let _ = subscriber_ret_tx
                .send(Frame::_DetachSubscribeMode(db_id))
                .await;

            let mut prev_amount = total_chn_amount.fetch_sub(
                subscribed_channel.len(),
                std::sync::atomic::Ordering::AcqRel,
            );
            subscribed_channel
                .iter()
                .map(|channel_id| {
                    vec![
                        Frame::SimpleString(Bytes::from_static(b"Unsubscribe")),
                        Frame::BulkStrings(
                            self.remove_subscriber(channel_id, &handler_id).unwrap(),
                        ),
                        Frame::Integers({
                            prev_amount -= 1;
                            prev_amount as i64
                        }),
                    ]
                    .into()
                })
                .collect::<Vec<Frame>>()
                .into()
        } else {
            let cmd_arr = keys.unwrap();

            let (subscriber_ret_tx, subscribed_channel) =
                self.subscriber_info.get_mut(&handler_id).unwrap();
            let mut channel_id_to_remove = Vec::with_capacity(cmd_arr.len());
            let mut is_key_removed = vec![true; cmd_arr.len()];
            let mut rm_subs_amount = channel_id_to_remove.len();

            for (idx, cmd) in cmd_arr.iter().enumerate() {
                let key = cmd.ref_single();
                let channel_id = match self.channels.get(key) {
                    None => {
                        continue;
                    }
                    Some(v) => *v,
                };
                if !subscribed_channel.remove(&channel_id) {
                    is_key_removed[idx] = false;
                    rm_subs_amount -= 1;
                    continue;
                }
                channel_id_to_remove.push(channel_id);
            }

            if subscribed_channel.len() == 0 {
                let _ = subscriber_ret_tx
                    .send(Frame::_DetachSubscribeMode(db_id))
                    .await;
                let _ = self.subscriber_info.remove(&handler_id).unwrap();
            }

            for channel_id in channel_id_to_remove.iter() {
                self.remove_subscriber(channel_id, &handler_id);
            }

            let mut prev_amount =
                total_chn_amount.fetch_sub(rm_subs_amount, std::sync::atomic::Ordering::AcqRel);
            is_key_removed
                .iter()
                .zip(cmd_arr)
                .map(|(is_removed, cmd)| {
                    if *is_removed {
                        prev_amount -= 1;
                    }

                    Frame::Arrays(vec![
                        Frame::BulkStrings(Bytes::from_static(b"Unsubscribe")),
                        Frame::BulkStrings(cmd.unwrap_single()),
                        Frame::Integers(prev_amount as i64),
                    ])
                })
                .collect::<Vec<Frame>>()
                .into()
        }
    }
}

impl Unsubscribe {
    fn new(value: (Option<Vec<MiniCommand>>, u64, Arc<AtomicUsize>)) -> Self {
        Self {
            cmds: value.0,
            handler_id: value.1,
            total_chn_amount: value.2,
        }
    }
}

#[define_traverse_command("N:N")]
#[derive(Debug, Clone, Default)]
pub struct UnsubDispatcher {
    handler_id: u64,
    sub_state: Vec<bool>,
    total_chn_amount: Arc<AtomicUsize>,
}

#[macro_export]
macro_rules! pop_unsub_chan {
    ($self:ident) => {{
        assert!(
            $self.sub_state.len() > 0,
            "self.sub_state wasn't properly initialized"
        );
        let state = $self.sub_state.pop().unwrap();
        if !$self.has_operand {
            if state {
                Some((None, $self.handler_id, $self.total_chn_amount.clone()))
            } else {
                None
            }
        } else {
            assert!(
                $self.cmds_tbl.len() > 0,
                "self.cmds_tbl wasn't properly initialized"
            );
            $self
                .cmds_tbl
                .pop()
                .filter(|v| v.len() > 0 && state)
                .map(|v| (Some(v), $self.handler_id, $self.total_chn_amount.clone()))
        }
    }};
}

impl_traverse_command!(
    for cmd: Unsubscribe = UnsubDispatcher((Key)*).pop_unsub_chan!() {
        cmd >> DB
    },
    DB >> N Frame(s) >> AsIs
);

impl InitSubscription for UnsubDispatcher {
    fn set_subscription(
        &mut self,
        sub_state: &mut Vec<bool>,
        _: &mpsc::Sender<Frame>,
        handler_id: u64,
        total_chn_amount: Arc<AtomicUsize>,
    ) {
        self.handler_id = handler_id;
        self.sub_state = sub_state.clone();
        self.total_chn_amount = total_chn_amount;
    }
}

impl UnsubDispatcher {
    pub fn unsubscribe_all(
        handler_id: u64,
        sub_state: Vec<bool>,
        db_amount: usize,
    ) -> HoldOnCommand {
        Self {
            handler_id,
            sub_state,
            db_amount,
            ..Default::default()
        }
        .into()
    }
}

impl AtomicCMDMarker for Unsubscribe {}

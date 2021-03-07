use super::MiniCommand;
use crate::{cmd::*, db::SubscriptionSubModule, protocol::Frame, *};
use async_redis::*;
use tokio::sync::mpsc;
use traverse_command::*;

#[derive(Debug, Clone)]
pub struct Unsubscribe {
    cmds: Option<Vec<MiniCommand>>, // `None` stands for `unsunscribe all`
    handler_id: u64,
}

impl Unsubscribe {
    pub async fn exec(mut self, db: &mut DB) -> Frame {
        db.subscribe
            .unsubscribe(&mut self.cmds, self.handler_id, db.id)
            .await
    }
}

impl SubscriptionSubModule {
    pub fn remove_subscriber(&mut self, channel_id: &usize, handler_id: &u64) {
        let listener = self.subscription.get_mut(channel_id).unwrap();
        listener.remove(handler_id);
        if listener.len() == 0 {
            // no listeners left, remove this channel
            self.subscription.remove(channel_id);
            self.channels
                .remove(&self.channel_info.remove(channel_id).unwrap());
        }
    }

    pub async fn unsubscribe(
        &mut self,
        keys: &mut Option<Vec<MiniCommand>>,
        handler_id: u64,
        db_id: usize,
    ) -> Frame {
        match keys {
            None => {
                let (subscriber_ret_tx, subscribed_channel) =
                    self.subscriber.remove(&handler_id).unwrap();
                for channel_id in subscribed_channel.iter() {
                    self.remove_subscriber(channel_id, &handler_id);
                }
                let _ = subscriber_ret_tx
                    .send(Frame::_DetachSubscribeMode(db_id))
                    .await;
            }
            Some(cmd_arr) => {
                let (subscriber_ret_tx, subscribed_channel) =
                    self.subscriber.get_mut(&handler_id).unwrap();
                let mut channel_id_to_remove = Vec::with_capacity(cmd_arr.len());
                for cmd in cmd_arr.drain(..) {
                    match cmd {
                        MiniCommand::Single(key) => {
                            let channel_id = match self.channels.get(&key) {
                                None => {
                                    continue;
                                }
                                Some(v) => *v,
                            };
                            if !subscribed_channel.remove(&channel_id) {
                                continue;
                            }
                            channel_id_to_remove.push(channel_id);
                        }
                        _ => panic!(),
                    }
                }
                if subscribed_channel.len() == 0 {
                    let _ = subscriber_ret_tx
                        .send(Frame::_DetachSubscribeMode(db_id))
                        .await;
                    let _ = self.subscriber.remove(&handler_id).unwrap();
                }
                for channel_id in channel_id_to_remove.iter() {
                    self.remove_subscriber(channel_id, &handler_id);
                }
            }
        }
        Frame::Ok
    }
}

impl Unsubscribe {
    fn new(value: (Option<Vec<MiniCommand>>, u64)) -> Self {
        Self {
            cmds: value.0,
            handler_id: value.1,
        }
    }
}

#[define_traverse_command("N:1")]
#[derive(Debug, Clone, Default)]
pub struct UnsubDispatcher {
    handler_id: u64,
    sub_state: Vec<bool>,
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
                Some((None, $self.handler_id))
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
                .map(|v| (Some(v), $self.handler_id))
        }
    }};
}

impl_traverse_command!(
    for cmd: Unsubscribe = UnsubDispatcher((Key)*).pop_unsub_chan!() {
        cmd >> DB
    },
    DB >> 1 Frame
);

impl InitSubscription for UnsubDispatcher {
    fn set_subscription(
        &mut self,
        sub_state: &mut Vec<bool>,
        _: &mpsc::Sender<Frame>,
        handler_id: u64,
    ) {
        self.handler_id = handler_id;
        self.sub_state = sub_state.clone();
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

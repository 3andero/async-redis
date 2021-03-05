use utils::VecMap;

use crate::{
    cmd::*,
    db::{SubscriptionSubModule, DB},
};

#[derive(Debug, Clone)]
pub struct Publish {
    key: Bytes,
    val: Frame,
}

impl PubSubExecDB for Publish {}

impl Publish {
    pub async fn exec(self, db: &mut DB) -> Frame {
        if let Some(listeners) = db.subscribe.get_listeners(&self.key) {
            let mut sent = 0;
            for &id in listeners.iter() {
                if let Some(sender) = db.subscribe.get_ret_tx(&id) {
                    if sender.send(self.val.clone()).await.is_ok() {
                        sent += 1;
                    } else {
                        todo!("drop inactive subscriber")
                    }
                }
            }
            Frame::Integers(sent)
        } else {
            Frame::Integers(0)
        }
    }
}

impl SubscriptionSubModule {
    pub fn get_listeners(&self, key: &Bytes) -> Option<&VecMap<u64>> {
        match self.channels.get(key) {
            Some(channel_id) => self.subscription.get(channel_id),
            None => None,
        }
    }

    pub fn get_ret_tx(&self, handler_id: &u64) -> Option<&mpsc::Sender<Frame>> {
        self.subscriber
            .get(handler_id)
            .map(|handler_info| &handler_info.0)
    }
}

impl Publish {
    fn new(key: Bytes, val: Frame) -> Self {
        Self { key, val }
    }
}

#[derive(Debug, Clone)]
pub struct PublishDispatcher {
    key: Bytes,
    val: Frame,
    db_amount: usize,
}

impl DispatchToMultipleDB for PublishDispatcher {
    fn next_command(&mut self) -> Option<IDCommandPair> {
        if self.db_amount > 0 {
            self.db_amount -= 1;
            Some((
                self.db_amount,
                AtomicCommand::PubSub(Publish::new(self.key.clone(), self.val.clone()).into()),
            ))
        } else {
            None
        }
    }

    fn get_result_collector(&mut self) -> ResultCollector {
        assert!(self.db_amount > 0, "self.db_amount wasn't initialized");
        let ret = unsafe { new_unsafe_vec(self.db_amount) };
        ResultCollector {
            result_type: ResultCollectorType::KeepFirst(self.db_amount),
            ret,
        }
    }

    fn dispatch(&mut self, db_amount: usize, _: impl Fn(&[u8]) -> usize) {
        self.db_amount = db_amount;
    }
    // fn len(&self) -> usize {
    //     self.db_amount
    // }
}

impl PublishDispatcher {
    pub fn new(parser: &mut CommandParser) -> Result<Self> {
        let key = parser.next_bytes()?.ok_or_else(missing_operand)?;
        let val = parser.next().ok_or_else(missing_operand)?;
        Ok(Self {
            key,
            val,
            db_amount: 0,
        })
    }
}

impl InitSubscription for PublishDispatcher {
    fn set_subscription(&mut self, _: &mut Vec<bool>, _: &mpsc::Sender<Frame>, _: u64) {}
}

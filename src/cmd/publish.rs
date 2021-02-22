use crate::{cmd::*, db::DB};

#[derive(Debug, Clone)]
pub struct Publish {
    key: Bytes,
    val: Frame,
}

impl OneshotExecDB for Publish {
    fn exec(self, db: &mut DB) -> Frame {
        if let Some(listeners) = db.subscription.get(&self.key) {
            let mut sent = 0;
            for &id in listeners {
                if let Some(sender) = db.subscriber.get(&id) {
                    sender.send(self.val.clone());
                    sent += 1;
                }
            }

            Frame::Integers(sent)
        } else {
            Frame::Integers(0)
        }
    }

    fn get_key(&self) -> &[u8] {
        self.key.as_ref()
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
    fn next_command(&mut self) -> IDCommandPair {
        self.db_amount -= 1;
        (
            self.db_amount,
            Some(Publish::new(self.key.clone(), self.val.clone()).into()),
        )
    }

    fn move_last_to(&mut self, _: usize, _: usize) {}

    fn get_result_collector(&mut self) -> ResultCollector {
        ResultCollector::KeepFirst(self.db_amount)
    }

    fn iter_data(&self) -> Iter<MiniCommand> {
        unimplemented!()
    }

    fn init_tbls(&mut self, _: &Vec<usize>) {
        unimplemented!()
    }
    fn dispatch(&mut self, db_amount: usize, _: impl Fn(&[u8]) -> usize) {
        self.db_amount = db_amount;
    }
    fn len(&self) -> usize {
        self.db_amount
    }
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
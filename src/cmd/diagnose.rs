use std::unimplemented;

use crate::cmd::*;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Dx {
    key: DxCommand,
}

#[derive(Debug, Clone)]
pub enum DxCommand {
    KeyNum,
    TotalKeyLen,
    TotalValLen,
    RandomKeys,
}

impl Dx {
    pub fn new(key: DxCommand) -> Dx {
        Self { key }
    }
}

impl OneshotExecDB for Dx {
    fn exec(self, db: &mut DB) -> Frame {
        db.diagnose(&self.key)
    }

    fn get_key(&self) -> &[u8] {
        b""
    }
}

#[derive(Debug, Clone)]
pub struct DxDispatcher {
    key: DxCommand,
    db_amount: usize,
}

impl TraverseExecDB for DxDispatcher {
    fn next_command(&mut self) -> IDCommandPair {
        self.db_amount -= 1;
        (
            self.db_amount,
            Some((
                Dx::new(self.key.clone()).into(),
                MergeStrategy::Insert(self.db_amount),
            )),
        )
    }

    fn move_last_to(&mut self, _: usize, _: usize) {}

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

impl DxDispatcher {
    pub fn new(parser: &mut CommandParser) -> Result<DxDispatcher> {
        let raw_key = parser.next_bytes()?.ok_or_else(missing_operand)?;
        let debug_command = match &raw_key.to_ascii_lowercase()[..] {
            b"key_num" => DxCommand::KeyNum,
            b"total_key_len" => DxCommand::TotalKeyLen,
            b"total_val_len" => DxCommand::TotalValLen,
            b"random_keys" => DxCommand::RandomKeys,
            _ => {
                return Err(Error::new(CommandError::NotImplemented));
            }
        };
        Ok(Self {
            key: debug_command,
            db_amount: 0,
        })
    }
}

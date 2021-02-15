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
    len: usize,
}

impl TraverseExecDB for DxDispatcher {
    fn next_command(&mut self) -> IDCommandPair {
        self.db_amount -= 1;
        (self.db_amount, Some(Dx::new(self.key.clone()).into()))
    }
    fn next_key(&self) -> Option<&Bytes> {
        None
    }
    fn init(&mut self, db_amount: usize) {
        self.db_amount = db_amount;

        self.len *= db_amount;
    }
    fn move_to(&mut self, _: usize) {}
    fn dispatch(&mut self, _: impl Fn(&[u8]) -> usize) {}
    fn len(&self) -> usize {
        self.len
    }
}

impl DxDispatcher {
    pub fn new(parser: &mut CommandParser) -> Result<DxDispatcher> {
        let raw_key = parser.next_bytes()?.ok_or_else(missing_operand)?;
        let (debug_command, len) = match &raw_key.to_ascii_lowercase()[..] {
            b"key_num" => (DxCommand::KeyNum, 1),
            b"total_key_len" => (DxCommand::TotalKeyLen, 1),
            b"total_val_len" => (DxCommand::TotalValLen, 1),
            b"random_keys" => (DxCommand::RandomKeys, 5),
            _ => {
                return Err(Error::new(CommandError::NotImplemented));
            }
        };
        Ok(Self {
            key: debug_command,
            db_amount: 0,
            len,
        })
    }
}

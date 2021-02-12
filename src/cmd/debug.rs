use crate::cmd::*;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Debug {
    key: DebugCommand,
    origin_key: Bytes,
}

#[derive(Debug, Clone)]
pub enum DebugCommand {
    KeyNum,
    TotalKeyLen,
    TotalValLen,
    RandomKeys,
}

impl Debug {
    pub fn new(parser: &mut CommandParser) -> Result<Debug> {
        let raw_key = parser.next_bytes()?.ok_or_else(missing_operand)?;
        let debug_command = match &raw_key.to_ascii_lowercase()[..] {
            b"key_num" => DebugCommand::KeyNum,
            b"total_key_len" => DebugCommand::TotalKeyLen,
            b"total_val_len" => DebugCommand::TotalValLen,
            b"random_keys" => DebugCommand::RandomKeys,
            _ => {
                return Err(Error::new(CommandError::NotImplemented));
            }
        };
        Ok(Self {
            key: debug_command,
            origin_key: raw_key,
        })
    }
}

impl ExecDB for Debug {
    fn exec(&self, db: &mut DB) -> Frame {
        db.debug(&self.key)
    }

    fn get_key(&self) -> &Bytes {
        &self.origin_key
    }

    fn set_nounce(&mut self, _: u64) {}
}

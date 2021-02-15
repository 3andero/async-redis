use crate::cmd::*;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Get {
    key: Bytes,
}

impl Get {
    pub fn new(parser: &mut CommandParser) -> Result<Get> {
        Ok(Self {
            key: parser.next_bytes()?.ok_or_else(missing_operand)?,
        })
    }
}

impl OneshotExecDB for Get {
    fn exec(self, db: &mut DB) -> Frame {
        db.get(&self.key).into()
    }

    fn get_key(&self) -> &[u8] {
        &self.key.as_ref()
    }
}

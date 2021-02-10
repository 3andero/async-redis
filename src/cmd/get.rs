use crate::cmd::*;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Get {
    key: Box<Bytes>,
    nounce: u64,
}

impl Get {
    pub fn new(parser: &mut CommandParser) -> Result<Get> {
        Ok(Self {
            key: parser.next_bytes()?.ok_or_else(missing_operand)?,
            nounce: 0,
        })
    }
}

impl ExecDB for Get {
    fn exec(self, db: &mut DB) -> Frame {
        db.get(&self.key).into()
    }

    fn get_key(&self) -> &Bytes {
        &self.key
    }

    fn set_nounce(&mut self, nounce: u64) {
        self.nounce = nounce;
    }
}

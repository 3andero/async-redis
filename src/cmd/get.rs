use crate::cmd::*;
use anyhow::Result;

#[derive(Debug)]
pub struct Get {
    key: Bytes,
    nounce: u64,
}

impl Get {
    pub fn new(parser: &mut Parser) -> Result<Get> {
        Ok(Self {
            key: parser.next_bytes()?.ok_or_else(missing_operand)?,
            nounce: 0,
        })
    }
}

impl ExecDB for Get {
    fn exec(&self, db: &mut DB) -> Frame {
        db.get(&self.key).map_or(Frame::NullString, |v| v.into())
    }

    fn get_key(&self) -> &Bytes {
        &self.key
    }

    fn set_nounce(&mut self, nounce: u64) {
        self.nounce = nounce;
    }
}

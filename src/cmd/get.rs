use crate::cmd::*;
use anyhow::Result;

#[derive(Debug)]
pub struct Get {
    key: Bytes,
}

impl Get {
    pub fn new(parser: &mut Parser) -> Result<Get> {
        Ok(Self {
            key: parser.next_bytes()?.ok_or_else(missing_operand)?,
        })
    }
}

impl ExecDB for Get {
    fn exec(&self, db: &DB) -> Frame {
        db.get(&self.key).map_or(Frame::Null, |v| v.into())
    }
}

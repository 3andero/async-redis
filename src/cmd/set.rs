use crate::{cmd::*, db::DB};
use anyhow::Result;

#[derive(Debug)]
pub struct Set {
    key: Bytes,
    val: Bytes,
    expiration: Option<u64>,
}

impl Set {
    pub fn new(parser: &mut Parser) -> Result<Set> {
        let k = parser.next_bytes()?.ok_or_else(missing_operand)?;
        let v = parser.next_bytes()?.ok_or_else(missing_operand)?;
        let expire = parser.next_number()?;
        Ok(Self {
            key: k,
            val: v,
            expiration: expire,
        })
    }
}

impl ExecDB for Set {
    fn exec(&self, db: &DB) -> Frame {
        db.set(self.key.clone(), self.val.clone(), self.expiration);
        Frame::Ok
    }
}

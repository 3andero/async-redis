use crate::cmd::*;
use crate::db::DBReturn;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Debug {
    key: Bytes,
}

impl Debug {
    pub fn new(parser: &mut Parser) -> Result<Debug> {
        Ok(Self {
            key: parser.next_bytes()?.ok_or_else(missing_operand)?,
        })
    }
}

impl ExecDB for Debug {
    fn exec(&self, db: &mut DB) -> Frame {
        match db.debug(&self.key) {
            DBReturn::List(arr) => arr.into(),
            DBReturn::Single(opt_b) => opt_b.into(),
        }
    }

    fn get_key(&self) -> &Bytes {
        &self.key
    }

    fn set_nounce(&mut self, _: u64) {}
}

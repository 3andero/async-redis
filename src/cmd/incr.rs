use crate::{cmd::*, utils::get_integer};
use anyhow::Result;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct Incr {
    key: Bytes,
}

impl Incr {
    pub fn new(parser: &mut CommandParser) -> Result<Incr> {
        Ok(Self {
            key: parser.next_bytes()?.ok_or_else(missing_operand)?,
        })
    }
}

impl OneshotExecDB for Incr {
    fn exec(self, db: &mut DB) -> Frame {
        db.incr(&self.key)
    }

    fn get_key(&self) -> &[u8] {
        &self.key.as_ref()
    }
}

impl DB {
    fn incr(&mut self, key: &Bytes) -> Frame {
        self.database
            .get_mut(key)
            .filter(|v| v.expiration.is_none() || v.expiration.unwrap() > Instant::now())
            .map_or_else(
                || Frame::NullString,
                |en| match &mut en.data {
                    Frame::BulkStrings(b) => match get_integer(b) {
                        Ok(v) => {
                            en.data = Frame::Integers(v + 1);
                            return Frame::Integers(v + 1);
                        }
                        Err(_) => {
                            return Frame::NullString;
                        }
                    },
                    Frame::Integers(i) => {
                        *i += 1;
                        return Frame::Integers(*i + 1);
                    }
                    _ => {
                        return Frame::NullString;
                    }
                },
            )
    }
}

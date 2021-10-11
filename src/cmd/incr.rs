use crate::{cmd::*, utils::get_integer};
use anyhow::Result;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct Incr {
    key: Bytes,
    by: i64,
}

#[derive(Debug, Clone, Copy)]
pub enum IncrVariant {
    Incr,
    IncrBy,
    Decr,
    DecrBy,
}

impl Incr {
    pub fn new(parser: &mut CommandParser, variant: IncrVariant) -> Result<Incr> {
        let (key, by) = match variant {
            IncrVariant::Incr => {
                if parser.len() > 1 {
                    return Err(invalid_operation());
                }
                (parser.next_bytes()?.ok_or_else(missing_operand)?, 1)
            }
            IncrVariant::IncrBy => {
                if parser.len() > 2 {
                    return Err(invalid_operation());
                }
                let key = parser.next_bytes()?.ok_or_else(missing_operand)?;
                let by = parser.next_integer()?.ok_or_else(missing_operand)?;
                (key, by)
            }
            IncrVariant::Decr => {
                if parser.len() > 1 {
                    return Err(invalid_operation());
                }
                (parser.next_bytes()?.ok_or_else(missing_operand)?, -1)
            }
            IncrVariant::DecrBy => {
                if parser.len() > 2 {
                    return Err(invalid_operation());
                }
                let key = parser.next_bytes()?.ok_or_else(missing_operand)?;
                let by = -parser.next_integer()?.ok_or_else(missing_operand)?;
                (key, by)
            }
        };
        Ok(Self { key, by })
    }

    pub fn exec(self, db: &mut DB) -> Frame {
        db.incr(&self.key, self.by)
    }
}

impl OneshotExecDB for Incr {
    fn get_key(&self) -> &[u8] {
        &self.key.as_ref()
    }
}

impl DB {
    fn incr(&mut self, key: &Bytes, by: i64) -> Frame {
        self.database
            .get_mut(key)
            .filter(|v| v.expiration.is_none() || v.expiration.unwrap() > Instant::now())
            .map_or_else(
                || Frame::NullString,
                |en| match &mut en.data {
                    Frame::BulkStrings(b) => match get_integer(b) {
                        Ok(v) => {
                            en.data = Frame::Integers(v + by);
                            return Frame::Integers(v + by);
                        }
                        Err(_) => {
                            return Frame::NullString;
                        }
                    },
                    Frame::Integers(i) => {
                        *i += by;
                        return Frame::Integers(*i);
                    }
                    _ => {
                        return Frame::NullString;
                    }
                },
            )
    }
}

impl AtomicCMDMarker for Incr {}

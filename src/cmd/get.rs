use crate::cmd::*;
use anyhow::Result;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct Get {
    key: Bytes,
    variant: GetVariant,
}

#[derive(Debug, Clone)]
pub enum GetVariant {
    Get,
    TTL,
    PTTL,
}

impl Get {
    pub fn new(parser: &mut CommandParser, variant: GetVariant) -> Result<Get> {
        Ok(Self {
            key: parser.next_bytes()?.ok_or_else(missing_operand)?,
            variant,
        })
    }
}

impl OneshotExecDB for Get {
    fn exec(self, db: &mut DB) -> Frame {
        match self.variant {
            GetVariant::Get => db.get(&self.key),
            GetVariant::TTL => db.ttl(&self.key, false),
            GetVariant::PTTL => db.ttl(&self.key, true),
        }
    }

    fn get_key(&self) -> &[u8] {
        &self.key.as_ref()
    }
}

impl DB {
    pub fn get(&self, key: &Bytes) -> Frame {
        self.database
            .get(key)
            .filter(|v| v.expiration.is_none() || v.expiration.unwrap() > Instant::now())
            .map_or_else(|| Frame::NullString, |v| v.data.clone())
    }

    fn ttl(&self, key: &Bytes, is_millis: bool) -> Frame {
        self.database
            .get(key)
            .filter(|v| v.expiration.is_none() || v.expiration.unwrap() > Instant::now())
            .map_or_else(
                || Frame::Integers(-2),
                |v| {
                    v.expiration.map_or_else(
                        || Frame::Integers(-1),
                        |v| {
                            Frame::Integers(if is_millis {
                                v.duration_since(Instant::now()).as_millis() as i64
                            } else {
                                v.duration_since(Instant::now()).as_secs() as i64
                            })
                        },
                    )
                },
            )
    }
}

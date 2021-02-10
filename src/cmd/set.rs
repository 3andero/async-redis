use crate::{cmd::*, db::DB};
use anyhow::Result;
use tokio::time::{Duration, Instant};
#[derive(Debug, Clone)]
pub struct Set {
    key: Box<Bytes>,
    val: Box<Bytes>,
    expiration: Option<u64>,
    nounce: u64,
}

impl Set {
    pub fn new(parser: &mut CommandParser) -> Result<Set> {
        let k = parser.next_bytes()?.ok_or_else(missing_operand)?;
        let v = parser.next_bytes()?.ok_or_else(missing_operand)?;
        let expire = match parser.next_integer()? {
            Some(v) => {
                if v < 0 {
                    return Err(Error::new(CommandError::InvalidOperand));
                }
                Some(v as u64)
            }
            None => None,
        };
        Ok(Self {
            key: k,
            val: v,
            expiration: expire,
            nounce: 0,
        })
    }
}

impl ExecDB for Set {
    fn exec(self, db: &mut DB) -> Frame {
        let now = Instant::now();
        let expiration = self.expiration.map(|v| now + Duration::new(v, 0));
        if expiration.is_none() {
            db.set(*self.key, *self.val, self.nounce, None);
            return Frame::Ok;
        }
        let expiration = expiration.unwrap();
        if expiration == now {
            return Frame::Ok;
        } else {
            db.set(
                *self.key.clone(),
                *self.val,
                self.nounce,
                Some(expiration),
            );
        }
        db.expiration
            .insert((expiration, self.nounce), *self.key);

        db.when
            .map_or_else(|| Some(expiration), |v| Some(v.min(expiration)));
        Frame::Ok
    }

    fn get_key(&self) -> &Bytes {
        &self.key
    }

    fn set_nounce(&mut self, nounce: u64) {
        self.nounce = nounce;
    }
}

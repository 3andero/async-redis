use crate::{cmd::*, db::DB};
use anyhow::Result;
use tokio::time::{Duration, Instant};
#[derive(Debug)]
pub struct Set {
    key: Bytes,
    val: Bytes,
    expiration: Option<u64>,
    nounce: u64,
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
            nounce: 0,
        })
    }
}

impl ExecDB for Set {
    fn exec(&self, db: &mut DB) -> Frame {
        let now = Instant::now();
        let expiration = self.expiration.map(|v| now + Duration::new(v, 0));
        if expiration.is_none() {
            db.set(self.key.clone(), self.val.clone(), self.nounce, None);
            return Frame::Ok;
        }
        let expiration = expiration.unwrap();
        if expiration == now {
            return Frame::Ok;
        } else {
            db.set(
                self.key.clone(),
                self.val.clone(),
                self.nounce,
                Some(expiration),
            );
        }
        db.expiration
            .insert((expiration, self.nounce), self.key.clone());

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

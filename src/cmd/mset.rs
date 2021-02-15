use crate::{cmd::*, db::DB};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct MSet {
    pairs: Vec<(Bytes, Bytes)>,
}

impl MSet {
    pub fn new(pairs: Vec<(Bytes, Bytes)>) -> Self {
        Self { pairs }
    }
}

impl OneshotExecDB for MSet {
    fn exec(self, db: &mut DB) -> Frame {
        let nounce0 = db.counter;
        db.counter += self.pairs.len() as u64;
        self.pairs.into_iter().fold(nounce0 + 1, |i, (k, v)| {
            db.set(k, v, i, None);
            i + 1
        });
        Frame::Ok
    }

    fn get_key(&self) -> &[u8] {
        &self.pairs[0].0.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct MSetDispatcher {
    pairs: Vec<(Bytes, Bytes)>,
    db_amount: usize,
    tbl: Vec<Vec<(Bytes, Bytes)>>,
    len: usize,
}

impl TraverseExecDB for MSetDispatcher {
    fn next_command(&mut self) -> IDCommandPair {
        let id = self.tbl.len() - 1;
        let v = self.tbl.pop().unwrap();
        if v.len() > 0 {
            return (id, Some(MSet::new(v).into()));
        } else {
            return (id, None);
        }
    }
    fn next_key(&self) -> Option<&Bytes> {
        self.pairs.last().map(|(b, _)| b)
    }
    fn init(&mut self, db_amount: usize) {
        self.db_amount = db_amount;
        self.tbl = vec![Vec::with_capacity(self.len / db_amount + 1); db_amount];
    }
    fn move_to(&mut self, db_id: usize) {
        let p = self.pairs.pop().unwrap();
        self.tbl[db_id].push(p);
    }

    fn len(&self) -> usize {
        1
    }
}

impl MSetDispatcher {
    pub fn new(parser: &mut CommandParser) -> Result<MSetDispatcher> {
        let len = parser.len() / 2;
        if len == 0 {
            return Err(Error::new(CommandError::MissingOperand));
        }
        let mut pairs = Vec::with_capacity(len);
        while let Some(p) = parser.next_bytes_pair()? {
            pairs.push(p);
        }

        Ok(Self {
            pairs,
            db_amount: 0,
            tbl: Vec::new(),
            len,
        })
    }
}

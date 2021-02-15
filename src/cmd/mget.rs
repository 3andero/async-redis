use crate::{cmd::*, db::DB};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct MGet {
    keys: Vec<Bytes>,
}

impl MGet {
    pub fn new(keys: Vec<Bytes>) -> MGet {
        Self { keys }
    }
}

impl OneshotExecDB for MGet {
    fn exec(self, db: &mut DB) -> Frame {
        self.keys
            .iter()
            .map(|v| db.get(v))
            .collect::<Vec<_>>()
            .into()
    }

    fn get_key(&self) -> &[u8] {
        &self.keys[0].as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct MGetDispatcher {
    keys: Vec<Bytes>,
    db_amount: usize,
    tbl: Vec<Vec<Bytes>>,
    len: usize,
}

impl TraverseExecDB for MGetDispatcher {
    fn next_command(&mut self) -> IDCommandPair {
        let id = self.tbl.len() - 1;
        let v = self.tbl.pop().unwrap();
        if v.len() > 0 {
            return (id, Some(MGet::new(v).into()));
        } else {
            return (id, None);
        }
    }
    fn next_key(&self) -> Option<&Bytes> {
        self.keys.last()
    }
    fn init(&mut self, db_amount: usize) {
        self.db_amount = db_amount;
        self.tbl = vec![Vec::with_capacity(self.len / db_amount + 1); db_amount];
    }
    fn move_to(&mut self, db_id: usize) {
        let p = self.keys.pop().unwrap();
        self.tbl[db_id].push(p);
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl MGetDispatcher {
    pub fn new(parser: &mut CommandParser) -> Result<MGetDispatcher> {
        if parser.len() == 0 {
            return Err(Error::new(CommandError::MissingOperand));
        }
        let mut keys = Vec::with_capacity(parser.len());
        while let Some(p) = parser.next_bytes()? {
            keys.push(p);
        }
        let len = keys.len();
        Ok(Self {
            keys,
            db_amount: 0,
            tbl: Vec::new(),
            len,
        })
    }
}

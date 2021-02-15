use crate::{cmd::*, db::DB};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct MSet {
    pairs: Vec<MiniCommand>,
}

impl MSet {
    pub fn new(pairs: Vec<MiniCommand>) -> Self {
        Self { pairs }
    }
}

impl OneshotExecDB for MSet {
    fn exec(self, db: &mut DB) -> Frame {
        let nounce0 = db.counter;
        db.counter += self.pairs.len() as u64;
        self.pairs
            .into_iter()
            .fold(nounce0 + 1, |i, cmd| {
                if let MiniCommand::Pair((k, v)) = cmd {
                    db.set(k, v, i, None);
                }
                i + 1
            });
        Frame::Ok
    }

    fn get_key(&self) -> &[u8] {
        &self.pairs[0].get_key()
    }
}

#[derive(Debug, Clone)]
pub struct MSetDispatcher {
    cmds: Vec<MiniCommand>,
    db_amount: usize,
    cmds_tbl: Vec<Vec<MiniCommand>>,
    len: usize,
    first_valid: bool,
}

impl TraverseExecDB for MSetDispatcher {
    fn next_command(&mut self) -> IDCommandPair {
        let id = self.cmds_tbl.len() - 1;
        let pairs = self.cmds_tbl.pop().unwrap();
        // let order = self.order_tbl.pop().unwrap();
        if pairs.len() > 0 {
            return (
                id,
                Some((
                    MSet::new(pairs).into(),
                    if self.first_valid {
                        self.first_valid = false;
                        MergeStrategy::Insert(0)
                    } else {
                        MergeStrategy::Drop
                    },
                )),
            );
        } else {
            return (id, None);
        }
    }

    fn len(&self) -> usize {
        1
    }

    fn init_tbls(&mut self, vec: &Vec<usize>) {
        self.cmds_tbl = vec.iter().map(|v| Vec::with_capacity(*v)).collect();
    }

    fn iter_data(&self) -> Iter<MiniCommand> {
        self.cmds.iter()
    }

    fn move_last_to(&mut self, db_id: usize, _: usize) {
        self.cmds_tbl[db_id].push(self.cmds.pop().unwrap());
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
            pairs.push(p.into());
        }

        Ok(Self {
            cmds: pairs,
            db_amount: 0,
            cmds_tbl: Vec::new(),
            len,
            first_valid: true,
        })
    }
}

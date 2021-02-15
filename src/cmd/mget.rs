use crate::{cmd::*, db::DB};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct MGet {
    keys: Vec<MiniCommand>,
}

impl MGet {
    pub fn new(keys: Vec<MiniCommand>) -> MGet {
        Self { keys }
    }
}

impl OneshotExecDB for MGet {
    fn exec(self, db: &mut DB) -> Frame {
        self.keys
            .iter()
            .map(|cmd| {
                if let MiniCommand::Single(v) = cmd {
                    return db.get(v);
                } else {
                    panic!()
                }
            })
            .collect::<Vec<_>>()
            .into()
    }

    fn get_key(&self) -> &[u8] {
        &self.keys[0].get_key()
    }
}

#[derive(Debug, Clone)]
pub struct MGetDispatcher {
    cmds: Vec<MiniCommand>,
    db_amount: usize,
    cmds_tbl: Vec<Vec<MiniCommand>>,
    order_tbl: Vec<Vec<usize>>,
    len: usize,
}

impl TraverseExecDB for MGetDispatcher {
    fn next_command(&mut self) -> IDCommandPair {
        let id = self.cmds_tbl.len() - 1;
        let v = self.cmds_tbl.pop().unwrap();
        let order = self.order_tbl.pop().unwrap();
        if v.len() > 0 {
            return (id, Some((MGet::new(v).into(), MergeStrategy::Reorder(order))));
        } else {
            return (id, None);
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn init_tbls(&mut self, vec: &Vec<usize>) {
        self.cmds_tbl = vec.iter().map(|v| Vec::with_capacity(*v)).collect();
        self.order_tbl = vec.iter().map(|v| Vec::with_capacity(*v)).collect();
    }

    fn iter_data(&self) -> Iter<MiniCommand> {
        self.cmds.iter()
    }

    fn move_last_to(&mut self, db_id: usize, original_idx: usize) {
        self.cmds_tbl[db_id].push(self.cmds.pop().unwrap());
        self.order_tbl[db_id].push(original_idx);
    }

}

impl MGetDispatcher {
    pub fn new(parser: &mut CommandParser) -> Result<MGetDispatcher> {
        if parser.len() == 0 {
            return Err(Error::new(CommandError::MissingOperand));
        }
        let mut cmds = Vec::with_capacity(parser.len());
        while let Some(p) = parser.next_bytes()? {
            cmds.push(p.into());
        }
        let len = cmds.len();
        Ok(Self {
            cmds,
            db_amount: 0,
            cmds_tbl: Vec::new(),
            order_tbl: Vec::new(),
            len,
        })
    }
}

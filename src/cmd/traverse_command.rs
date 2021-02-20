use crate::cmd::*;


#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum TraverseCommand {
    SendNReturn1,
    SendNReturnN,
    // MGet(MGetDispatcher),
    Dx(DxDispatcher),
}

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum MiniCommand {
    Pair(_Pair),
    Single(_Single),
}

#[enum_dispatch(MiniCommand)]
pub trait MiniCommandTrait {
    fn get_key(&self) -> &[u8];
}

pub type _Pair = (Bytes, Frame);

impl MiniCommandTrait for _Pair {
    fn get_key(&self) -> &[u8] {
        return self.0.as_ref();
    }
}

pub type _Single = Bytes;

impl MiniCommandTrait for _Single {
    fn get_key(&self) -> &[u8] {
        return self.as_ref();
    }
}

pub type IDCommandPair = (usize, Option<OneshotCommand>);

#[enum_dispatch(TraverseCommand)]
pub trait DispatchToMultipleDB {
    fn len(&self) -> usize;
    fn next_command(&mut self) -> IDCommandPair;
    fn iter_data(&self) -> Iter<MiniCommand>;
    fn move_last_to(&mut self, db_id: usize, original_idx: usize);
    fn init_tbls(&mut self, vec: &Vec<usize>);
    fn get_result_collector(&mut self) -> ResultCollector;
    fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize) {
        let mut tbl_len = vec![0; db_amount];
        let db_ids: Vec<usize> = self
            .iter_data()
            .map(|v| {
                let id = dispatch_fn(v.get_key());
                tbl_len[id] += 1 as usize;
                id
            })
            .collect();

        self.init_tbls(&tbl_len);

        let mut order = db_ids.len();
        for _ in 0..db_ids.len() {
            order -= 1;
            self.move_last_to(db_ids[order], order);
        }
    }
}

#[derive(Debug, Clone)]
pub enum TraverseVariant {
    MSet,
    MGet,
    // Subscribe,
}

impl TraverseVariant {
    fn new(&self, mini_cmds: Vec<MiniCommand>) -> OneshotCommand {
        match self {
            TraverseVariant::MSet => MSet::new(mini_cmds).into(),
            TraverseVariant::MGet => MGet::new(mini_cmds).into(),
        }
    }
}

#[macro_export]
macro_rules! define_dispatch_to_multiple_db {
    () => {
        cmds: Vec<MiniCommand>,
        db_amount: usize,
        cmds_tbl: Vec<Vec<MiniCommand>>,
        len: usize,
    };
}

#[derive(Debug, Clone)]
pub struct SendNReturn1 {
    cmds: Vec<MiniCommand>,
    db_amount: usize,
    cmds_tbl: Vec<Vec<MiniCommand>>,
    len: usize,
}

#[macro_export]
macro_rules! impl_dispatch_to_multiple_db {
    (Consts, $corresponding_cmd:ident) => {
        fn next_command(&mut self) -> IDCommandPair {
            let id = self.cmds_tbl.len() - 1;
            let cmd = self
                .cmds_tbl
                .pop()
                .filter(|v| v.len() > 0)
                .map(|v| $corresponding_cmd::new(v).into());
            (id, cmd)
        }

        fn iter_data(&self) -> Iter<MiniCommand> {
            self.cmds.iter()
        }
    };
    (SendNReturn1, $target:ident, $corresponding_cmd:ident) => {
        impl DispatchToMultipleDB for SendNReturn1 {
            impl_dispatch_to_multiple_db!(Consts, corresponding_cmd);
        
            fn get_result_collector(&mut self) -> ResultCollector {
                ResultCollector::KeepFirst(1)
            }
        
            fn len(&self) -> usize {
                1
            }
        
            fn init_tbls(&mut self, vec: &Vec<usize>) {
                self.cmds_tbl = vec.iter().map(|v| Vec::with_capacity(*v)).collect();
            }
        
            fn move_last_to(&mut self, db_id: usize, _: usize) {
                self.cmds_tbl[db_id].push(self.cmds.pop().unwrap());
            }
        }
    };
}


impl DispatchToMultipleDB for SendNReturn1 {
    impl_dispatch_to_multiple_db!(Consts, MSet);
    // fn next_command(&mut self) -> IDCommandPair {
    //     let id = self.cmds_tbl.len() - 1;
    //     let cmd = self
    //         .cmds_tbl
    //         .pop()
    //         .filter(|v| v.len() > 0)
    //         .map(|v| self.variant.new(v).into());
    //     (id, cmd)
    // }

    fn get_result_collector(&mut self) -> ResultCollector {
        ResultCollector::KeepFirst(1)
    }

    fn len(&self) -> usize {
        1
    }

    fn init_tbls(&mut self, vec: &Vec<usize>) {
        self.cmds_tbl = vec.iter().map(|v| Vec::with_capacity(*v)).collect();
    }

    // fn iter_data(&self) -> Iter<MiniCommand> {
    //     self.cmds.iter()
    // }

    fn move_last_to(&mut self, db_id: usize, _: usize) {
        self.cmds_tbl[db_id].push(self.cmds.pop().unwrap());
    }
}

impl SendNReturn1 {
    pub fn new(parser: &mut CommandParser) -> Result<SendNReturn1> {
        let len = parser.len() / 2;
        if len == 0 {
            return Err(Error::new(CommandError::MissingOperand));
        }
        let mut pairs = Vec::with_capacity(len);
        while let Some(p) = parser.next_kv_pair()? {
            pairs.push(p.into());
        }

        Ok(Self {
            cmds: pairs,
            db_amount: 0,
            cmds_tbl: Vec::new(),
            len,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SendNReturnN {
    cmds: Vec<MiniCommand>,
    db_amount: usize,
    cmds_tbl: Vec<Vec<MiniCommand>>,
    order_tbl: Vec<Vec<usize>>,
    len: usize,
    variant: TraverseVariant,
}

impl DispatchToMultipleDB for SendNReturnN {
    fn next_command(&mut self) -> IDCommandPair {
        let id = self.cmds_tbl.len() - 1;
        let cmd = self
            .cmds_tbl
            .pop()
            .filter(|v| v.len() > 0)
            .map(|v| self.variant.new(v).into());
        (id, cmd)
    }

    fn get_result_collector(&mut self) -> ResultCollector {
        ResultCollector::Reorder(std::mem::take(&mut self.order_tbl))
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

impl SendNReturnN {
    pub fn new(parser: &mut CommandParser, variant: TraverseVariant) -> Result<SendNReturnN> {
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
            variant,
        })
    }
}

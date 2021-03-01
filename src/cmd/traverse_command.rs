use crate::cmd::*;

#[enum_dispatch]
#[derive(Debug, Clone)]
pub enum TraverseCommand {
    MSet(MSetDispatcher),
    MGet(MGetDispatcher),
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

pub type IDCommandPair = (usize, AtomicCommand);

#[enum_dispatch(TraverseCommand)]
pub trait DispatchToMultipleDB {
    fn len(&self) -> usize;
    fn next_command(&mut self) -> IDCommandPair;
    fn get_result_collector(&mut self) -> ResultCollector;
    fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize);
}

#[derive(Debug)]
pub enum AtomicCommand {
    Oneshot(OneshotCommand),
    PubSub(PubSubCommand),
    None,
}

impl AtomicCommand {
    pub fn is_none(&self) -> bool {
        use AtomicCommand::*;
        match self {
            None => true,
            _ => false,
        }
    }

    pub fn unwrap_oneshot(self) -> OneshotCommand {
        match self {
            AtomicCommand::Oneshot(c) => c,
            _ => panic!(),
        }
    }

    pub fn unwrap_pubsub(self) -> PubSubCommand {
        match self {
            AtomicCommand::PubSub(c) => c,
            _ => panic!(),
        }
    }
}

impl From<Option<OneshotCommand>> for AtomicCommand {
    fn from(cmd: Option<OneshotCommand>) -> Self {
        match cmd {
            None => AtomicCommand::None,
            Some(c) => AtomicCommand::Oneshot(c),
        }
    }
}

impl From<Option<PubSubCommand>> for AtomicCommand {
    fn from(cmd: Option<PubSubCommand>) -> Self {
        match cmd {
            None => AtomicCommand::None,
            Some(c) => AtomicCommand::PubSub(c),
        }
    }
}

#[macro_export]
macro_rules! new_traverse_command {
    (@Construct, SendNReturn1, $cmds:ident, $len:ident) => {
        Ok(Self {
            cmds: $cmds,
            db_amount: 0,
            cmds_tbl: Vec::new(),
            len: $len,
            has_operand: $len > 0,
            ..Default::default()
        })
    };
    (@Construct, SendNReturnN, $cmds:ident, $len:ident) => {
        Ok(Self {
            cmds: $cmds,
            db_amount: 0,
            cmds_tbl: Vec::new(),
            order_tbl: Vec::new(),
            len: $len,
            has_operand: $len > 0,
            ..Default::default()
        })
    };
    (@EmptyGuard, +, $len:ident) => {
        if $len == 0 {
            return Err(Error::new(CommandError::MissingOperand));
        }
    };
    (@EmptyGuard, *, $len:ident) => {};
    (KeyValue$allow_empty:tt, $type:ident, $target:ident) => {
        impl $target {
            pub fn new(parser: &mut CommandParser) -> Result<$target> {
                let len = parser.len() / 2;
                new_traverse_command!(@EmptyGuard, $allow_empty, len);
                let mut cmds = Vec::with_capacity(len);
                while let Some(p) = parser.next_kv_pair()? {
                    cmds.push(p.into());
                }

                new_traverse_command!(@Construct, $type, cmds, len)
            }
        }
    };
    (Key$allow_empty:tt, $type:ident, $target:ident) => {
        impl $target {
            pub fn new(parser: &mut CommandParser) -> Result<$target> {
                let len = parser.len();
                new_traverse_command!(@EmptyGuard, $allow_empty, len);
                let mut cmds = Vec::with_capacity(len);
                while let Some(p) = parser.next_bytes()? {
                    cmds.push(p.into());
                }
                new_traverse_command!(@Construct, $type, cmds, len)
            }
        }
    };
}

#[macro_export]
macro_rules! default_pop {
    ($self:ident) => {
        $self.cmds_tbl.pop().filter(|v| v.len() > 0)
    };
}

#[macro_export]
macro_rules! impl_traverse_command {
    (@Consts, $corresponding_cmd:ident, $atomic_type:ident, $pop:ident) => {
        fn next_command(&mut self) -> IDCommandPair {
            let id = self.cmds_tbl.len() - 1;
            let cmd = $pop!(self)
                .map(|v| $atomic_type::from($corresponding_cmd::new(v))).into();
            (id, cmd)
        }
    };

    (SendNReturn1, ($mini_command_type:ident)$allow_empty:tt, $target:ident, $corresponding_cmd:ident, $atomic_type:ident) => {
        impl_traverse_command!(SendNReturn1, ($mini_command_type)$allow_empty, $target, $corresponding_cmd, $atomic_type, default_pop);
    };
    (SendNReturnN, ($mini_command_type:ident)$allow_empty:tt, $target:ident, $corresponding_cmd:ident, $atomic_type:ident) => {
        impl_traverse_command!(SendNReturnN, ($mini_command_type)$allow_empty, $target, $corresponding_cmd, $atomic_type, default_pop);
    };

    (SendNReturn1, ($mini_command_type:ident)$allow_empty:tt, $target:ident, $corresponding_cmd:ident, $atomic_type:ident, $pop:ident) => {

        crate::new_traverse_command!($mini_command_type$allow_empty, SendNReturn1, $target);

        impl DispatchToMultipleDB for $target {
            impl_traverse_command!(@Consts, $corresponding_cmd, $atomic_type, $pop);

            fn get_result_collector(&mut self) -> ResultCollector {
                ResultCollector::KeepFirst(1)
            }

            fn len(&self) -> usize {
                1
            }

            fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize) {
                let mut tbl_len = vec![0; db_amount];
                let mut db_ids: Vec<usize> = self
                    .cmds.iter()
                    .map(|v| {
                        let id = dispatch_fn(v.get_key());
                        tbl_len[id] += 1 as usize;
                        id
                    })
                    .collect();

                self.cmds_tbl = tbl_len.iter().map(|v| Vec::with_capacity(*v)).collect();

                while let Some(db_id) = db_ids.pop() {
                    self.cmds_tbl[db_id].push(self.cmds.pop().unwrap());
                }
            }
        }
    };

    (SendNReturnN, ($mini_command_type:ident)$allow_empty:tt, $target:ident, $corresponding_cmd:ident, $atomic_type:ident, $pop:ident) => {

        crate::new_traverse_command!($mini_command_type$allow_empty, SendNReturnN, $target);

        impl DispatchToMultipleDB for $target {
            impl_traverse_command!(@Consts, $corresponding_cmd, $atomic_type, $pop);

            fn get_result_collector(&mut self) -> ResultCollector {
                ResultCollector::Reorder(std::mem::take(&mut self.order_tbl))
            }

            fn len(&self) -> usize {
                self.len
            }

            fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize) {
                let mut tbl_len = vec![0; db_amount];
                let mut db_ids: Vec<usize> = self
                    .cmds.iter()
                    .map(|v| {
                        let id = dispatch_fn(v.get_key());
                        tbl_len[id] += 1 as usize;
                        id
                    })
                    .collect();

                self.cmds_tbl = tbl_len.iter().map(|v| Vec::with_capacity(*v)).collect();
                self.order_tbl = tbl_len.iter().map(|v| Vec::with_capacity(*v)).collect();

                while let Some(db_id) = db_ids.pop() {
                    self.cmds_tbl[db_id].push(self.cmds.pop().unwrap());
                    self.order_tbl[db_id].push(db_ids.len());
                }
            }
        }
    };
}

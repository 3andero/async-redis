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
    // fn len(&self) -> usize;
    fn next_command(&mut self) -> Option<IDCommandPair>;
    fn get_result_collector(&mut self) -> ResultCollector;
    fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize);
}

#[derive(Debug)]
pub enum AtomicCommand {
    Oneshot(OneshotCommand),
    PubSub(PubSubCommand),
    // None,
}

impl AtomicCommand {
    // pub fn is_none(&self) -> bool {
    //     use AtomicCommand::*;
    //     match self {
    //         None => true,
    //         _ => false,
    //     }
    // }

    pub fn unwrap_oneshot(self) -> OneshotCommand {
        match self {
            AtomicCommand::Oneshot(c) => c,
            _ => panic!("This is not a `Oneshot` Command"),
        }
    }

    pub fn unwrap_pubsub(self) -> PubSubCommand {
        match self {
            AtomicCommand::PubSub(c) => c,
            _ => panic!("This is not a `PubSub` Command"),
        }
    }
}

impl From<OneshotCommand> for AtomicCommand {
    fn from(cmd: OneshotCommand) -> Self {
        AtomicCommand::Oneshot(cmd)
    }
}

impl From<PubSubCommand> for AtomicCommand {
    fn from(cmd: PubSubCommand) -> Self {
        AtomicCommand::PubSub(cmd)
    }
}

pub(in crate::cmd) unsafe fn new_unsafe_vec(expected_amount_ret: usize) -> Vec<Frame> {
    assert!(expected_amount_ret > 0);
    let mut ret: Vec<Frame> = Vec::with_capacity(expected_amount_ret);
    ret.set_len(expected_amount_ret);
    ret
}

#[macro_export]
macro_rules! new_traverse_command {
    (@Construct, SendNReturn1, $cmds:ident, $len:ident) => {
        Ok(Self {
            cmds: $cmds,
            len: $len,
            has_operand: $len > 0,
            ..Default::default()
        })
    };
    (@Construct, SendNReturnN, $cmds:ident, $len:ident) => {
        Ok(Self {
            cmds: $cmds,
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
    ($self:ident) => {{
        assert!(
            $self.cmds_tbl.len() > 0,
            "self.cmds_tbl was not properly initialized"
        );
        $self.cmds_tbl.pop().filter(|v| v.len() > 0)
    }};
}

#[macro_export]
macro_rules! impl_traverse_command {
    (@Consts, $corresponding_cmd:ident, $atomic_type:ident, $pop:ident) => {
        fn next_command(&mut self) -> Option<IDCommandPair> {
            while self.db_amount > 0 {
                self.db_amount -= 1;
                if let Some(v) = $pop!(self) {
                    return Some((self.db_amount, $atomic_type::from($corresponding_cmd::new(v)).into()));
                }
            }
            None
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
                let ret = unsafe {
                    new_unsafe_vec(1)
                };
                ResultCollector {
                    result_type: ResultCollectorType::KeepFirst(1),
                    ret,
                }
            }

            // fn len(&self) -> usize {
            //     1
            // }

            fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize) {
                self.db_amount = db_amount;
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
                assert!(self.db_amount > 0, "self.db_amount should not be 0");
                assert!(self.order_tbl.len() == self.db_amount, "self.order_tbl.len() should not be 0");
                let ret = unsafe {
                    new_unsafe_vec(self.len)
                };
                ResultCollector {
                    result_type: ResultCollectorType::Reorder(std::mem::take(&mut self.order_tbl)),
                    ret,
                }
            }

            // fn len(&self) -> usize {
            //     self.len
            // }

            fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize) {
                self.db_amount = db_amount;
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

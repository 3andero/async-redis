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

pub type IDCommandPair = (usize, AtomicCMD);

#[enum_dispatch(TraverseCommand)]
pub trait DispatchToMultipleDB {
    fn next_command(&mut self) -> Option<IDCommandPair>;
    fn get_result_collector(&mut self) -> ResultCollector;
    fn dispatch(&mut self, db_amount: usize, dispatch_fn: impl Fn(&[u8]) -> usize);
}

pub(in crate::cmd) unsafe fn new_unsafe_vec(expected_amount_ret: usize) -> Vec<Frame> {
    assert!(expected_amount_ret > 0);
    let mut ret: Vec<Frame> = Vec::with_capacity(expected_amount_ret);
    ret.set_len(expected_amount_ret);
    ret
}

#[macro_export]
macro_rules! new_traverse_command {
    (@Construct, Return1, $cmds:ident, $len:ident) => {
        Ok(Self {
            cmds: $cmds,
            len: $len,
            has_operand: $len > 0,
            ..Default::default()
        })
    };
    (@Construct, ReturnN, $cmds:ident, $len:ident) => {
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
    (@Consts, $corresponding_cmd:ident, $pop:ident) => {
        fn next_command(&mut self) -> Option<IDCommandPair> {
            while self.db_amount > 0 {
                self.db_amount -= 1;
                if let Some(v) = $pop!(self) {
                    return Some((self.db_amount, $corresponding_cmd::new(v).into()));
                }
            }
            None
        }
    };

    (for cmd: $atomic_cmd:ident = $dispatcher:ident(($token_stream_schema:ident)$repetition:tt).$pop:ident!() {
        cmd >> DB
    }, DB >> N Frame(s) $(>> Sum)?) => {
        crate::new_traverse_command!($token_stream_schema$repetition, ReturnN, $dispatcher);

        impl DispatchToMultipleDB for $dispatcher {
            impl_traverse_command!(@Consts, $atomic_cmd, $pop);

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

    (for cmd: $atomic_cmd:ident = $dispatcher:ident(($token_stream_schema:ident)$repetition:tt).$pop:ident!() {
        cmd >> DB
    }, DB >> 1 Frame) => {
        crate::new_traverse_command!($token_stream_schema$repetition, Return1, $dispatcher);

        impl DispatchToMultipleDB for $dispatcher {
            impl_traverse_command!(@Consts, $atomic_cmd, $pop);

            fn get_result_collector(&mut self) -> ResultCollector {
                let ret = unsafe {
                    new_unsafe_vec(1)
                };
                ResultCollector {
                    result_type: ResultCollectorType::KeepFirst(1),
                    ret,
                }
            }

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
}

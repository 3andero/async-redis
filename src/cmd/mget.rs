use crate::{cmd::*, db::DB, impl_traverse_command, new_traverse_command, *};
use async_redis::*;

#[derive(Debug, Clone)]
pub struct MGet {
    keys: Vec<MiniCommand>,
}

impl MGet {
    pub fn new(keys: Vec<MiniCommand>) -> MGet {
        Self { keys }
    }

    pub fn exec(self, db: &mut DB) -> Frame {
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
}

impl OneshotExecDB for MGet {
    fn get_key(&self) -> &[u8] {
        &self.keys[0].get_key()
    }
}

#[define_traverse_command("N:N")]
#[derive(Debug, Clone, Default)]
pub struct MGetDispatcher {}

use crate::default_pop;
impl_traverse_command!(
    for cmd: MGet = MGetDispatcher((Key)+).default_pop!() {
        cmd >> DB
    },
    DB >> N Frame(s)
);

impl AtomicCMDMarker for MGet {}

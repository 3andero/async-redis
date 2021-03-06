use crate::{cmd::*, db::DB, impl_traverse_command, new_traverse_command, *};
use async_redis::*;

#[derive(Debug, Clone)]
pub struct MSet {
    cmds: Vec<MiniCommand>,
}

impl MSet {
    pub fn new(cmds: Vec<MiniCommand>) -> Self {
        Self { cmds }
    }
}

impl OneshotExecDB for MSet {
    fn exec(self, db: &mut DB) -> Frame {
        let nounce0 = db.counter;
        db.counter += self.cmds.len() as u64;
        self.cmds.into_iter().fold(nounce0 + 1, |i, cmd| {
            if let MiniCommand::Pair((k, v)) = cmd {
                db.set_lite(k, v, i, None);
            }
            i + 1
        });
        Frame::Ok
    }

    fn get_key(&self) -> &[u8] {
        &self.cmds[0].get_key()
    }
}

#[define_traverse_command("N:1")]
#[derive(Debug, Clone, Default)]
pub struct MSetDispatcher {}

use crate::default_pop;
impl_traverse_command!(
    for cmd: MSet = MSetDispatcher((KeyValue)+).default_pop!() {
        cmd >> DB
    },
    DB >> 1 Frame
);

crate::impl_into_atomic_cmd!(MSet, OneshotCommand);

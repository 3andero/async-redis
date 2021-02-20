use crate::{cmd::*, db::DB};

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
        self.pairs.into_iter().fold(nounce0 + 1, |i, cmd| {
            if let MiniCommand::Pair((k, v)) = cmd {
                db.set_lite(k, v, i, None);
            }
            i + 1
        });
        Frame::Ok
    }

    fn get_key(&self) -> &[u8] {
        &self.pairs[0].get_key()
    }
}

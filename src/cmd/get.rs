use crate::cmd::*;

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(parser: &mut Parser) -> crate::Result<Get> {
        Ok(Self {
            key: parser.next_string()?,
        })
    }
}

impl ExecDB for Get {
    fn exec(&self, db: &DB) -> Frame {
        db.get(&self.key).map_or(Frame::Null, |v| v.into())
    }
}

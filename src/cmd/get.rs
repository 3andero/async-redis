use crate::cmd::*;

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

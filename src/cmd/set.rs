use crate::cmd::*;

pub struct Set {
    key: String,
    val: Bytes,
}

impl Set {
    pub fn new(parser: &mut Parser) -> crate::Result<Set> {
        let k = parser.next_string()?;
        let v = parser.next_bytes()?;
        Ok(Self { key: k, val: v })
    }
}

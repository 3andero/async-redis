use crate::{protocol::*, utils::get_integer};

use intermediate_parsing::*;
use reusable_buf::*;

#[derive(Debug)]
pub struct IntermediateParser {
    token_stack: Vec<IntermediateToken>,
    elem_cnt: usize,
}

impl IntermediateParser {
    pub fn new() -> Self {
        Self {
            token_stack: Vec::with_capacity(2),
            elem_cnt: 0,
        }
    }

    pub fn parse(&mut self, buf: &mut ReusableBuf) -> FrameResult<Frame> {
        loop {
            // println!("stack: {:?}", self.token_stack);
            if self.token_stack.len() == 0
                || !self.token_stack.last().unwrap().has_raw_bytes_remain()
            {
                if buf.len() == 0 {
                    return Err(FrameError::Incomplete);
                }
                self.elem_cnt += 1;
                let token_type = buf[0];
                buf.advance(1);
                self.token_stack.push(IntermediateToken::new(token_type, self.elem_cnt < 4));
            }

            // println!("stack: {:?}", self.token_stack);
            self.token_stack
                .last_mut()
                .unwrap()
                .consume_raw_bytes(buf)?;

            while !self.token_stack.last_mut().unwrap().has_token_remain() {
                // println!("stack: {:?}", self.token_stack);
                let last_token = self.token_stack.pop().unwrap();
                if self.token_stack.len() == 0 {
                    self.elem_cnt = 0;
                    return last_token.into_frame();
                } else {
                    self.token_stack
                        .last_mut()
                        .unwrap()
                        .consume_token(last_token)?;
                }
            }
        }
    }
}

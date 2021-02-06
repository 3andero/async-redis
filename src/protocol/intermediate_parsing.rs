use crate::protocol::*;
use bytes::Bytes;
use reusable_buf::ReusableBuf;
use std::io::Cursor;

#[derive(Debug)]
pub struct IntermediateToken {
    token_type: u8,
    expected_len: Option<usize>,
    recognized_len: Option<u64>,
    is_recognized: bool,
    is_complete: bool,
    data: Option<Frame>,
}

impl IntermediateToken {
    pub fn new(token_type: u8) -> Self {
        // println!("new token: {}", token_type as char);
        Self {
            token_type,
            expected_len: None,
            recognized_len: None,
            is_recognized: false,
            is_complete: false,
            data: None,
        }
    }

    pub fn has_raw_bytes_remain(&self) -> bool {
        !self.is_recognized
    }
    pub fn has_token_remain(&self) -> bool {
        !self.is_complete
    }

    fn read_line(&mut self, buf: &mut ReusableBuf) -> FrameResult<Bytes> {
        if buf.len() < 2 {
            return Err(FrameError::Incomplete);
        }
        let mut cursor = Cursor::new(&buf[..]);
        if self.recognized_len.is_some() {
            let pos = self.recognized_len.take().unwrap();
            // println!("prev position: {}", pos);
            if cursor.remaining() < pos as usize {
                return Err(FrameError::Invalid(format!(
                    "[0] pos: {}, buf: {:?}",
                    pos, buf
                )));
            } else {
                // println!("set pos: {}", pos);
                cursor.set_position(pos);
            }
        }
        let next_line = get_line(&mut cursor).map_err(|e| match e {
            FrameError::Incomplete => {
                self.recognized_len = Some(cursor.position());
                e
            }
            e => e,
        })?;
        // println!("next_line: {:?}", Bytes::copy_from_slice(next_line));
        let ret = Ok(Bytes::copy_from_slice(next_line));
        let advance_pos = (cursor.position() + 2) as usize; // double check
        drop(cursor);
        buf.advance(advance_pos as usize);
        // println!("buf remains: {:?}", buf);
        ret
    }

    fn read_span(&mut self, span: usize, buf: &mut ReusableBuf) -> FrameResult<Bytes> {
        if buf.len() < span + 2 {
            return Err(FrameError::Incomplete);
        }
        if &buf.chunk()[span..span + 2] == b"\r\n" {
            let ret = Ok(Bytes::copy_from_slice(&buf.chunk()[..span]));
            buf.advance(span + 2);
            return ret;
        } else {
            return Err(FrameError::Invalid(String::from("[1]")));
        }
    }

    pub fn consume_raw_bytes(&mut self, buf: &mut ReusableBuf) -> FrameResult<()> {
        // println!("token: {}, buf: {:?}", self.token_type as char, buf);
        match self.token_type {
            SIMPLE_STRING_MARK => {
                self.data = Some(Frame::SimpleString(self.read_line(buf)?));
                self.is_recognized = true;
                self.is_complete = true;
            }
            ERROR_MARK => {
                self.data = Some(Frame::Errors(self.read_line(buf)?));
                self.is_recognized = true;
                self.is_complete = true;
            }
            BULK_STRING_MARK => {
                if self.expected_len.is_none() {
                    let next_line = self.read_line(buf)?;
                    let maybe_len = get_integer(&next_line)?;
                    if maybe_len < 0 {
                        self.is_recognized = true;
                        self.is_complete = true;
                        self.data = Some(Frame::NullString);
                        return Ok(());
                    } else {
                        self.expected_len = Some(maybe_len as usize);
                    }
                }

                let span = self.expected_len.unwrap();
                self.data = Some(Frame::BulkStrings(self.read_span(span, buf)?));
                self.is_complete = true;
                self.is_recognized = true;
            }
            INTEGER_MARK => {
                let next_line = self.read_line(buf)?;
                self.data = Some(Frame::Integers(get_integer(&next_line)?));
                self.is_complete = true;
                self.is_recognized = true;
            }
            ARRAY_MARK => {
                let next_line = self.read_line(buf)?;
                let maybe_len = get_integer(&next_line)?;
                if maybe_len < 0 {
                    self.is_recognized = true;
                    self.is_complete = true;
                    self.data = Some(Frame::NullArray);
                    return Ok(());
                } else {
                    self.expected_len = Some(maybe_len as usize);
                    self.is_recognized = true;
                    if maybe_len == 0 {
                        self.is_complete = true;
                    }
                    self.data = Some(Frame::Arrays(FrameArrays {
                        val: Vec::with_capacity(maybe_len as usize),
                        _encode_length: 0,
                    }));
                }
            }
            _ => {
                return Err(FrameError::NotImplemented);
            }
        }

        Ok(())
    }

    pub fn consume_token(&mut self, token: IntermediateToken) -> FrameResult<()> {
        // println!("consume token: {:?}", &token);
        let token = token.into_frame()?;
        match (self.token_type, self.data.as_mut(), self.expected_len) {
            (ARRAY_MARK, Some(Frame::Arrays(FrameArrays { val, .. })), Some(len)) => {
                if val.len() < len {
                    val.push(token);
                }

                if val.len() == len {
                    self.is_complete = true;
                }
            }
            _ => {
                return Err(FrameError::Invalid(String::from("[2]")));
            }
        }
        Ok(())
    }

    pub fn into_frame(self) -> FrameResult<Frame> {
        return self
            .data
            .ok_or_else(|| FrameError::Invalid(String::from("[3]")));
    }
}

fn get_line<'a>(cursor: &mut Cursor<&'a [u8]>) -> FrameResult<&'a [u8]> {
    if cursor.remaining() < 2 {
        return Err(FrameError::Incomplete);
    }
    let start = cursor.position() as usize;
    let end = cursor.get_ref().len() - 1;
    // println!("get_line start {}, end {}", start, end);

    for i in (start..end) {
        if cursor.get_ref()[i] == b'\r' && cursor.get_ref()[i + 1] == b'\n' {
            cursor.set_position(i as u64);
            return Ok(&cursor.get_ref()[..i]);
        }
    }

    cursor.set_position(end as u64);
    Err(FrameError::Incomplete)
}

pub fn get_integer(line: &Bytes) -> FrameResult<i64> {
    let (neg, line) = if line.len() == 0 {
        return Err("Not Digit".into());
    } else if line[0] == b'-' {
        (true, &line[1..])
    } else {
        (false, &line[..])
    };
    let mut res = 0;
    for v in line {
        if *v >= b'0' && *v <= b'9' {
            res = res * 10 + ((*v - b'0') as i64);
        } else {
            return Err("Not Digit".into());
        }
    }

    Ok(if neg { -res } else { res })
}

use crate::protocol::*;
use bytes::Bytes;
use reusable_buf::ReusableBuf;

#[derive(Debug)]
pub struct IntermediateToken {
    token_type: u8,
    expected_len: Option<usize>,
    is_recognized: bool,
    is_complete: bool,
    data: Option<Frame>,
    tmp_buf: BytesMut,
    num_buf: i64,
    num_sign: i8,
}

impl IntermediateToken {
    pub fn new(token_type: u8) -> Self {
        // println!("new token: {}", token_type as char);
        Self {
            token_type,
            expected_len: None,
            is_recognized: false,
            is_complete: false,
            data: None,
            tmp_buf: BytesMut::new(),
            num_buf: 0,
            num_sign: 0,
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
        let (length, is_complete) = get_line(&buf[..]);
        // println!("next_line: {:?}", Bytes::copy_from_slice(next_line));
        if is_complete {
            let ret = if self.tmp_buf.len() == 0 {
                Ok(Bytes::copy_from_slice(&buf[..length]))
            } else {
                self.tmp_buf.extend_from_slice(&buf[..length]);
                Ok(std::mem::take(&mut self.tmp_buf).freeze())
            };

            buf.advance(length + 2);
            return ret;
        }

        self.tmp_buf.extend_from_slice(&buf[..length]);
        buf.advance(length);
        // println!("buf remains: {:?}", buf);
        return Err(FrameError::Incomplete);
    }

    fn read_i64(&mut self, buf: &mut ReusableBuf) -> FrameResult<i64> {
        if self.num_sign == 0 {
            if buf[0] != b'-' {
                self.num_sign = 1;
            } else {
                self.num_sign = -1;
                buf.advance(1);
            }
        }

        for i in 0..buf.len() - 1 {
            if buf[i] >= b'0' && buf[i] <= b'9' {
                self.num_buf = self.num_buf * 10 + (buf[i] - b'0') as i64;
            } else if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                buf.advance(i + 2);
                let res = self.num_buf * self.num_sign as i64;
                self.num_sign = 0;
                self.num_buf = 0;
                return Ok(res);
            } else {
                return Err(FrameError::Invalid(String::from("char")));
            }
        }

        buf.advance(buf.len() - 1);
        return Err(FrameError::Incomplete);
    }

    fn skip_to_next_line(&mut self, buf: &mut ReusableBuf) -> FrameResult<()> {
        if buf.len() < 2 {
            return Err(FrameError::Incomplete);
        }
        for i in 0..buf.len() - 1 {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                buf.advance(i + 2);
                return Ok(());
            }
        }
        buf.advance(buf.len() - 1);
        return Ok(());
    }

    fn read_expected(&mut self, buf: &mut ReusableBuf) -> FrameResult<Bytes> {
        assert!(self.expected_len.is_some());
        let span = self.expected_len.unwrap();
        if buf.len() < span + 2 {
            let max_copy_cnt = std::cmp::min(buf.len(), span);
            self.tmp_buf.extend_from_slice(&buf[..max_copy_cnt]);
            self.expected_len = Some(span - max_copy_cnt);
            buf.advance(max_copy_cnt);
            buf.slide();
            return Err(FrameError::Incomplete);
        }
        if &buf.chunk()[span..span + 2] == b"\r\n" {
            self.tmp_buf.extend_from_slice(&buf[..span]);
            buf.advance(span + 2);
            return Ok(std::mem::take(&mut self.tmp_buf).freeze());
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
                    let maybe_len = self.read_i64(buf)?;
                    if maybe_len < 0 {
                        self.is_recognized = true;
                        self.is_complete = true;
                        self.data = Some(Frame::NullString);
                        return Ok(());
                    } else {
                        self.expected_len = Some(maybe_len as usize);
                        self.tmp_buf = BytesMut::with_capacity(maybe_len as usize);
                    }
                }

                self.data = Some(Frame::BulkStrings(self.read_expected(buf)?));
                self.is_complete = true;
                self.is_recognized = true;
            }
            INTEGER_MARK => {
                self.data = Some(Frame::Integers(self.read_i64(buf)?));
                self.is_complete = true;
                self.is_recognized = true;
            }
            ARRAY_MARK => {
                let maybe_len = self.read_i64(buf)?;
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
                    self.data = Some(Frame::Arrays(Vec::with_capacity(maybe_len as usize)));
                }
            }
            x => {
                self.skip_to_next_line(buf)?;
                return Err(FrameError::NotImplemented(x));
            }
        }

        Ok(())
    }

    pub fn consume_token(&mut self, token: IntermediateToken) -> FrameResult<()> {
        // println!("consume token: {:?}", &token);
        let token = token.into_frame()?;
        match (self.token_type, self.data.as_mut(), self.expected_len) {
            (ARRAY_MARK, Some(Frame::Arrays(val)), Some(len)) => {
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

fn get_line<'a>(buf: &'a [u8]) -> (usize, bool) {
    if buf.remaining() < 2 {
        return (0, false);
    }

    let end = buf.len() - 1;
    // println!("get_line start {}, end {}", start, end);

    for i in 0..end {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return (i, true);
        }
    }

    let tail = end + (buf[end] != b'\r') as usize;
    (tail, false)
}

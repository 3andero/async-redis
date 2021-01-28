use crate::protocol::*;
use intermediate_parsing::*;

#[derive(Debug)]
struct IntermediateParser<'a> {
    token_stack: Vec<IntermediateToken>,
    buf: &'a BytesMut
}

impl<'a> IntermediateParser<'a> {
    fn new(buf: &'a BytesMut) -> Self {
        Self {
            token_stack: Vec::with_capacity(4),
            buf
        }
    }

    fn parse() -> 
}

pub fn decode(buf: &mut Bytes) -> FrameResult<Frame> {
    if buf.len() == 0 {
        return Err(FrameError::Incomplete);
    }
    let buf0 = buf.split_to(1);
    match buf0[0] {
        b'+' => {
            let next_line = get_line(buf)?;
            Ok(Frame::SimpleString(next_line))
        }
        b'-' => {
            let next_line = get_line(buf)?;
            Ok(Frame::Errors(next_line))
        }
        b':' => {
            let next_line = get_line(buf)?;
            let res = get_integer(&next_line)?;
            Ok(Frame::Integers(res))
        }
        b'$' => {
            let mut next_line = get_line(buf)?;
            let len = get_integer(&next_line)?;
            let res = if len == -1 {
                Frame::NullString
            } else {
                next_line = get_line(buf)?;
                if next_line.len() != len as usize {
                    return Err("Integrity Error".into());
                }
                Frame::BulkStrings(next_line)
            };
            Ok(res)
        }
        b'*' => {
            let next_line = get_line(buf)?;
            let len = get_integer(&next_line)?;
            let res = if len == -1 {
                Frame::NullString
            } else {
                let mut frame_arr = Vec::<Frame>::new();
                for _ in 0..len as usize {
                    frame_arr.push(decode(buf)?)
                }
                Frame::Arrays(FrameArrays::new(frame_arr))
            };
            Ok(res)
        }
        x => {
            error!("Not Implemented: {}", x);
            return Err(FrameError::NotImplemented);
        }
    }
}

fn get_line(buf: &mut Bytes) -> FrameResult<Bytes> {
    let mut matched = false;
    for (pos, bt) in buf.iter().enumerate() {
        if *bt == b'\r' {
            matched = true;
        } else if *bt == b'\n' && matched {
            let x = buf.slice(0..pos - 1);
            buf.advance(pos + 1);
            return Ok(x);
        } else {
            matched = false;
        }
    }
    Err(FrameError::Incomplete)
}

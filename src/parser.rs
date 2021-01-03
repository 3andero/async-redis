use bytes::{Buf, Bytes};
use tokio::net::*;

use crate::Error;

#[derive(Debug)]
pub enum Frame {
    SimpleString(Bytes),
    Errors(Bytes),
    Integers(i64),
    BulkStrings(Bytes),
    Null,
    Arrays(Vec<Frame>),
}
#[derive(Debug)]
pub enum FrameError {
    Incomplete,
    Other(crate::Error),
}

impl From<String> for FrameError {
    fn from(msg: String) -> FrameError {
        FrameError::Other(msg.into())
    }
}

impl From<&str> for FrameError {
    fn from(msg: &str) -> FrameError {
        msg.to_string().into()
    }
}

type FrameResult<T> = std::result::Result<T, FrameError>;

pub fn parse(buf: &mut Bytes) -> FrameResult<Frame> {
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
            let res = get_number(&next_line)?;
            Ok(Frame::Integers(res))
        }
        b'$' => {
            let mut next_line = get_line(buf)?;
            let len = get_number(&next_line)?;
            let res = if len == -1 {
                Frame::Null
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
            let mut next_line = get_line(buf)?;
            let len = get_number(&next_line)?;
            let res = if len == -1 {
                Frame::Null
            } else {
                let mut frame_arr = Vec::<Frame>::new();
                for i in 0..len as usize {
                    frame_arr.push(parse(buf)?)
                }
                Frame::Arrays(frame_arr)
            };
            Ok(res)
        }
        _ => {
            unimplemented!()
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

fn get_number(line: &Bytes) -> FrameResult<i64> {
    let x = String::from_utf8(line.to_vec()).map_err(|e| FrameError::Other(Box::new(e)))?;
    let res = x
        .parse::<i64>()
        .map_err(|e| FrameError::Other(Box::new(e)))?;

    Ok(res)
}

#[macro_export]
macro_rules! FrameDisplayTests {
    ($($cmd:expr),*) => {
        let mut params = vec![$(Bytes::from($cmd.to_owned()),)*];
        for param in params.iter_mut() {
            let res = parser::parse(&mut param.clone());
            println!("{:?} => {:?}", param, res);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser;
    use bytes::*;
    #[test]
    fn Displays() {
        FrameDisplayTests!(
            "*0\r\n",
            "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            "*3\r\n:1\r\n:2\r\n:3\r\n",
            "*-1\r\n",
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n",
            "$6\r\nfoobar\r\n",
            "+OK\r\n",
            "$3\r\nfoobar\r\n",
            "$6\r\nfoar\r\n",
            "$6\r\rfoobar\r\n"
        );
    }
}

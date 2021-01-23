use anyhow::{anyhow, Error, Result};
use bytes::*;
use tracing::*;

use crate::BytesToString;

#[derive(Debug)]
pub enum Frame {
    SimpleString(Bytes),
    Errors(Bytes),
    Integers(i64),
    BulkStrings(Bytes),
    Null,
    Arrays(Vec<Frame>),
    Ok,
}

impl From<Bytes> for Frame {
    fn from(bt: Bytes) -> Frame {
        return Frame::BulkStrings(bt);
    }
}
#[derive(Debug, err_derive::Error)]
pub enum FrameError {
    #[error(display = "Incomplete")]
    Incomplete,
    #[error(display = "Not Implemented")]
    NotImplemented,
    #[error(display = "{}", _0)]
    Other(Error),
}

impl From<String> for FrameError {
    fn from(msg: String) -> FrameError {
        FrameError::Other(anyhow!(msg))
    }
}

impl From<&str> for FrameError {
    fn from(msg: &str) -> FrameError {
        msg.to_string().into()
    }
}

type FrameResult<T> = std::result::Result<T, FrameError>;

const NILFRAME: &'static [u8] = b"$-1\r\n";
const OKFRAME: &'static [u8] = b"+OK\r\n";

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
            let next_line = get_line(buf)?;
            let len = get_number(&next_line)?;
            let res = if len == -1 {
                Frame::Null
            } else {
                let mut frame_arr = Vec::<Frame>::new();
                for _ in 0..len as usize {
                    frame_arr.push(decode(buf)?)
                }
                Frame::Arrays(frame_arr)
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

fn get_number(line: &Bytes) -> FrameResult<i64> {
    let mut res = 0;
    for v in line {
        if *v >= b'0' && *v <= b'9' {
            res = res * 10 + ((*v - b'0') as i64);
        } else {
            return Err("Not Digit".into());
        }
    }

    Ok(res)
}

pub fn encode(frame: &Frame) -> Result<Bytes> {
    match frame {
        Frame::Null => {
            return Ok(Bytes::from(NILFRAME));
        }
        Frame::Ok => {
            return Ok(Bytes::from(OKFRAME));
        }
        _ => (),
    }

    let msg_encoded = match frame {
        Frame::SimpleString(msg) => {
            format!("+{}\r\n", BytesToString!(msg))
        }
        Frame::Errors(msg) => {
            format!("-{}\r\n", BytesToString!(msg))
        }
        Frame::Integers(num) => {
            format!(":{}\r\n", num)
        }
        Frame::BulkStrings(msg) => {
            format!("${}\r\n{}\r\n", msg.len(), BytesToString!(msg))
        }
        Frame::Arrays(arr) => {
            let mut res = String::with_capacity(arr.len() * 8);
            for f in arr.iter() {
                res += BytesToString!(encode(f)?).as_ref();
            }
            format!("*{}\r\n{}", arr.len(), res)
        }
        _ => unimplemented!(),
    };
    Ok(Bytes::from(msg_encoded))
}

#[macro_export]
macro_rules! FrameTests {
    (Display $($cmd:expr),*) => {
        let mut params = vec![$(Bytes::from($cmd.to_owned()),)*];
        for param in params.iter_mut() {
            let res = protocol::decode(&mut param.clone());
            println!("{:?} => {:?}", param, res);
        }
    };
    (Encode $($cmd:expr),*) => {
        let mut params = vec![$(Bytes::from($cmd.to_owned()),)*];
        for param in params.iter_mut() {
            let mut _p = param.clone();
            let mut err_msg = String::new();
            let res = match protocol::decode(&mut _p) {
                Ok(v) => v,
                Err(e) => {
                    err_msg = format!("{:?}", e);
                    protocol::Frame::Null
                }
            };
            let decoded = protocol::encode(&res).unwrap();
            let equal = decoded.to_vec() == param.to_vec();
            println!("{:?} => {:?} + {:?} => {:?} | {} | Equal={}", param, _p, res, decoded, err_msg, equal);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol;
    use bytes::*;
    #[test]
    fn displays() {
        FrameTests!(Display
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

    #[test]
    fn encode() {
        FrameTests!(Encode
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

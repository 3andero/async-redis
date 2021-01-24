use crate::utils::{integer_to_bytes, len_of};
use anyhow::{anyhow, Error, Result};
use bytes::*;
use tracing::*;

// use crate::BytesToString;
#[derive(Debug)]
pub struct FrameArrays {
    pub val: Vec<Frame>,
    _encode_length: usize,
}

impl FrameArrays {
    pub fn new(val: Vec<Frame>) -> Self {
        Self {
            _encode_length: 3 + len_of(val.len()) + val.iter().fold(0, |res, f| res + f.len()),
            val,
        }
    }
}

#[derive(Debug)]
pub enum Frame {
    SimpleString(Bytes),
    Errors(Bytes),
    Integers(i64),
    BulkStrings(Bytes),
    Null,
    Arrays(FrameArrays),
    Ok,
}

impl From<Bytes> for Frame {
    fn from(bt: Bytes) -> Frame {
        return Frame::BulkStrings(bt);
    }
}

impl Frame {
    fn len(&self) -> usize {
        match self {
            Frame::Ok | Frame::Null => 5,
            Frame::SimpleString(v) | Frame::Errors(v) => v.len() + 3,
            Frame::BulkStrings(v) => 5 + v.len() + len_of(v.len()),
            &Frame::Integers(v) => len_of(v) + 3,
            Frame::Arrays(v) => v._encode_length,
        }
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
const SIMPLE_STRING_MARK: &'static [u8] = b"+";
const ERROR_MARK: &'static [u8] = b"-";
const BULK_STRING_MARK: &'static [u8] = b"$";
const INTEGER_MARK: &'static [u8] = b":";
const ARRAY_MARK: &'static [u8] = b"*";
const DLEM_MARK: &'static [u8] = b"\r\n";

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
            let len = get_integer(&next_line)?;
            let res = if len == -1 {
                Frame::Null
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

fn get_integer(line: &Bytes) -> FrameResult<i64> {
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

fn encode_iter(frame: &Frame, buf: &mut BytesMut) {
    match frame {
        Frame::SimpleString(msg) => {
            buf.put_slice(SIMPLE_STRING_MARK);
            buf.put_slice(msg);
            buf.put_slice(DLEM_MARK);
        }
        Frame::Errors(msg) => {
            buf.put_slice(ERROR_MARK);
            buf.put_slice(msg);
            buf.put_slice(DLEM_MARK);
        }
        &Frame::Integers(num) => {
            buf.put_slice(INTEGER_MARK);
            buf.put_slice(&integer_to_bytes(num)[..]);
            buf.put_slice(DLEM_MARK);
        }
        Frame::BulkStrings(msg) => {
            buf.put_slice(BULK_STRING_MARK);
            buf.put_slice(&integer_to_bytes(msg.len())[..]);
            buf.put_slice(DLEM_MARK);
            buf.put_slice(msg);
            buf.put_slice(DLEM_MARK);
        }
        Frame::Arrays(arr) => {
            buf.put_slice(ARRAY_MARK);
            buf.put_slice(&integer_to_bytes(arr.val.len())[..]);
            buf.put_slice(DLEM_MARK);
            for f in &arr.val {
                encode_iter(&f, buf);
            }
        }
        Frame::Null => {
            buf.put_slice(NILFRAME);
        }
        Frame::Ok => {
            buf.put_slice(OKFRAME);
        }
    };
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

    let mut buf = BytesMut::with_capacity(frame.len());
    encode_iter(frame, &mut buf);
    Ok(buf.freeze())
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
            "$-1\r\n",
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n",
            "*12\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n",
            "$6\r\nfoobar\r\n",
            "+OK\r\n",
            "$3\r\nfoobar\r\n",
            "$6\r\nfoar\r\n",
            "$6\r\rfoobar\r\n"
        );
    }
}

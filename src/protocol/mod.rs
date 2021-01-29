use crate::utils::{integer_to_bytes, len_of};
use anyhow::{anyhow, Error, Result};
use bytes::*;
use tracing::*;

pub mod decode;
pub mod encode;
mod intermediate_parsing;

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
    NullString,
    Arrays(FrameArrays),
    Ok,
    NullArray,
}

impl From<Bytes> for Frame {
    fn from(bt: Bytes) -> Frame {
        return Frame::BulkStrings(bt);
    }
}

impl Frame {
    fn len(&self) -> usize {
        match self {
            Frame::Ok | Frame::NullString | Frame::NullArray => 5,
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
    #[error(display = "Invalid: {}", _0)]
    Invalid(String),
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

const NIL_STRING_FRAME: &'static [u8] = b"$-1\r\n";
const NIL_ARRAY_FRAME: &'static [u8] = b"*-1\r\n";
const OK_FRAME: &'static [u8] = b"+OK\r\n";
const SIMPLE_STRING_MARK: u8 = b'+';
const ERROR_MARK: u8 = b'-';
const BULK_STRING_MARK: u8 = b'$';
const INTEGER_MARK: u8 = b':';
const ARRAY_MARK: u8 = b'*';
const DLEM_MARK: &'static [u8] = b"\r\n";

#[macro_export]
macro_rules! FrameTests {
    (DisplayDecodeFn $($cmd:expr),*) => {
        let mut params = vec![$(Bytes::from($cmd.to_owned()),)*];
        for param in params.iter_mut() {
            let res = decode(&mut param.clone());
            println!("{:?} => {:?}", param, res);
        }
    };
    (DisplayIntermediateParser $($cmd:expr),*) => {
        let mut params = vec![$($cmd,)*];
        for param in params.iter_mut() {
            let mut buf = BytesMut::new();
            buf.put_slice(&param.as_bytes());
            let mut parser = decode::IntermediateParser::new();
            let res = parser.parse(&mut buf);
            println!("{:?} => {:?}", param, res);
        }
    };
    (Encode $($cmd:expr),*) => {
        let mut params = vec![$($cmd,)*];
        for param in params.iter_mut() {
            let mut buf = BytesMut::new();
            buf.put_slice(&param.as_bytes());
            let mut parser = decode::IntermediateParser::new();
            let (res, err_msg) = match parser.parse(&mut buf) {
                Ok(v) => (v, String::from("")),
                Err(e) => {
                    (Frame::NullString, format!("{:?}", e))
                }
            };
            let decoded = encode(&res).unwrap();
            let equal = decoded.to_vec() == param.as_bytes();
            println!("{:?} => {:?} + {:?} => {:?} | {} | Equal={}", param, buf, res, decoded, err_msg, equal);
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::protocol::*;
    use decode::*;
    use encode::*;
    #[test]
    fn displays_decode() {
        FrameTests!(DisplayDecodeFn
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
    fn displays_parser() {
        FrameTests!(DisplayIntermediateParser
            "*0\r\n",
            "$0\r\n\r\n",
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
    fn encode_test() {
        FrameTests!(Encode
            "*0\r\n",
            "$0\r\n\r\n",
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

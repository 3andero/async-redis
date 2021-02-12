use crate::utils::{integer_to_bytes, len_of};
use anyhow::{anyhow, Error, Result};
use bytes::*;
use tracing::*;

pub mod decode;
pub mod encode;
mod intermediate_parsing;
pub mod reusable_buf;

#[derive(Debug, Clone)]
pub struct FrameArrays {
    pub val: Vec<Frame>,
    _raw_bytes_length: usize,
    _msg_length: usize,
    _msg_num: usize,
    _initialized: bool,
}

const SMALL_BYTES_THRESHOLD: usize = 0;

impl FrameArrays {
    pub fn new(val: Vec<Frame>) -> Self {
        let (_raw_bytes_length, _msg_length, _msg_num) =
            val.iter().fold((0, 0, 0), |(a, b, c), f| {
                (a + f.raw_bytes_len(), b + f.msg_len(), c + f.msg_num())
            });
        Self {
            _raw_bytes_length: _raw_bytes_length + 3 + len_of(val.len()),
            _msg_length,
            _msg_num,
            val,
            _initialized: true,
        }
    }

    pub fn init_for_encoding(&mut self) {
        if self._initialized {
            return;
        }
        let (_raw_bytes_length, _msg_length, _msg_num) =
            self.val.iter().fold((0, 0, 0), |(a, b, c), f| {
                (a + f.raw_bytes_len(), b + f.msg_len(), c + f.msg_num())
            });

        self._raw_bytes_length = _raw_bytes_length + 3 + len_of(self.val.len());
        self._msg_length = _msg_length;
        self._msg_num = _msg_num;
        self._initialized = true;
    }
}

#[derive(Debug, Clone)]
pub enum Frame {
    SimpleString(Bytes),
    Errors(Bytes),
    Integers(i64),
    BulkStrings(Bytes),
    BulkStringsEncoded(Bytes),
    NullString,
    Arrays(FrameArrays),
    Ok,
    NullArray,
}

// impl From<Bytes> for Frame {
//     fn from(bt: Bytes) -> Frame {
//         return Frame::BulkStrings(bt);
//     }
// }

// impl From<Option<Bytes>> for Frame {
//     fn from(bt: Option<Bytes>) -> Frame {
//         return bt.map_or(Frame::NullString, |x| x.into());
//     }
// }

// impl From<Vec<Option<Bytes>>> for Frame {
//     fn from(arr: Vec<Option<Bytes>>) -> Frame {
//         let x = arr.into_iter().map(|x| x.into()).collect();
//         Frame::Arrays(FrameArrays::new(x))
//     }
// }

impl From<Vec<Frame>> for Frame {
    fn from(arr: Vec<Frame>) -> Frame {
        Frame::Arrays(FrameArrays::new(arr))
    }
}

impl Frame {
    fn raw_bytes_len(&self) -> usize {
        match self {
            Frame::Ok | Frame::NullString | Frame::NullArray => 5,
            Frame::SimpleString(v) | Frame::Errors(v) => v.len() + 3,
            Frame::BulkStrings(v) => 5 + v.len() + len_of(v.len()),
            &Frame::Integers(v) => len_of(v) + 3,
            Frame::BulkStringsEncoded(v) => v.len(),
            Frame::Arrays(v) => v._raw_bytes_length,
        }
    }

    pub fn msg_len(&self) -> usize {
        match self {
            Frame::Ok | Frame::NullString | Frame::NullArray => 5,
            Frame::SimpleString(v) | Frame::Errors(v) | Frame::BulkStrings(v) => {
                if v.len() > SMALL_BYTES_THRESHOLD {
                    v.len()
                } else {
                    0
                }
            }
            Frame::BulkStringsEncoded(v) => v.len(),
            Frame::Arrays(v) => v._msg_length,
            Frame::Integers(_) => 0,
        }
    }

    fn msg_num(&self) -> usize {
        match self {
            Frame::Ok | Frame::NullString | Frame::NullArray => 0,
            Frame::SimpleString(b) | Frame::Errors(b) | Frame::BulkStrings(b) => {
                if b.len() > SMALL_BYTES_THRESHOLD {
                    1
                } else {
                    0
                }
            }
            Frame::Arrays(v) => v._msg_num,
            Frame::Integers(_) | Frame::BulkStringsEncoded(_) => 0,
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

pub const NIL_STRING_FRAME: &'static [u8] = b"$-1\r\n";
pub const NIL_ARRAY_FRAME: &'static [u8] = b"*-1\r\n";
pub const OK_FRAME: &'static [u8] = b"+OK\r\n";
const SIMPLE_STRING_MARK: u8 = b'+';
const ERROR_MARK: u8 = b'-';
const BULK_STRING_MARK: u8 = b'$';
const INTEGER_MARK: u8 = b':';
const ARRAY_MARK: u8 = b'*';
const DLEM_MARK: &'static [u8] = b"\r\n";

#[macro_export]
macro_rules! FrameTests {
    (DisplayIntermediateParser $($cmd:expr),*) => {
        let mut params = vec![$($cmd,)*];
        for param in params.iter_mut() {
            // let mut buf = BytesMut::new();
            let mut buf = reusable_buf::ReusableBuf::new();
            buf.put_slice(&param.as_bytes());
            let mut parser = decode::IntermediateParser::new();
            let res = parser.parse(&mut buf);
            println!("{:?} => {:?}", param, res);
        }
    };
    (Encode $($cmd:expr),*) => {
        let mut params = vec![$($cmd,)*];
        let mut buf = reusable_buf::ReusableBuf::new();
        for param in params.iter_mut() {
            buf.reset();
            buf.reserve(param.len());
            buf.put_slice(&param.as_bytes());
            let mut parser = decode::IntermediateParser::new();
            let mut start = 0;
            while buf.len() > 0 {
                let len0 = buf.len();
                let (res, err_msg) = match parser.parse(&mut buf) {
                    Ok(v) => (v, String::from("")),
                    Err(e) => {
                        (Frame::NullString, format!("{:?}", e))
                    }
                };
                let len1 = buf.len();
                let nstart = start + len0 - len1;
                let decoded = encode(&res).unwrap();
                let mut final_byte = BytesMut::new();
                for b in decoded.iter() {
                    final_byte.put_slice(&b[..]);
                }
                let equal = final_byte == &param[start..nstart];
                println!("{:?} => {:?} + {:?} => {:?} | {} | Equal={}", &param[start..nstart], buf, res, decoded, err_msg, equal);
                start = nstart;
                if err_msg.len() > 0 {
                    break;
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::protocol::*;
    use encode::*;

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
            "*3\r\n$3\r\nset\r\n$3\r\nabc\r\n$2\r\nab\r\n*3\r\n$3\r\nset\r\n$3\r\nbbc\r\n$2\r\nbb\r\n",
            "*12\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n$-1\r\n",
            "*-1\r\n",
            "$-1\r\n",
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n",
            "$6\r\nfoobar\r\n",
            "+OK\r\n",
            "$3\r\nfoobar\r\n",
            "$6\r\nfoar\r\n",
            "$6\r\rfoobar\r\n"
        );
    }
}

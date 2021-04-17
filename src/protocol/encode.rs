use crate::protocol::*;

struct EfficientBuffer {
    hot_buf: BytesMut,
    fragments: Vec<Bytes>,
}

impl EfficientBuffer {
    fn new(frame: &Frame) -> Self {
        Self {
            hot_buf: BytesMut::with_capacity(frame.raw_bytes_len() - frame.encode_msg_len()),
            fragments: Vec::with_capacity(frame.msg_num() * 2 + 1),
        }
    }

    fn put_u8(&mut self, byte: u8) {
        self.hot_buf.put_u8(byte);
    }

    fn put_slice(&mut self, slice: &[u8]) {
        self.hot_buf.put_slice(slice);
    }

    fn append_bytes(&mut self, msg: &Bytes) {
        self.flush();
        self.fragments.push(msg.clone());
    }

    fn flush(&mut self) {
        if self.hot_buf.len() > 0 {
            let frag = self.hot_buf.split_to(self.hot_buf.len()).freeze();
            self.fragments.push(frag);
        }
    }
}

fn encode_iter(frame: &Frame, buf: &mut EfficientBuffer) {
    use Frame::*;
    match frame {
        SimpleString(msg) => {
            buf.put_u8(SIMPLE_STRING_MARK);
            if msg.len() > SMALL_BYTES_THRESHOLD {
                buf.append_bytes(msg);
            } else {
                buf.put_slice(msg);
            }
            buf.put_slice(DLEM_MARK);
        }
        Errors(msg) => {
            buf.put_u8(ERROR_MARK);
            if msg.len() > SMALL_BYTES_THRESHOLD {
                buf.append_bytes(msg);
            } else {
                buf.put_slice(msg);
            }
            buf.put_slice(DLEM_MARK);
        }
        &Integers(num) => {
            buf.put_u8(INTEGER_MARK);
            buf.put_slice(&integer_to_bytes(num)[..]);
            buf.put_slice(DLEM_MARK);
        }
        BulkStrings(msg) => {
            buf.put_u8(BULK_STRING_MARK);
            buf.put_slice(&integer_to_bytes(msg.len())[..]);
            buf.put_slice(DLEM_MARK);
            if msg.len() > SMALL_BYTES_THRESHOLD {
                buf.append_bytes(msg);
            } else {
                buf.put_slice(msg);
            }
            buf.put_slice(DLEM_MARK);
        }
        Arrays(arr) => {
            buf.put_u8(ARRAY_MARK);
            buf.put_slice(&integer_to_bytes(arr.len())[..]);
            buf.put_slice(DLEM_MARK);
            for f in arr {
                encode_iter(&f, buf);
            }
        }
        NullString => {
            buf.put_slice(NIL_STRING_FRAME);
        }
        NullArray => {
            buf.put_slice(NIL_ARRAY_FRAME);
        }
        Frame::Ok => {
            buf.put_slice(OK_FRAME);
        }
        Message => {
            buf.put_slice(MESSAGE_FRAME);
        }
        _DetachSubscribeMode(_) => panic!(),
        Pong => {
            buf.put_slice(PONG_FRAME);
        }
    };
}

pub fn encode(frame: &Frame) -> Result<Vec<Bytes>> {
    let mut buf = EfficientBuffer::new(frame);
    encode_iter(frame, &mut buf);
    buf.flush();
    Ok(buf.fragments)
}

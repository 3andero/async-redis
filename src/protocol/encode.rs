use crate::protocol::*;

fn encode_iter(frame: &Frame, buf: &mut BytesMut) {
    match frame {
        Frame::SimpleString(msg) => {
            buf.put_u8(SIMPLE_STRING_MARK);
            buf.put_slice(msg);
            buf.put_slice(DLEM_MARK);
        }
        Frame::Errors(msg) => {
            buf.put_u8(ERROR_MARK);
            buf.put_slice(msg);
            buf.put_slice(DLEM_MARK);
        }
        &Frame::Integers(num) => {
            buf.put_u8(INTEGER_MARK);
            buf.put_slice(&integer_to_bytes(num)[..]);
            buf.put_slice(DLEM_MARK);
        }
        Frame::BulkStrings(msg) => {
            buf.put_u8(BULK_STRING_MARK);
            buf.put_slice(&integer_to_bytes(msg.len())[..]);
            buf.put_slice(DLEM_MARK);
            buf.put_slice(msg);
            buf.put_slice(DLEM_MARK);
        }
        Frame::Arrays(arr) => {
            buf.put_u8(ARRAY_MARK);
            buf.put_slice(&integer_to_bytes(arr.val.len())[..]);
            buf.put_slice(DLEM_MARK);
            for f in &arr.val {
                encode_iter(&f, buf);
            }
        }
        Frame::NullString => {
            buf.put_slice(NIL_STRING_FRAME);
        }
        Frame::NullArray => {
            buf.put_slice(NIL_ARRAY_FRAME);
        }
        Frame::Ok => {
            buf.put_slice(OK_FRAME);
        }
    };
}

pub fn encode(frame: &Frame) -> Result<Bytes> {
    match frame {
        Frame::NullString => {
            return Ok(Bytes::from(NIL_STRING_FRAME));
        }
        Frame::Ok => {
            return Ok(Bytes::from(OK_FRAME));
        }
        _ => (),
    }

    let mut buf = BytesMut::with_capacity(frame.len());
    encode_iter(frame, &mut buf);
    Ok(buf.freeze())
}

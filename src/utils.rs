use num_traits::{FromPrimitive, PrimInt, ToPrimitive, Zero};
use bytes::Bytes;
use crate::Result;
use anyhow::anyhow;

#[macro_export]
macro_rules! BytesToString {
    ($bytes: expr) => {
        String::from_utf8($bytes.to_vec()).map_err(|e| Error::new(e))?
    };
    ($bytes: expr, $err_type: expr) => {
        String::from_utf8($bytes.to_vec()).map_err(|e| $err_type(Error::new(e)))?
    };
}

pub fn len_of<T>(mut v: T) -> usize
where
    T: PrimInt + Zero + FromPrimitive,
{
    if v.is_zero() {
        1
    } else {
        let mut res = if v < T::zero() {
            v = T::zero() - v;
            1
        } else {
            0
        };
        while v > T::zero() {
            v = v.div(FromPrimitive::from_u8(10).unwrap());
            res += 1;
        }
        res
    }
}

pub fn integer_to_bytes<T>(mut num: T) -> Vec<u8>
where
    T: PrimInt + Zero + FromPrimitive + ToPrimitive,
{
    let size = len_of(num);
    let mut res = vec![0u8; size];
    for i in (0..size).rev() {
        res[i] = b'0' + ToPrimitive::to_u8(&(num % FromPrimitive::from_u8(10).unwrap())).unwrap();
        num = num / FromPrimitive::from_u8(10).unwrap();
    }
    res
}

pub fn get_integer(line: &Bytes) -> Result<i64> {
    let (neg, line) = if line.len() == 0 {
        return Err(anyhow!("Not Digit: {:?}", line));
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
            return Err(anyhow!("Not Digit: {:?}", line));
        }
    }

    Ok(if neg { -res } else { res })
}

const ASSERT: [(); 1] = [()];
pub const PRIME: usize = 1e9 as usize + 9;

#[allow(unconditional_panic, unused_assignments)]
pub const fn rolling_hash_const(arr: &[u8]) -> usize {
    let mut res = 0;
    let mut i = 0;
    if arr.len() >= 1 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (arr[i] - b'a') as usize;
        i += 1;
    }
    if arr.len() >= 2 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 3 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 4 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 5 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 6 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 7 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 8 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 9 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 10 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 11 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 12 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 13 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 14 {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    if arr.len() >= 15 {
        ASSERT[1];
    }
    res
}
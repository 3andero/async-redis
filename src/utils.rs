use crate::{cmd::CommandError, Result};
use anyhow::{anyhow, Error};
use bytes::Bytes;
use num_traits::{FromPrimitive, PrimInt, ToPrimitive, Zero};

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
    let start = if num < T::zero() {
        res[0] = b'-';
        num = T::zero() - num;
        1
    } else {
        0
    };
    for i in (start..size).rev() {
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

pub const fn rolling_hash_const(arr: &[u8]) -> usize {
    let mut res = 0;
    let mut i = 0;

    while i < arr.len() {
        ASSERT[!(arr[i] <= b'z' && arr[i] >= b'a') as usize];
        res = (res * 26 + (arr[i] - b'a') as usize) % PRIME;
        i += 1;
    }
    res
}

pub const fn const_panic() {
    ASSERT[1];
}

pub fn rolling_hash(arr: &[u8]) -> Result<usize> {
    let mut res = 0;
    for &b in arr {
        if b <= b'z' && b >= b'a' {
            res = (res * 26 + (b - b'a') as usize) % PRIME;
        } else if b <= b'Z' && b >= b'A' {
            res = (res * 26 + (b - b'A') as usize) % PRIME;
        } else {
            return Err(Error::new(CommandError::InvalidOperation));
        }
    }
    Ok(res)
}
#[macro_use]
#[macro_export]
macro_rules! impl_enum_is_branch {
    (@branch, $target:ident, $true_branch:ident) => {
        $target::$true_branch
    };
    (@branch, $target:ident, $true_branch:ident, $x:ident) => {
        $target::$true_branch(_)
    };
    ($target:ident, $fn_name:ident, $(($($true_branch:ident),*)),*) => {
        impl $target {
            pub fn $fn_name(&self) -> bool {
                match self {
                    $(
                        // $target::$true_branch(_) => true,
                        crate::impl_enum_is_branch!(@branch, $target, $($true_branch),*) => true,
                    )*
                    _ => false,
                }
            }
        }
    };
}

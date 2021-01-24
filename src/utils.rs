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
    for i in (0..size).rev() {
        res[i] = b'0' + ToPrimitive::to_u8(&(num % FromPrimitive::from_u8(10).unwrap())).unwrap();
        num = num / FromPrimitive::from_u8(10).unwrap();
    }
    res
}

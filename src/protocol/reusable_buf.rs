use bytes::buf::UninitSlice;
use bytes::{Buf, BufMut};
use core::borrow::{Borrow, BorrowMut};
use core::ops::{Deref, DerefMut};

#[derive(Debug)]
pub struct ReusableBuf {
    inner: Vec<u8>,
    start: usize,
}

impl ReusableBuf {
    pub fn new() -> Self {
        Self {
            inner: Vec::new(),
            start: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len() - self.start
    }

    pub fn reset(&mut self) {
        self.start = 0;
        unsafe {
            self.inner.set_len(0);
        }
    }

    fn remaining_space_straight(&self) -> usize {
        assert!(self.inner.capacity() >= self.inner.len());
        self.inner.capacity() - self.inner.len()
    }

    pub fn reserve(&mut self, additional: usize) {
        if self.remaining_space_straight() >= additional {
            // println!("no need to allocate.");
            return;
        }

        if self.start != 0 && (self.len() + additional) <= self.inner.capacity() {
            // println!(
            //     "reuse previous space. current start: {}, current len: {}",
            //     self.start,
            //     self.len()
            // );
            for i in self.start..self.inner.len() {
                self.inner[i - self.start] = self.inner[i];
            }
            unsafe {
                self.inner.set_len(self.inner.len() - self.start);
            }
            self.start = 0;
        } else {
            // println!(
            //     "allocate new space. current start: {}, current len: {}, require: {}",
            //     self.start,
            //     self.len(),
            //     self.len() + additional,
            // );
            let mut new_vec = Vec::new();
            new_vec.reserve_exact(self.len() + additional);
            new_vec.extend_from_slice(self.as_ref());
            self.inner = new_vec;
            self.start = 0;
        }
    }

    pub fn extend_from_slice(&mut self, other: &[u8]) {
        let cnt = other.len();
        self.reserve(cnt);
        self.inner.extend_from_slice(other);
    }
}

unsafe impl BufMut for ReusableBuf {
    #[inline]
    fn remaining_mut(&self) -> usize {
        usize::MAX - self.len()
    }

    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        let remaining = self.remaining_space_straight();

        assert!(
            cnt <= remaining,
            "cannot advance past `remaining_mut`: {:?} <= {:?}, {:?}",
            cnt,
            remaining,
            self
        );

        self.inner.set_len(self.inner.len() + cnt);
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut UninitSlice {
        if self.remaining_space_straight() == 0 {
            self.reserve(64); // Grow the vec
        }

        self.inner.chunk_mut()
    }

    // Specialize these methods so they can skip checking `remaining_mut`
    // and `advance_mut`.
    fn put<T: Buf>(&mut self, mut src: T)
    where
        Self: Sized,
    {
        // In case the src isn't contiguous, reserve upfront
        self.reserve(src.remaining());

        while src.has_remaining() {
            let l;

            // a block to contain the src.bytes() borrow
            {
                let s = src.chunk();
                l = s.len();
                self.inner.extend_from_slice(s);
            }

            src.advance(l);
        }
    }

    #[inline]
    fn put_slice(&mut self, src: &[u8]) {
        self.extend_from_slice(src);
    }
}

impl Buf for ReusableBuf {
    #[inline]
    fn remaining(&self) -> usize {
        self.len()
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        &self.inner[self.start..]
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        assert!(
            cnt <= self.len(),
            "cannot advance past `remaining`: {:?} <= {:?}, {:?}",
            cnt,
            self.len(),
            self,
        );

        self.start += cnt;
    }
}

impl AsRef<[u8]> for ReusableBuf {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.inner[self.start..]
    }
}

impl AsMut<[u8]> for ReusableBuf {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.inner[self.start..]
    }
}

impl Borrow<[u8]> for ReusableBuf {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl BorrowMut<[u8]> for ReusableBuf {
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl DerefMut for ReusableBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl Deref for ReusableBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

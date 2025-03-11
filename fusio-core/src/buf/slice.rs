#[cfg(feature = "alloc")]
use alloc::vec::Vec;
use core::ops::{Bound, RangeBounds};

use super::{IoBuf, IoBufMut};

pub struct Buf {
    pub(super) layout: BufLayout,
    pub(super) start: usize,
    pub(super) end: usize,
}

#[cfg(not(feature = "no-send"))]
unsafe impl Send for Buf {}

pub(super) enum BufLayout {
    Slice {
        ptr: *const u8,
        len: usize,
    },
    #[cfg(feature = "alloc")]
    Vec(Vec<u8>),
    #[cfg(feature = "bytes")]
    Bytes(bytes::Bytes),
    #[cfg(feature = "bytes")]
    BytesMut(bytes::BytesMut),
}

impl IoBuf for Buf {
    fn as_ptr(&self) -> *const u8 {
        match &self.layout {
            BufLayout::Slice { ptr, .. } => unsafe { (*ptr).add(self.start) },
            #[cfg(feature = "alloc")]
            BufLayout::Vec(vec) => vec[self.start..].as_ptr(),
            #[cfg(feature = "bytes")]
            BufLayout::Bytes(bytes) => bytes[self.start..].as_ptr(),
            #[cfg(feature = "bytes")]
            BufLayout::BytesMut(bytes) => bytes[self.start..].as_ptr(),
        }
    }

    fn bytes_init(&self) -> usize {
        match &self.layout {
            BufLayout::Slice { len, .. } => *len - self.start,
            #[cfg(feature = "alloc")]
            BufLayout::Vec(vec) => vec.len() - self.start,
            #[cfg(feature = "bytes")]
            BufLayout::Bytes(bytes) => bytes.len() - self.start,
            #[cfg(feature = "bytes")]
            BufLayout::BytesMut(bytes) => bytes.len() - self.start,
        }
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        match &self.layout {
            BufLayout::Slice { ptr, .. } => bytes::Bytes::copy_from_slice(unsafe {
                core::slice::from_raw_parts((*ptr).add(self.start), self.end - self.start)
            }),
            BufLayout::Vec(vec) => bytes::Bytes::copy_from_slice(&vec[self.start..self.end]),
            #[cfg(feature = "bytes")]
            BufLayout::Bytes(bytes) => bytes.slice(self.start..self.end),
            #[cfg(feature = "bytes")]
            BufLayout::BytesMut(bytes) => bytes.clone().freeze().slice(self.start..self.end),
        }
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf {
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => self.start,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end + 1,
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.end,
        };
        Buf {
            layout: self.layout,
            start,
            end,
        }
    }

    unsafe fn recover_from_slice(buf: Buf) -> Self {
        buf
    }
}

pub struct BufMut {
    pub(super) layout: BufMutLayout,
    pub(super) start: usize,
    pub(super) end: usize,
}

#[cfg(not(feature = "no-send"))]
unsafe impl Send for BufMut {}

pub(super) enum BufMutLayout {
    #[allow(unused)]
    Slice { ptr: *mut u8, len: usize },
    #[cfg(feature = "alloc")]
    Vec(Vec<u8>),
    #[cfg(feature = "bytes")]
    BytesMut(bytes::BytesMut),
}

impl IoBuf for BufMut {
    fn as_ptr(&self) -> *const u8 {
        match &self.layout {
            BufMutLayout::Slice { ptr, .. } => unsafe { (*ptr).add(self.start) },
            #[cfg(feature = "alloc")]
            BufMutLayout::Vec(vec) => vec[self.start..].as_ptr(),
            #[cfg(feature = "bytes")]
            BufMutLayout::BytesMut(bytes) => bytes[self.start..].as_ptr(),
        }
    }

    fn bytes_init(&self) -> usize {
        match &self.layout {
            BufMutLayout::Slice { len, .. } => *len - self.start,
            #[cfg(feature = "alloc")]
            BufMutLayout::Vec(vec) => vec.len() - self.start,
            #[cfg(feature = "bytes")]
            BufMutLayout::BytesMut(bytes) => bytes.len() - self.start,
        }
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        match &self.layout {
            BufMutLayout::Slice { ptr, .. } => bytes::Bytes::copy_from_slice(unsafe {
                core::slice::from_raw_parts((*ptr).add(self.start), self.end - self.start)
            }),
            BufMutLayout::Vec(vec) => bytes::Bytes::copy_from_slice(&vec[self.start..self.end]),
            #[cfg(feature = "bytes")]
            BufMutLayout::BytesMut(bytes) => bytes.clone().freeze().slice(self.start..self.end),
        }
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf {
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => self.start,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end + 1,
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.end,
        };
        match self.layout {
            BufMutLayout::Slice { ptr, len } => Buf {
                layout: BufLayout::Slice {
                    ptr: ptr as *const u8,
                    len,
                },
                start,
                end,
            },
            #[cfg(feature = "alloc")]
            BufMutLayout::Vec(vec) => Buf {
                layout: BufLayout::Vec(vec),
                start,
                end,
            },
            #[cfg(feature = "bytes")]
            BufMutLayout::BytesMut(bytes) => Buf {
                layout: BufLayout::BytesMut(bytes),
                start,
                end,
            },
        }
    }

    unsafe fn recover_from_slice(buf: Buf) -> Self {
        match buf.layout {
            BufLayout::Slice { ptr, len } => BufMut {
                layout: BufMutLayout::Slice {
                    ptr: ptr as *mut u8,
                    len,
                },
                start: buf.start,
                end: buf.end,
            },
            #[cfg(feature = "alloc")]
            BufLayout::Vec(vec) => BufMut {
                layout: BufMutLayout::Vec(vec),
                start: buf.start,
                end: buf.end,
            },
            #[cfg(feature = "bytes")]
            BufLayout::Bytes(_) => unreachable!(),
            #[cfg(feature = "bytes")]
            BufLayout::BytesMut(bytes) => BufMut {
                layout: BufMutLayout::BytesMut(bytes),
                start: buf.start,
                end: buf.end,
            },
        }
    }
}

impl IoBufMut for BufMut {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        match &mut self.layout {
            BufMutLayout::Slice { ptr, .. } => unsafe { (*ptr).add(self.start) },
            #[cfg(feature = "alloc")]
            BufMutLayout::Vec(vec) => vec[self.start..].as_mut_ptr(),
            #[cfg(feature = "bytes")]
            BufMutLayout::BytesMut(bytes) => bytes[self.start..].as_mut_ptr(),
        }
    }

    unsafe fn slice_mut_unchecked(self, range: impl RangeBounds<usize>) -> BufMut {
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => self.start,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end + 1,
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.end,
        };
        BufMut {
            layout: self.layout,
            start,
            end,
        }
    }

    unsafe fn recover_from_slice_mut(buf: BufMut) -> Self {
        buf
    }
}

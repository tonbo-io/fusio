use std::ops::RangeBounds;

use crate::{IoBuf, IoBufMut};

pub struct Slice {
    pub(super) layout: SliceLayout,
    pub(super) start: usize,
    pub(super) end: usize,
}

#[cfg(not(feature = "no-send"))]
unsafe impl Send for Slice {}

pub(super) enum SliceLayout {
    Slice {
        ptr: *const u8,
        len: usize,
    },
    Vec(Vec<u8>),
    #[cfg(feature = "bytes")]
    Bytes(bytes::Bytes),
    #[cfg(feature = "bytes")]
    BytesMut(bytes::BytesMut),
}

unsafe impl Send for SliceLayout {}

impl IoBuf for Slice {
    fn as_ptr(&self) -> *const u8 {
        match &self.layout {
            SliceLayout::Slice { ptr, .. } => unsafe { (*ptr).add(self.start) },
            SliceLayout::Vec(vec) => vec[self.start..].as_ptr(),
            #[cfg(feature = "bytes")]
            SliceLayout::Bytes(bytes) => bytes[self.start..].as_ptr(),
            #[cfg(feature = "bytes")]
            SliceLayout::BytesMut(bytes) => bytes[self.start..].as_ptr(),
        }
    }

    fn bytes_init(&self) -> usize {
        match &self.layout {
            SliceLayout::Slice { len, .. } => *len - self.start,
            SliceLayout::Vec(vec) => vec.len() - self.start,
            #[cfg(feature = "bytes")]
            SliceLayout::Bytes(bytes) => bytes.len() - self.start,
            #[cfg(feature = "bytes")]
            SliceLayout::BytesMut(bytes) => bytes.len() - self.start,
        }
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        match &self.layout {
            SliceLayout::Slice { ptr, .. } => bytes::Bytes::copy_from_slice(unsafe {
                std::slice::from_raw_parts((*ptr).add(self.start), self.end - self.start)
            }),
            SliceLayout::Vec(vec) => bytes::Bytes::copy_from_slice(&vec[self.start..self.end]),
            #[cfg(feature = "bytes")]
            SliceLayout::Bytes(bytes) => bytes.slice(self.start..self.end),
            #[cfg(feature = "bytes")]
            SliceLayout::BytesMut(bytes) => bytes.clone().freeze().slice(self.start..self.end),
        }
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Slice {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&start) => start,
            std::ops::Bound::Excluded(&start) => start + 1,
            std::ops::Bound::Unbounded => self.start,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&end) => end + 1,
            std::ops::Bound::Excluded(&end) => end,
            std::ops::Bound::Unbounded => self.end,
        };
        Slice {
            layout: self.layout,
            start,
            end,
        }
    }

    unsafe fn recover_from_slice(buf: Slice) -> Self {
        buf
    }
}

pub struct SliceMut {
    pub(super) layout: SliceMutLayout,
    pub(super) start: usize,
    pub(super) end: usize,
}

#[cfg(not(feature = "no-send"))]
unsafe impl Send for SliceMut {}

pub(super) enum SliceMutLayout {
    #[allow(unused)]
    Slice {
        ptr: *mut u8,
        len: usize,
    },
    Vec(Vec<u8>),
    #[cfg(feature = "bytes")]
    BytesMut(bytes::BytesMut),
}

unsafe impl Send for SliceMutLayout {}

impl IoBuf for SliceMut {
    fn as_ptr(&self) -> *const u8 {
        match &self.layout {
            SliceMutLayout::Slice { ptr, .. } => unsafe { (*ptr).add(self.start) },
            SliceMutLayout::Vec(vec) => vec[self.start..].as_ptr(),
            #[cfg(feature = "bytes")]
            SliceMutLayout::BytesMut(bytes) => bytes[self.start..].as_ptr(),
        }
    }

    fn bytes_init(&self) -> usize {
        match &self.layout {
            SliceMutLayout::Slice { len, .. } => *len - self.start,
            SliceMutLayout::Vec(vec) => vec.len() - self.start,
            #[cfg(feature = "bytes")]
            SliceMutLayout::BytesMut(bytes) => bytes.len() - self.start,
        }
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        match &self.layout {
            SliceMutLayout::Slice { ptr, .. } => bytes::Bytes::copy_from_slice(unsafe {
                std::slice::from_raw_parts((*ptr).add(self.start), self.end - self.start)
            }),
            SliceMutLayout::Vec(vec) => bytes::Bytes::copy_from_slice(&vec[self.start..self.end]),
            #[cfg(feature = "bytes")]
            SliceMutLayout::BytesMut(bytes) => bytes.clone().freeze().slice(self.start..self.end),
        }
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Slice {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&start) => start,
            std::ops::Bound::Excluded(&start) => start + 1,
            std::ops::Bound::Unbounded => self.start,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&end) => end + 1,
            std::ops::Bound::Excluded(&end) => end,
            std::ops::Bound::Unbounded => self.end,
        };
        match self.layout {
            SliceMutLayout::Slice { ptr, len } => Slice {
                layout: SliceLayout::Slice {
                    ptr: ptr as *const u8,
                    len,
                },
                start,
                end,
            },
            SliceMutLayout::Vec(vec) => Slice {
                layout: SliceLayout::Vec(vec),
                start,
                end,
            },
            #[cfg(feature = "bytes")]
            SliceMutLayout::BytesMut(bytes) => Slice {
                layout: SliceLayout::BytesMut(bytes),
                start,
                end,
            },
        }
    }

    unsafe fn recover_from_slice(buf: Slice) -> Self {
        match buf.layout {
            SliceLayout::Slice { ptr, len } => SliceMut {
                layout: SliceMutLayout::Slice {
                    ptr: ptr as *mut u8,
                    len,
                },
                start: buf.start,
                end: buf.end,
            },
            SliceLayout::Vec(vec) => SliceMut {
                layout: SliceMutLayout::Vec(vec),
                start: buf.start,
                end: buf.end,
            },
            #[cfg(feature = "bytes")]
            SliceLayout::Bytes(_) => unreachable!(),
            #[cfg(feature = "bytes")]
            SliceLayout::BytesMut(bytes) => SliceMut {
                layout: SliceMutLayout::BytesMut(bytes),
                start: buf.start,
                end: buf.end,
            },
        }
    }
}

impl IoBufMut for SliceMut {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        match &mut self.layout {
            SliceMutLayout::Slice { ptr, .. } => unsafe { (*ptr).add(self.start) },
            SliceMutLayout::Vec(vec) => vec[self.start..].as_mut_ptr(),
            #[cfg(feature = "bytes")]
            SliceMutLayout::BytesMut(bytes) => bytes[self.start..].as_mut_ptr(),
        }
    }

    unsafe fn slice_mut_unchecked(self, range: impl RangeBounds<usize>) -> SliceMut {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&start) => start,
            std::ops::Bound::Excluded(&start) => start + 1,
            std::ops::Bound::Unbounded => self.start,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&end) => end + 1,
            std::ops::Bound::Excluded(&end) => end,
            std::ops::Bound::Unbounded => self.end,
        };
        SliceMut {
            layout: self.layout,
            start,
            end,
        }
    }

    unsafe fn recover_from_slice_mut(buf: SliceMut) -> Self {
        buf
    }
}

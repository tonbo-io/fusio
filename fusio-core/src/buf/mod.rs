pub mod slice;

#[cfg(feature = "alloc")]
use alloc::vec::Vec;
use core::ops::{Bound, RangeBounds};

use slice::{Buf, BufLayout, BufMut, BufMutLayout};

use crate::{maybe::MaybeOwned, MaybeSend};

pub trait IoBuf: Unpin + Sized + MaybeOwned + MaybeSend {
    //! A poll-based I/O and completion-based I/O buffer compatible buffer.
    //! The [`IoBuf`] trait is implemented by buffer types that can be used with [`crate::Read`].
    //! Fusio has already implemented this trait for common buffer types
    //! like `Vec<u8>`, `&[u8]`, `&mut [u8]`, `bytes::Bytes`, `bytes::BytesMut`, every buffer type
    //! may be not be able to be used in all async runtimes, fusio provides compile-time safety to
    //! ensure which buffer types are compatible with the async runtime.

    fn as_ptr(&self) -> *const u8;

    fn bytes_init(&self) -> usize;

    fn as_slice(&self) -> &[u8] {
        // SAFETY: The buffer is pinned and the bytes are initialized.
        unsafe { core::slice::from_raw_parts(self.as_ptr(), self.bytes_init()) }
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::copy_from_slice(self.as_slice())
    }

    /// # Safety
    /// The buffer must be recovered from the same type of buffer before it drops.
    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf;

    /// # Safety
    /// The buffer must be recovered from the same type.
    unsafe fn recover_from_slice(buf: Buf) -> Self;

    fn calculate_bounds<R: RangeBounds<usize>>(&self, range: R) -> (usize, usize) {
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end + 1,
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.bytes_init(),
        };
        (start, end)
    }
}

pub trait IoBufMut: IoBuf {
    //! Mutable version of [`IoBuf`] which is used with [`crate::Write`].

    fn as_mut_ptr(&mut self) -> *mut u8;

    fn as_slice_mut(&mut self) -> &mut [u8] {
        // SAFETY: The buffer is pinned and the bytes are initialized.
        unsafe { core::slice::from_raw_parts_mut(self.as_mut_ptr(), self.bytes_init()) }
    }

    /// # Safety
    /// The buffer must be recovered from the same type of buffer before it drops.
    unsafe fn slice_mut_unchecked(self, range: impl RangeBounds<usize>) -> BufMut;

    /// # Safety
    /// The buffer must be recovered from the same type.
    unsafe fn recover_from_slice_mut(buf: BufMut) -> Self;
}

#[cfg(feature = "alloc")]
impl IoBuf for Vec<u8> {
    fn as_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf {
        let (start, end) = self.calculate_bounds(range);
        Buf {
            layout: BufLayout::Vec(self),
            start,
            end,
        }
    }

    unsafe fn recover_from_slice(buf: Buf) -> Self {
        match buf.layout {
            BufLayout::Vec(vec) => vec,
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "alloc")]
impl IoBufMut for Vec<u8> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        Vec::as_mut_ptr(self)
    }

    unsafe fn slice_mut_unchecked(self, range: impl RangeBounds<usize>) -> BufMut {
        let (start, end) = self.calculate_bounds(range);
        BufMut {
            layout: BufMutLayout::Vec(self),
            start,
            end,
        }
    }

    unsafe fn recover_from_slice_mut(buf: BufMut) -> Self {
        match buf.layout {
            BufMutLayout::Vec(vec) => vec,
            _ => unreachable!(),
        }
    }
}

#[cfg(not(feature = "completion-based"))]
impl IoBuf for &[u8] {
    fn as_ptr(&self) -> *const u8 {
        (*self).as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf {
        let (start, end) = self.calculate_bounds(range);
        Buf {
            layout: BufLayout::Slice {
                ptr: self.as_ptr() as *mut u8,
                len: self.len(),
            },
            start,
            end,
        }
    }

    unsafe fn recover_from_slice(buf: Buf) -> Self {
        match buf.layout {
            BufLayout::Slice { ptr, len } => core::slice::from_raw_parts(ptr, len),
            _ => unreachable!(),
        }
    }
}

#[cfg(not(feature = "completion-based"))]
impl IoBuf for &mut [u8] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf {
        let (start, end) = self.calculate_bounds(range);
        Buf {
            layout: BufLayout::Slice {
                ptr: self.as_ptr() as *mut u8,
                len: self.len(),
            },
            start,
            end,
        }
    }

    unsafe fn recover_from_slice(buf: Buf) -> Self {
        match buf.layout {
            BufLayout::Slice { ptr, len } => core::slice::from_raw_parts_mut(ptr as *mut u8, len),
            _ => unreachable!(),
        }
    }
}

#[cfg(not(feature = "completion-based"))]
impl IoBufMut for &mut [u8] {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    unsafe fn slice_mut_unchecked(self, range: impl RangeBounds<usize>) -> BufMut {
        let (start, end) = self.calculate_bounds(range);
        BufMut {
            layout: BufMutLayout::Slice {
                ptr: self.as_mut_ptr(),
                len: self.len(),
            },
            start,
            end,
        }
    }

    unsafe fn recover_from_slice_mut(buf: BufMut) -> Self {
        match buf.layout {
            BufMutLayout::Slice { ptr, len } => core::slice::from_raw_parts_mut(ptr, len),
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "completion-based")]
impl IoBuf for &'static [u8] {
    fn as_ptr(&self) -> *const u8 {
        (*self).as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from_static(self)
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf {
        let (start, end) = self.calculate_bounds(range);
        Buf {
            layout: BufLayout::Slice {
                ptr: self.as_ptr() as *mut u8,
                len: self.len(),
            },
            start,
            end,
        }
    }

    unsafe fn recover_from_slice(buf: Buf) -> Self {
        match buf.layout {
            BufLayout::Slice { ptr, len } => core::slice::from_raw_parts(ptr, len),
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "bytes")]
impl IoBuf for bytes::Bytes {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
    fn as_bytes(&self) -> bytes::Bytes {
        self.clone()
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf {
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end + 1,
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.len(),
        };
        Buf {
            layout: BufLayout::Bytes(self),
            start,
            end,
        }
    }

    unsafe fn recover_from_slice(buf: Buf) -> Self {
        match buf.layout {
            BufLayout::Bytes(bytes) => bytes,
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "bytes")]
impl IoBuf for bytes::BytesMut {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn as_bytes(&self) -> bytes::Bytes {
        self.clone().freeze()
    }

    unsafe fn slice_unchecked(self, range: impl RangeBounds<usize>) -> Buf {
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end + 1,
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.len(),
        };
        Buf {
            layout: BufLayout::BytesMut(self),
            start,
            end,
        }
    }

    unsafe fn recover_from_slice(buf: Buf) -> Self {
        match buf.layout {
            BufLayout::BytesMut(bytes) => bytes,
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "bytes")]
impl IoBufMut for bytes::BytesMut {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    unsafe fn slice_mut_unchecked(self, range: impl RangeBounds<usize>) -> BufMut {
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end + 1,
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.len(),
        };
        BufMut {
            layout: BufMutLayout::BytesMut(self),
            start,
            end,
        }
    }

    unsafe fn recover_from_slice_mut(buf: BufMut) -> Self {
        match buf.layout {
            BufMutLayout::BytesMut(bytes) => bytes,
            _ => unreachable!(),
        }
    }
}

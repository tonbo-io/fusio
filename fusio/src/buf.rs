use crate::MaybeSend;

/// # Safety
/// Completion-based I/O operations require the buffer to be pinned.
#[cfg(not(feature = "completion-based"))]
pub unsafe trait MaybeOwned {}
#[cfg(not(feature = "completion-based"))]
unsafe impl<T> MaybeOwned for T {}

/// # Safety
/// Completion-based I/O operations require the buffer to be pinned.
#[cfg(feature = "completion-based")]
pub unsafe trait MaybeOwned: 'static {}

#[cfg(feature = "completion-based")]
unsafe impl<T: 'static> MaybeOwned for T {}

pub trait IoBuf: Unpin + Sized + MaybeOwned + MaybeSend {
    fn as_ptr(&self) -> *const u8;

    fn bytes_init(&self) -> usize;

    fn as_slice(&self) -> &[u8] {
        // SAFETY: The buffer is pinned and the bytes are initialized.
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.bytes_init()) }
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::copy_from_slice(self.as_slice())
    }

    /// # Safety
    /// The buffer must be recovered from the same type of buffer before it drops.
    unsafe fn to_buf_nocopy(self) -> Buf;

    /// # Safety
    /// The buffer must be recovered from the same type.
    unsafe fn recover_from_buf(buf: Buf) -> Self;
}

pub trait IoBufMut: IoBuf {
    fn set_init(&mut self, init: usize);

    fn as_mut_ptr(&mut self) -> *mut u8;

    fn as_slice_mut(&mut self) -> &mut [u8] {
        // SAFETY: The buffer is pinned and the bytes are initialized.
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.bytes_init()) }
    }

    /// # Safety
    /// The buffer must be recovered from the same type of buffer before it drops.
    unsafe fn to_buf_mut_nocopy(self) -> BufMut;

    /// # Safety
    /// The buffer must be recovered from the same type.
    unsafe fn recover_from_buf_mut(buf: BufMut) -> Self;
}

impl IoBuf for Vec<u8> {
    fn as_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    unsafe fn to_buf_nocopy(self) -> Buf {
        Buf(BufInner::Vec(self))
    }

    unsafe fn recover_from_buf(buf: Buf) -> Self {
        match buf.0 {
            BufInner::Vec(vec) => vec,
            _ => unreachable!(),
        }
    }
}

impl IoBufMut for Vec<u8> {
    fn set_init(&mut self, init: usize) {
        self.resize(init, 0);
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        Vec::as_mut_ptr(self)
    }

    unsafe fn to_buf_mut_nocopy(self) -> BufMut {
        BufMut {
            layout: BufMutInner::Vec(self),
            start: 0,
        }
    }

    unsafe fn recover_from_buf_mut(buf: BufMut) -> Self {
        match buf.layout {
            BufMutInner::Vec(vec) => vec,
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

    unsafe fn to_buf_nocopy(self) -> Buf {
        Buf(BufInner::Slice {
            ptr: self.as_ptr(),
            len: self.len(),
        })
    }

    unsafe fn recover_from_buf(buf: Buf) -> Self {
        match buf.0 {
            BufInner::Slice { ptr, len } => std::slice::from_raw_parts(ptr, len),
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

    unsafe fn to_buf_nocopy(self) -> Buf {
        Buf(BufInner::Slice {
            ptr: self.as_ptr(),
            len: self.len(),
        })
    }

    unsafe fn recover_from_buf(buf: Buf) -> Self {
        match buf.0 {
            BufInner::Slice { ptr, len } => std::slice::from_raw_parts_mut(ptr as *mut u8, len),
            _ => unreachable!(),
        }
    }
}

#[cfg(not(feature = "completion-based"))]
impl IoBufMut for &mut [u8] {
    fn set_init(&mut self, _init: usize) {}

    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    unsafe fn to_buf_mut_nocopy(self) -> BufMut {
        BufMut {
            layout: BufMutInner::Slice {
                ptr: self.as_mut_ptr(),
                len: self.len(),
            },
            start: 0,
        }
    }

    unsafe fn recover_from_buf_mut(buf: BufMut) -> Self {
        match buf.layout {
            BufMutInner::Slice { ptr, len } => std::slice::from_raw_parts_mut(ptr, len),
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

    unsafe fn to_buf_nocopy(self) -> Buf {
        Buf(BufInner::Slice {
            ptr: self.as_ptr(),
            len: self.len(),
        })
    }

    unsafe fn recover_from_buf(buf: Buf) -> Self {
        match buf.0 {
            BufInner::Slice { ptr, len } => std::slice::from_raw_parts(ptr, len),
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

    unsafe fn to_buf_nocopy(self) -> Buf {
        Buf(BufInner::Bytes(self))
    }

    unsafe fn recover_from_buf(buf: Buf) -> Self {
        match buf.0 {
            BufInner::Bytes(bytes) => bytes,
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

    unsafe fn to_buf_nocopy(self) -> Buf {
        Buf(BufInner::BytesMut(self))
    }

    unsafe fn recover_from_buf(buf: Buf) -> Self {
        match buf.0 {
            BufInner::BytesMut(bytes) => bytes,
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "bytes")]
impl IoBufMut for bytes::BytesMut {
    fn set_init(&mut self, init: usize) {
        self.resize(init, 0)
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    unsafe fn to_buf_mut_nocopy(self) -> BufMut {
        BufMut {
            layout: BufMutInner::BytesMut(self),
            start: 0,
        }
    }

    unsafe fn recover_from_buf_mut(buf: BufMut) -> Self {
        match buf.layout {
            BufMutInner::BytesMut(bytes) => bytes,
            _ => unreachable!(),
        }
    }
}

pub struct Buf(BufInner);

#[cfg(not(feature = "no-send"))]
unsafe impl Send for Buf {}

enum BufInner {
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

impl IoBuf for Buf {
    fn as_ptr(&self) -> *const u8 {
        match &self.0 {
            BufInner::Slice { ptr, .. } => *ptr,
            BufInner::Vec(vec) => vec.as_ptr(),
            #[cfg(feature = "bytes")]
            BufInner::Bytes(bytes) => bytes.as_ptr(),
            #[cfg(feature = "bytes")]
            BufInner::BytesMut(bytes) => bytes.as_ptr(),
        }
    }

    fn bytes_init(&self) -> usize {
        match &self.0 {
            BufInner::Slice { len, .. } => *len,
            BufInner::Vec(vec) => vec.len(),
            #[cfg(feature = "bytes")]
            BufInner::Bytes(bytes) => bytes.len(),
            #[cfg(feature = "bytes")]
            BufInner::BytesMut(bytes) => bytes.len(),
        }
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        match &self.0 {
            BufInner::Slice { ptr, len } => {
                bytes::Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(*ptr, *len) })
            }
            BufInner::Vec(vec) => bytes::Bytes::copy_from_slice(vec),
            #[cfg(feature = "bytes")]
            BufInner::Bytes(bytes) => bytes.clone(),
            #[cfg(feature = "bytes")]
            BufInner::BytesMut(bytes) => bytes.clone().freeze(),
        }
    }

    unsafe fn to_buf_nocopy(self) -> Buf {
        self
    }

    unsafe fn recover_from_buf(buf: Buf) -> Self {
        buf
    }
}

pub struct BufMut {
    layout: BufMutInner,
    start: usize,
}

impl BufMut {
    pub(crate) fn set_start(&mut self, start: usize) {
        self.start = start;
    }
}

#[cfg(not(feature = "no-send"))]
unsafe impl Send for BufMut {}

pub(crate) enum BufMutInner {
    #[allow(unused)]
    Slice {
        ptr: *mut u8,
        len: usize,
    },
    Vec(Vec<u8>),
    #[cfg(feature = "bytes")]
    BytesMut(bytes::BytesMut),
}

impl IoBuf for BufMut {
    fn as_ptr(&self) -> *const u8 {
        match &self.layout {
            BufMutInner::Slice { ptr, .. } => unsafe { (*ptr).add(self.start) },
            BufMutInner::Vec(vec) => vec[self.start..].as_ptr(),
            #[cfg(feature = "bytes")]
            BufMutInner::BytesMut(bytes) => bytes.as_ptr(),
        }
    }

    fn bytes_init(&self) -> usize {
        match &self.layout {
            BufMutInner::Slice { len, .. } => *len - self.start,
            BufMutInner::Vec(vec) => vec.len() - self.start,
            #[cfg(feature = "bytes")]
            BufMutInner::BytesMut(bytes) => bytes.len() - self.start,
        }
    }

    #[cfg(feature = "bytes")]
    fn as_bytes(&self) -> bytes::Bytes {
        match &self.layout {
            BufMutInner::Slice { ptr, len } => {
                bytes::Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(*ptr, *len) })
            }
            BufMutInner::Vec(vec) => bytes::Bytes::copy_from_slice(&vec[self.start..]),
            #[cfg(feature = "bytes")]
            BufMutInner::BytesMut(bytes) => bytes.clone().freeze(),
        }
    }

    unsafe fn to_buf_nocopy(self) -> Buf {
        match self.layout {
            BufMutInner::Slice { ptr, len } => Buf(BufInner::Slice { ptr, len }),
            BufMutInner::Vec(vec) => Buf(BufInner::Vec(vec)),
            #[cfg(feature = "bytes")]
            BufMutInner::BytesMut(bytes) => Buf(BufInner::Bytes(bytes.freeze())),
        }
    }

    unsafe fn recover_from_buf(buf: Buf) -> Self {
        match buf.0 {
            BufInner::Slice { .. } => unreachable!(),
            BufInner::Vec(vec) => BufMut {
                layout: BufMutInner::Vec(vec),
                start: 0,
            },
            #[cfg(feature = "bytes")]
            BufInner::Bytes(_) => unreachable!(),
            #[cfg(feature = "bytes")]
            BufInner::BytesMut(bytes) => BufMut {
                layout: BufMutInner::BytesMut(bytes),
                start: 0,
            },
        }
    }
}

impl IoBufMut for BufMut {
    fn set_init(&mut self, init: usize) {
        match &mut self.layout {
            BufMutInner::Slice { .. } => {}
            BufMutInner::Vec(vec) => vec.set_init(init),
            #[cfg(feature = "bytes")]
            BufMutInner::BytesMut(bytes) => bytes.set_init(init),
        }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        match &mut self.layout {
            BufMutInner::Slice { ptr, .. } => *ptr,
            BufMutInner::Vec(vec) => vec.as_mut_ptr(),
            #[cfg(feature = "bytes")]
            BufMutInner::BytesMut(bytes) => bytes.as_mut_ptr(),
        }
    }

    unsafe fn to_buf_mut_nocopy(self) -> BufMut {
        self
    }

    unsafe fn recover_from_buf_mut(buf: BufMut) -> Self {
        buf
    }
}

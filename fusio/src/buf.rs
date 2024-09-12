#[cfg(all(not(feature = "completion-based"), feature = "no-send"))]
pub unsafe trait IoBuf: Unpin {
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
}

#[cfg(all(not(feature = "completion-based"), not(feature = "no-send")))]
pub unsafe trait IoBuf: Unpin + Send {
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
}

/// # Safety
/// Completion-based I/O operations require the buffer to be pinned.
#[cfg(feature = "completion-based")]
pub unsafe trait IoBuf: Unpin + 'static {
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
}

/// # Safety
/// Completion-based I/O operations require the buffer to be pinned.
pub unsafe trait IoBufMut: IoBuf {
    fn as_mut_ptr(&mut self) -> *mut u8;

    /// # Safety
    /// The caller must ensure that all bytes starting at stable_mut_ptr() up to pos are initialized
    /// and owned by the buffer.
    unsafe fn set_init(&mut self, pos: usize);

    fn bytes_total(&self) -> usize;
}

unsafe impl IoBuf for Vec<u8> {
    fn as_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

unsafe impl IoBufMut for Vec<u8> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.set_len(pos);
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

#[cfg(not(feature = "completion-based"))]
unsafe impl IoBuf for &[u8] {
    fn as_ptr(&self) -> *const u8 {
        (*self).as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

#[cfg(not(feature = "completion-based"))]
unsafe impl IoBuf for &mut [u8] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

#[cfg(not(feature = "completion-based"))]
unsafe impl IoBufMut for &mut [u8] {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    unsafe fn set_init(&mut self, _pos: usize) {}

    fn bytes_total(&self) -> usize {
        self.len()
    }
}

#[cfg(feature = "completion-based")]
unsafe impl IoBuf for &'static [u8] {
    fn as_ptr(&self) -> *const u8 {
        (*self).as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

#[cfg(feature = "completion-based")]
unsafe impl IoBuf for &'static mut [u8] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

#[cfg(feature = "completion-based")]
unsafe impl IoBufMut for &'static mut [u8] {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    unsafe fn set_init(&mut self, _pos: usize) {}

    fn bytes_total(&self) -> usize {
        self.len()
    }
}

#[cfg(feature = "bytes")]
unsafe impl IoBuf for bytes::Bytes {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }

    fn as_bytes(&self) -> bytes::Bytes {
        self.clone()
    }
}

#[cfg(feature = "bytes")]
unsafe impl IoBuf for bytes::BytesMut {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
    fn as_bytes(&self) -> bytes::Bytes {
        self.clone().freeze()
    }
}

#[cfg(feature = "bytes")]
unsafe impl IoBufMut for bytes::BytesMut {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    unsafe fn set_init(&mut self, pos: usize) {
        if self.len() < pos {
            self.set_len(pos);
        }
    }

    fn bytes_total(&self) -> usize {
        self.capacity()
    }
}

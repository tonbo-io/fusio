use std::io;

use web_sys::{FileSystemFileHandle, FileSystemReadWriteOptions, FileSystemSyncAccessHandle};

use crate::{disk::opfs::promise, error::wasm_err, Error, IoBuf, IoBufMut, Read, Write};

/// OPFS based on [FileSystemWritableFileStream](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle)
/// This file is only accessible inside dedicated Web Workers.
pub struct OPFSSyncFile {
    access_handle: Option<FileSystemSyncAccessHandle>,
}

impl OPFSSyncFile {
    pub(crate) async fn new(file_handle: FileSystemFileHandle) -> Result<Self, Error> {
        let js_promise = file_handle.create_sync_access_handle();
        let access_handle = Some(promise::<FileSystemSyncAccessHandle>(js_promise).await?);
        Ok(Self { access_handle })
    }
}

impl Write for OPFSSyncFile {
    /// Attempts to write an entire buffer into the file.
    ///
    /// No changes are written to the actual file on disk until [`OPFSFile::close`] has been called.
    /// See more detail in [write](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle/write)
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        debug_assert!(self.access_handle.is_some(), "file is already closed");

        match self
            .access_handle
            .as_ref()
            .unwrap()
            .write_with_u8_array(buf.as_slice())
        {
            Ok(_) => (Ok(()), buf),
            Err(err) => (Err(wasm_err(err)), buf),
        }
    }

    /// Persists any changes made to the file.
    /// See more detail in [write](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle/flush)
    async fn flush(&mut self) -> Result<(), Error> {
        debug_assert!(self.access_handle.is_some(), "file is already closed");

        self.access_handle
            .as_ref()
            .unwrap()
            .flush()
            .map_err(wasm_err)
    }

    async fn close(&mut self) -> Result<(), Error> {
        debug_assert!(self.access_handle.is_some(), "file is already closed");

        if let Some(access_handle) = self.access_handle.take() {
            access_handle.close();
        }
        Ok(())
    }
}

impl Read for OPFSSyncFile {
    /// Reads the exact number of bytes required to fill `buf` at `pos`.
    ///
    /// # Errors
    ///
    /// If the operation encounters an "end of file" before completely
    /// filling the buffer, it returns an error of  [`crate::Error`].
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        debug_assert!(self.access_handle.is_some(), "file is already closed");

        let buf_len = buf.bytes_init() as i32;
        let options = FileSystemReadWriteOptions::new();
        options.set_at(pos as f64);

        let access_handle = self.access_handle.as_ref().unwrap();
        let size = access_handle
            .get_size()
            .expect("InvalidStateError: file is already closed.");
        if (size.round() as u64) < pos + buf_len as u64 {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Read unexpected eof",
                ))),
                buf,
            );
        }
        match access_handle.read_with_u8_array_and_options(buf.as_slice_mut(), &options) {
            Ok(_) => (Ok(()), buf),
            Err(err) => (Err(wasm_err(err)), buf),
        }
    }

    /// Reads all bytes until EOF in this source, placing them into `buf`.
    ///
    /// # Errors
    ///
    /// If an error is encountered then the `read_to_end_at` operation
    /// immediately completes.
    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        debug_assert!(self.access_handle.is_some(), "file is already closed");

        let options = FileSystemReadWriteOptions::new();
        options.set_at(pos as f64);

        let access_handle = self.access_handle.as_ref().unwrap();
        let size = access_handle
            .get_size()
            .expect("InvalidStateError: file is already closed.");
        let buf_len = size.round() as usize - pos as usize;
        if buf_len == 0 {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Read unexpected eof",
                ))),
                buf,
            );
        }
        buf.resize(buf_len, 0);

        match access_handle.read_with_u8_array_and_options(buf.as_slice_mut(), &options) {
            Ok(_) => (Ok(()), buf),
            Err(err) => (Err(wasm_err(err)), buf),
        }
    }

    /// Return the size of file in bytes.
    async fn size(&self) -> Result<u64, Error> {
        debug_assert!(self.access_handle.is_some(), "file is already closed");
        self.access_handle
            .as_ref()
            .unwrap()
            .get_size()
            .map(|sz| sz.round() as u64)
            .map_err(wasm_err)
    }
}

impl Drop for OPFSSyncFile {
    fn drop(&mut self) {
        if let Some(access_handle) = self.access_handle.take() {
            access_handle.close();
        }
    }
}

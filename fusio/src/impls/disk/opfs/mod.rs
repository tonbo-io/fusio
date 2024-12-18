#[cfg(feature = "fs")]
pub mod fs;

#[cfg(feature = "sync")]
pub mod sync;

use std::{io, sync::Arc};

use js_sys::Uint8Array;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    wasm_bindgen::JsCast, window, File, FileSystemCreateWritableOptions, FileSystemDirectoryHandle,
    FileSystemFileHandle, FileSystemWritableFileStream, ReadableStreamDefaultReader,
    ReadableStreamReadResult, WorkerGlobalScope,
};

use crate::{error::wasm_err, Error, IoBuf, IoBufMut, Read, Write};

pub(crate) async fn promise<T>(promise: js_sys::Promise) -> Result<T, Error>
where
    T: JsCast,
{
    let js_val = JsFuture::from(promise).await.map_err(wasm_err)?;

    js_val.dyn_into::<T>().map_err(|_obj| Error::CastError)
}

/// File handle of OPFS file
pub struct FileHandle {
    file_handle: FileSystemFileHandle,
}

impl FileHandle {
    fn new(file_handle: FileSystemFileHandle) -> Self {
        Self { file_handle }
    }
}

impl FileHandle {
    /// Attempts to write an entire buffer into the file.
    ///
    /// Unlike [`OPFSFile::write_all`], changes will be written to the actual file.
    pub async fn write_at<B: IoBuf>(&self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let options = FileSystemCreateWritableOptions::new();
        options.set_keep_existing_data(true);

        let writer_promise = self.create_writable_with_options(&options);
        let writer = match promise::<FileSystemWritableFileStream>(writer_promise).await {
            Ok(writer) => writer,
            Err(err) => return (Err(err), buf),
        };

        if let Err(err) = JsFuture::from(writer.seek_with_u32(pos as u32).unwrap()).await {
            return (Err(wasm_err(err)), buf);
        }

        let (result, buf) = self.write_with_stream(buf, &writer).await;
        if result.is_err() {
            return (result, buf);
        }
        let result = JsFuture::from(writer.close())
            .await
            .map_err(wasm_err)
            .map(|_| ());

        (result, buf)
    }

    /// Attempts to write an entire buffer into the stream.
    ///
    /// No changes are written to the actual file on disk until the stream is closed.
    async fn write_with_stream<B: IoBuf>(
        &self,
        buf: B,
        stream: &FileSystemWritableFileStream,
    ) -> (Result<(), Error>, B) {
        match JsFuture::from(stream.write_with_u8_array(buf.as_slice()).unwrap()).await {
            Ok(_) => (Ok(()), buf),
            Err(err) => (Err(wasm_err(err)), buf),
        }
    }

    /// Create a `FileSystemWritableFileStream` and return a JavaScript Promise
    fn create_writable_with_options(
        &self,
        options: &FileSystemCreateWritableOptions,
    ) -> js_sys::Promise {
        self.file_handle.create_writable_with_options(options)
    }
}

impl FileHandle {
    /// Reads all bytes until EOF in this source, placing them into `buf`.
    ///
    /// # Errors
    ///
    /// If an error is encountered then the `read_to_end_at` operation
    /// immediately completes.
    async fn read_to_end_at(&self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let file_promise = self.file_handle.get_file();
        let file = match promise::<File>(file_promise).await {
            Ok(file) => file,
            Err(err) => return (Err(err), buf),
        };

        if (file.size().round() as u64) < pos + buf.bytes_init() as u64 {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Read unexpected eof",
                ))),
                buf,
            );
        }

        let blob = file.slice_with_i32(pos as i32).unwrap();
        let reader = match blob
            .stream()
            .get_reader()
            .dyn_into::<ReadableStreamDefaultReader>()
            .map_err(|_obj| Error::CastError)
        {
            Ok(reader) => reader,
            Err(err) => return (Err(err), buf),
        };

        while let Ok(v) = JsFuture::from(reader.read()).await {
            let result = ReadableStreamReadResult::from(v);
            if result.get_done().unwrap() {
                break;
            }
            let chunk = result.get_value().dyn_into::<Uint8Array>().unwrap();
            buf.extend(chunk.to_vec());
        }

        (Ok(()), buf)
    }

    /// Reads the exact number of bytes required to fill `buf` at `pos`.
    ///
    /// # Errors
    ///
    /// If the operation encounters an "end of file" before completely
    /// filling the buffer, it returns an error of  [`Error::Io`].
    pub async fn read_exact_at<B: IoBufMut>(&self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let buf_len = buf.bytes_init() as i32;
        let buf_slice = buf.as_slice_mut();
        let end = pos as i32 + buf_len;

        let file = match promise::<File>(self.file_handle.get_file()).await {
            Ok(file) => file,
            Err(err) => return (Err(err), buf),
        };

        if (file.size().round() as u64) < pos + buf_len as u64 {
            return (
                Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Read unexpected eof",
                ))),
                buf,
            );
        }
        let blob = file.slice_with_i32_and_i32(pos as i32, end).unwrap();
        let reader = match blob
            .stream()
            .get_reader()
            .dyn_into::<ReadableStreamDefaultReader>()
            .map_err(|_obj| Error::CastError)
        {
            Ok(reader) => reader,
            Err(err) => return (Err(err), buf),
        };

        let mut offset = 0;
        while let Ok(v) = JsFuture::from(reader.read()).await {
            let result = ReadableStreamReadResult::from(v);
            if result.get_done().unwrap() {
                break;
            }

            let chunk = result.get_value().dyn_into::<Uint8Array>().unwrap();
            let chunk_len = chunk.length() as usize;
            buf_slice[offset..offset + chunk_len].copy_from_slice(chunk.to_vec().as_slice());
            offset += chunk_len;
        }

        (Ok(()), buf)
    }

    pub async fn size(&self) -> Result<u64, Error> {
        let file = promise::<File>(self.file_handle.get_file()).await?;

        Ok(file.size() as u64)
    }
}

/// OPFS based on [FileSystemWritableFileStream](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemWritableFileStream)
pub struct OPFSFile {
    file_handle: Option<Arc<FileHandle>>,
    write_stream: Option<FileSystemWritableFileStream>,
    pos: u32,
}

impl OPFSFile {
    #[allow(unused)]
    pub(crate) fn new(file_handle: FileSystemFileHandle) -> Self {
        Self {
            file_handle: Some(Arc::new(FileHandle::new(file_handle))),
            write_stream: None,
            pos: 0,
        }
    }

    pub fn file_handle(&self) -> Option<Arc<FileHandle>> {
        match self.file_handle.as_ref() {
            None => None,
            Some(file_handle) => Some(Arc::clone(file_handle)),
        }
    }
}

impl Write for OPFSFile {
    /// Attempts to write an entire buffer into the file.
    ///
    /// No changes are written to the actual file on disk until [`OPFSFile::close`] has been called.
    /// See more detail in [write](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemWritableFileStream/write)
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let file_handle = self.file_handle.as_ref().expect("write file after closed");
        if self.write_stream.is_none() {
            let options = FileSystemCreateWritableOptions::new();
            options.set_keep_existing_data(true);
            let writer_promise = file_handle.create_writable_with_options(&options);

            let writer = match promise::<FileSystemWritableFileStream>(writer_promise).await {
                Ok(writer) => writer,
                Err(err) => return (Err(err), buf),
            };

            if let Err(err) = JsFuture::from(writer.seek_with_u32(self.pos).unwrap())
                .await
                .map_err(wasm_err)
            {
                return (Err(err), buf);
            }

            self.write_stream = Some(writer);
        }

        let writer = self.write_stream.as_ref().unwrap();
        let len = buf.bytes_init();
        self.pos += len as u32;
        file_handle.write_with_stream(buf, writer).await
    }

    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /// Close the associated OPFS file.
    async fn close(&mut self) -> Result<(), Error> {
        let writer = self.write_stream.take();
        if let Some(writer) = writer {
            JsFuture::from(writer.close()).await.map_err(wasm_err)?;
        }
        Ok(())
    }
}

impl Read for OPFSFile {
    /// Reads the exact number of bytes required to fill `buf` at `pos`.
    ///
    /// # Errors
    ///
    /// If the operation encounters an "end of file" before completely
    /// filling the buffer, it returns an error of  [`Error::Io`].
    async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let file_handle = self.file_handle.as_ref().expect("read file after closed");

        file_handle.read_exact_at(buf, pos).await
    }

    /// Reads all bytes until EOF in this source, placing them into `buf`.
    ///
    /// # Errors
    ///
    /// If an error is encountered then the `read_to_end_at` operation
    /// immediately completes.
    async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let file_handle = self.file_handle.as_ref().expect("read file after closed");

        file_handle.read_to_end_at(buf, pos).await
    }

    /// Return the size of file in bytes.
    async fn size(&self) -> Result<u64, Error> {
        let file_handle = self.file_handle.as_ref().expect("read file after closed");
        file_handle.size().await
    }
}

pub(crate) async fn storage() -> Result<FileSystemDirectoryHandle, Error> {
    let storage_promise = match window() {
        Some(window) => window.navigator().storage(),
        None => match js_sys::eval("self")
            .unwrap()
            .dyn_into::<WorkerGlobalScope>()
        {
            Ok(worker) => worker.navigator().storage(),
            Err(err) => return Err(wasm_err(err)),
        },
    }
    .get_directory();

    promise::<FileSystemDirectoryHandle>(storage_promise).await
}

#[cfg(feature = "fs")]
pub mod fs;

#[cfg(feature = "sync")]
pub mod sync;

use std::io;

use js_sys::Uint8Array;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    wasm_bindgen::JsCast, window, File, FileSystemCreateWritableOptions, FileSystemDirectoryHandle,
    FileSystemFileHandle, FileSystemWritableFileStream, ReadableStreamDefaultReader,
    ReadableStreamReadResult, WorkerGlobalScope,
};

use crate::{
    error::{wasm_err, Error},
    fs::OpenOptions,
    IoBuf, IoBufMut, Read, Write,
};

pub(crate) async fn promise<T>(promise: js_sys::Promise) -> Result<T, Error>
where
    T: JsCast,
{
    let js_val = JsFuture::from(promise).await.map_err(wasm_err)?;

    js_val.dyn_into::<T>().map_err(|_obj| Error::CastError)
}

/// OPFS based on [FileSystemWritableFileStream](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemWritableFileStream)
pub struct OPFSFile {
    file_handle: Option<FileSystemFileHandle>,
    write_stream: Option<FileSystemWritableFileStream>,
    pos: u64,
}

impl OPFSFile {
    #[allow(unused)]
    pub(crate) async fn new(
        file_handle: FileSystemFileHandle,
        open_options: OpenOptions,
    ) -> Result<Self, Error> {
        let size = if open_options.truncate {
            0.0
        } else {
            let file = promise::<File>(file_handle.get_file()).await?;
            file.size()
        };
        let write_stream = if open_options.truncate || open_options.write {
            let options = FileSystemCreateWritableOptions::new();
            options.set_keep_existing_data(!open_options.truncate);

            let writer_promise = file_handle.create_writable_with_options(&options);
            let write_stream = promise::<FileSystemWritableFileStream>(writer_promise).await?;
            JsFuture::from(write_stream.seek_with_f64(size).unwrap())
                .await
                .map_err(wasm_err)?;

            Some(write_stream)
        } else {
            None
        };

        Ok(Self {
            file_handle: Some(file_handle),
            write_stream,
            pos: size.round() as u64,
        })
    }

    async fn reader(&self, pos: u64, buf_len: u64) -> Result<ReadableStreamDefaultReader, Error> {
        debug_assert!(self.file_handle.is_some());
        let file_handle = self.file_handle.as_ref().expect("read file after closed.");
        let file = promise::<File>(file_handle.get_file()).await?;

        if (file.size().round() as u64) < pos + buf_len as u64 {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Read unexpected eof",
            )));
        }

        let blob = if buf_len == 0 {
            file.slice_with_i32(pos as i32).unwrap()
        } else {
            file.slice_with_i32_and_i32(pos as i32, (pos + buf_len) as i32)
                .unwrap()
        };
        blob.stream()
            .get_reader()
            .dyn_into::<ReadableStreamDefaultReader>()
            .map_err(|_obj| Error::CastError)
    }
}

impl Write for OPFSFile {
    /// Attempts to write an entire buffer into the file.
    ///
    /// No changes are written to the actual file on disk until [`OPFSFile::close`] has been called.
    /// See more detail in [write](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemWritableFileStream/write)
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        debug_assert!(self.write_stream.is_some());

        let len = buf.bytes_init();
        self.pos += len as u64;
        match JsFuture::from(
            self.write_stream
                .as_ref()
                .expect("write file after closed.")
                .write_with_u8_array(buf.as_slice())
                .unwrap(),
        )
        .await
        {
            Ok(_) => (Ok(()), buf),
            Err(err) => (Err(wasm_err(err)), buf),
        }
    }

    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /// Close the associated OPFS file.
    async fn close(&mut self) -> Result<(), Error> {
        if let Some(writer) = self.write_stream.take() {
            JsFuture::from(writer.close()).await.map_err(wasm_err)?;
        }
        let _ = self.file_handle.take();

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
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let buf_len = buf.bytes_init() as u64;
        let buf_slice = buf.as_slice_mut();

        let reader = match self.reader(pos, buf_len).await {
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

    /// Reads all bytes until EOF in this source, placing them into `buf`.
    ///
    /// # Errors
    ///
    /// If an error is encountered then the `read_to_end_at` operation
    /// immediately completes.
    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let reader = match self.reader(pos, 0).await {
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

    /// Return the size of file in bytes.
    async fn size(&self) -> Result<u64, Error> {
        debug_assert!(self.file_handle.is_some());

        let file = promise::<File>(
            self.file_handle
                .as_ref()
                .expect("read file after closed.")
                .get_file(),
        )
        .await?;

        Ok(file.size() as u64)
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

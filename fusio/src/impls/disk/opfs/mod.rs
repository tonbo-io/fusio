#[cfg(feature = "fs")]
pub mod fs;

use std::sync::Arc;

use js_sys::Uint8Array;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    wasm_bindgen::JsCast, window, File, FileSystemCreateWritableOptions, FileSystemDirectoryHandle,
    FileSystemFileHandle, FileSystemWritableFileStream, ReadableStreamDefaultReader,
    ReadableStreamReadResult,
};

use crate::{error::wasm_err, Error, IoBuf, IoBufMut, Read, Write};

pub(crate) async fn promise<T>(promise: js_sys::Promise) -> Result<T, Error>
where
    T: JsCast,
{
    let js_val = JsFuture::from(promise).await.map_err(wasm_err)?;

    js_val.dyn_into::<T>().map_err(|_obj| Error::CastError)
}

pub struct FileHandle {
    file_handle: FileSystemFileHandle,
}

impl FileHandle {
    fn new(file_handle: FileSystemFileHandle) -> Self {
        Self { file_handle }
    }
}

impl FileHandle {
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

    pub async fn write_with_stream<B: IoBuf>(
        &self,
        buf: B,
        stream: &FileSystemWritableFileStream,
    ) -> (Result<(), Error>, B) {
        match JsFuture::from(stream.write_with_u8_array(buf.as_slice()).unwrap()).await {
            Ok(_) => (Ok(()), buf),
            Err(err) => (Err(wasm_err(err)), buf),
        }
    }

    fn create_writable_with_options(
        &self,
        options: &FileSystemCreateWritableOptions,
    ) -> js_sys::Promise {
        self.file_handle.create_writable_with_options(options)
    }
}

impl FileHandle {
    async fn read_to_end_at(&self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let file_promise = self.file_handle.get_file();
        let file = match promise::<File>(file_promise).await {
            Ok(file) => file,
            Err(err) => return (Err(err), buf),
        };

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

    pub async fn read_exact_at<B: IoBufMut>(&self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let buf_len = buf.bytes_init() as i32;
        let buf_slice = buf.as_slice_mut();
        let end = pos as i32 + buf_len;

        let file = match promise::<File>(self.file_handle.get_file()).await {
            Ok(file) => file,
            Err(err) => return (Err(err), buf),
        };

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

pub struct OPFSFile {
    file_handle: Option<Arc<FileHandle>>,
    write_stream: Option<FileSystemWritableFileStream>,
    pos: u32,
}

impl OPFSFile {
    pub fn new(file_handle: FileSystemFileHandle) -> Self {
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

    async fn close(&mut self) -> Result<(), Error> {
        let writer = self.write_stream.take();
        if let Some(writer) = writer {
            JsFuture::from(writer.close()).await.map_err(wasm_err)?;
        }
        self.file_handle.take();
        Ok(())
    }
}

impl Read for OPFSFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let file_handle = self.file_handle.as_ref().expect("read file after closed");

        file_handle.read_exact_at(buf, pos).await
    }

    async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let file_handle = self.file_handle.as_ref().expect("read file after closed");

        file_handle.read_to_end_at(buf, pos).await
    }

    async fn size(&self) -> Result<u64, Error> {
        let file_handle = self.file_handle.as_ref().expect("read file after closed");
        file_handle.size().await
    }
}

pub(crate) async fn storage() -> FileSystemDirectoryHandle {
    let storage_promise = window().unwrap().navigator().storage().get_directory();
    JsFuture::from(storage_promise)
        .await
        .unwrap()
        .dyn_into::<FileSystemDirectoryHandle>()
        .unwrap()
}

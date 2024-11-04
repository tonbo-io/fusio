#[cfg(feature = "fs")]
pub mod fs;

use js_sys::Uint8Array;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    wasm_bindgen::JsCast, window, File, FileSystemCreateWritableOptions, FileSystemDirectoryHandle,
    FileSystemFileHandle, FileSystemWritableFileStream, ReadableStreamDefaultReader,
    ReadableStreamReadResult,
};

use crate::{error::wasm_err, Error, IoBuf, IoBufMut, Read, Write};

pub struct OPFSFile {
    file_handle: Option<FileSystemFileHandle>,
    write_stream: Option<FileSystemWritableFileStream>,
    pos: u32,
}

impl OPFSFile {
    pub(crate) fn new(file_handle: Option<FileSystemFileHandle>) -> Self {
        Self {
            file_handle,
            write_stream: None,
            pos: 0,
        }
    }
}

impl Write for OPFSFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        if let Some(file_handle) = self.file_handle.as_ref() {
            if self.write_stream.is_none() {
                let options = FileSystemCreateWritableOptions::new();
                options.set_keep_existing_data(true);

                let writer = JsFuture::from(file_handle.create_writable_with_options(&options))
                    .await
                    .unwrap()
                    .dyn_into::<FileSystemWritableFileStream>()
                    .unwrap();

                JsFuture::from(writer.seek_with_u32(self.pos).unwrap())
                    .await
                    .unwrap();
                self.write_stream = Some(writer);
            }

            let writer = self.write_stream.as_ref().unwrap();

            let len = buf.bytes_init();
            JsFuture::from(writer.write_with_u8_array(buf.as_slice()).unwrap())
                .await
                .unwrap();
            self.pos += len as u32;
        }
        (Ok(()), buf)
    }

    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let writer = self.write_stream.take();
        if let Some(writer) = writer {
            JsFuture::from(writer.close()).await.map_err(wasm_err)?;
        }
        Ok(())
    }
}

impl Read for OPFSFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        if let Some(file_handle) = self.file_handle.as_ref() {
            let buf_len = buf.bytes_init() as i32;
            let buf_slice = buf.as_slice_mut();
            let end = pos as i32 + buf_len;

            let file = JsFuture::from(file_handle.get_file())
                .await
                .unwrap()
                .dyn_into::<File>()
                .map_err(wasm_err)
                .unwrap();
            let blob = file.slice_with_i32_and_i32(pos as i32, end).unwrap();
            let reader = blob
                .stream()
                .get_reader()
                .dyn_into::<ReadableStreamDefaultReader>()
                .unwrap();
            while let Ok(v) = JsFuture::from(reader.read()).await {
                let result = ReadableStreamReadResult::from(v);
                if result.get_done().unwrap() {
                    break;
                }
                let chunk = result.get_value().dyn_into::<Uint8Array>().unwrap();
                buf_slice.copy_from_slice(chunk.to_vec().as_slice());
            }
        }
        (Ok(()), buf)
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        if let Some(file_handle) = self.file_handle.as_ref() {
            let file = JsFuture::from(file_handle.get_file())
                .await
                .unwrap()
                .dyn_into::<File>()
                .unwrap();

            let blob = file.slice_with_i32(pos as i32).unwrap();
            let reader = blob
                .stream()
                .get_reader()
                .dyn_into::<ReadableStreamDefaultReader>()
                .unwrap();
            while let Ok(v) = JsFuture::from(reader.read()).await {
                let result = ReadableStreamReadResult::from(v);
                if result.get_done().unwrap() {
                    break;
                }
                let chunk = result.get_value().dyn_into::<Uint8Array>().unwrap();
                buf.extend(chunk.to_vec());
            }
        }
        (Ok(()), buf)
    }

    async fn size(&self) -> Result<u64, Error> {
        Ok(self.pos as u64)
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

#[cfg(feature = "fs")]
pub mod fs;

use std::usize;

use js_sys::JsString;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    wasm_bindgen::JsCast, window, File, FileSystemCreateWritableOptions, FileSystemDirectoryHandle,
    FileSystemFileHandle, FileSystemWritableFileStream,
};

use crate::{Error, IoBuf, IoBufMut, Read, Write};

pub struct OPFSFile {
    file_handle: Option<FileSystemFileHandle>,
    write_stream: Option<FileSystemWritableFileStream>,
    pos: u32,
    remain: Option<u8>,
}

impl OPFSFile {
    pub(crate) fn new(file_handle: Option<FileSystemFileHandle>) -> Self {
        Self {
            file_handle,
            write_stream: None,
            pos: 0,
            remain: None,
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

                JsFuture::from(writer.seek_with_u32(self.pos as u32).unwrap())
                    .await
                    .unwrap();
                self.write_stream = Some(writer);
            }

            let writer = self.write_stream.as_ref().unwrap();
            let (s, remain) = Self::encode(buf.as_slice(), self.remain.take());

            let len = s.len();
            self.remain = remain;
            JsFuture::from(
                writer
                    .write_with_u8_array(s.into_bytes().as_slice())
                    .unwrap(),
            )
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
            if let Some(remain) = self.remain.take() {
                JsFuture::from(writer.write_with_u8_array(&[remain]).unwrap())
                    .await
                    .unwrap();
            }
            JsFuture::from(writer.close()).await.unwrap();
        }
        Ok(())
    }
}

impl Read for OPFSFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        if let Some(file_handle) = self.file_handle.as_ref() {
            let buf_len = buf.bytes_init();
            let start = 2_i32.max(pos as i32) - 2;
            let end = pos as i32 + 2 * buf_len as i32 + 2;

            let file = JsFuture::from(file_handle.get_file())
                .await
                .unwrap()
                .dyn_into::<File>()
                .unwrap();
            let blob = file.slice_with_i32_and_i32(start, end).unwrap();
            let text = JsFuture::from(blob.text())
                .await
                .unwrap()
                .dyn_into::<JsString>()
                .unwrap();

            let data = Self::decode(&text);

            let start = (pos - start as u64) as usize;
            let end = start + buf_len;
            if end >= data.len() {
                return (
                    Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Read end of file",
                    ))),
                    buf,
                );
            }

            let buf_slice = buf.as_slice_mut();
            buf_slice.copy_from_slice(&data[start..end]);
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

            let start = 2_i32.max(pos as i32) - 2;
            let blob = file.slice_with_i32(start).unwrap();
            let text = JsFuture::from(blob.text())
                .await
                .unwrap()
                .dyn_into::<JsString>()
                .unwrap();

            let mut data = Self::decode(&text);
            let start = (pos - start as u64) as usize;
            buf.extend_from_slice(&mut data[start..]);
        }
        (Ok(()), buf)
    }

    async fn size(&self) -> Result<u64, Error> {
        Ok(self.pos as u64)
    }
}

impl OPFSFile {
    fn encode(src: &[u8], mut remain: Option<u8>) -> (String, Option<u8>) {
        let mut data = Vec::new();
        let mut iter = src.iter();
        let mut remain_data = None;
        while let Some(num) = iter.next() {
            let (low, high) = match remain.take() {
                Some(low) => (low, Some(num)),
                None => (*num, iter.next()),
            };

            if high.is_none() {
                remain_data = Some(low);
                break;
            }
            data.push(((*high.unwrap() as u16) << 8) | low as u16);
        }
        let s = String::from_utf16(&data).unwrap();
        (s, remain_data)
    }

    fn decode(s: &JsString) -> Vec<u8> {
        let mut data = Vec::with_capacity(s.length() as usize);
        for n in s.iter() {
            let low = n & 0xFF;
            let high = n >> 8;
            data.push(low as u8);
            data.push(high as u8);
        }
        data
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

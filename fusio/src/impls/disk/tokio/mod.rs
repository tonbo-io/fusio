#[cfg(feature = "fs")]
pub mod fs;

use std::{io::SeekFrom, ptr::slice_from_raw_parts};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{buf::IoBufMut, Error, IoBuf, Read, Write};

pub struct TokioFile {
    file: Option<File>,
    pos: u64,
}
impl TokioFile {
    pub(crate) fn new(file: File, pos: u64) -> Self {
        Self {
            file: Some(file),
            pos,
        }
    }
}

impl Write for TokioFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_mut().unwrap();
        if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(self.pos)).await {
            return (Err(Error::Io(e)), buf);
        }
        let buf_len = buf.bytes_init();
        self.pos += buf_len as u64;

        (
            AsyncWriteExt::write_all(self.file.as_mut().unwrap(), unsafe {
                &*slice_from_raw_parts(buf.as_ptr(), buf_len)
            })
            .await
            .map_err(Error::from),
            buf,
        )
    }

    async fn flush(&mut self) -> Result<(), Error> {
        debug_assert!(self.file.is_some(), "file is already closed");

        AsyncWriteExt::flush(self.file.as_mut().unwrap())
            .await
            .map_err(Error::from)
    }

    async fn close(&mut self) -> Result<(), Error> {
        debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_mut().unwrap();
        AsyncWriteExt::flush(file).await.map_err(Error::from)?;
        File::shutdown(file).await?;
        self.file.take();
        Ok(())
    }
}

impl Read for TokioFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        debug_assert!(self.file.is_some(), "file is already closed");

        let size = self.size().await.unwrap();
        if size < pos + buf.bytes_init() as u64 {
            return (
                Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Read unexpected eof",
                ))),
                buf,
            );
        }
        let file = self.file.as_mut().unwrap();
        // TODO: Use pread instead of seek + read_exact
        if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(pos)).await {
            return (Err(Error::Io(e)), buf);
        }
        match AsyncReadExt::read_exact(file, buf.as_slice_mut()).await {
            Ok(_) => (Ok(()), buf),
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_mut().unwrap();
        // TODO: Use pread instead of seek + read_exact
        if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(pos)).await {
            return (Err(Error::Io(e)), buf);
        }
        match AsyncReadExt::read_to_end(file, &mut buf).await {
            Ok(_) => (Ok(()), buf),
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        debug_assert!(self.file.is_some(), "file is already closed");

        Ok(self.file.as_ref().unwrap().metadata().await?.len())
    }
}

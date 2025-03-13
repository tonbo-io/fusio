#[cfg(feature = "fs")]
pub mod fs;

use std::{
    io::SeekFrom,
    os::fd::{AsFd, AsRawFd, FromRawFd},
};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    task::block_in_place,
};

use crate::{buf::IoBufMut, Error, IoBuf, Read, Write};

pub struct TokioFile {
    file: Option<File>,
}
impl TokioFile {
    pub(crate) fn new(file: File) -> Self {
        Self { file: Some(file) }
    }
}

impl Write for TokioFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        debug_assert!(self.file.is_some(), "file is already closed");

        (
            AsyncWriteExt::write_all(self.file.as_mut().unwrap(), buf.as_slice())
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

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            let f = file.as_fd();
            let result = block_in_place(|| {
                let buf = buf.as_slice_mut();
                let file = unsafe { std::fs::File::from_raw_fd(f.as_raw_fd()) };
                let result = file.read_exact_at(buf, pos).map_err(Error::Io);
                std::mem::forget(file);
                result
            });
            (result, buf)
        }
        #[cfg(not(unix))]
        {
            // TODO: Use pread instead of seek + read_exact
            if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            match AsyncReadExt::read_exact(file, buf.as_slice_mut()).await {
                Ok(_) => (Ok(()), buf),
                Err(e) => (Err(Error::Io(e)), buf),
            }
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

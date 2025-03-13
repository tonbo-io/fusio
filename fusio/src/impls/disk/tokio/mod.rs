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
    file: File,
}

impl TokioFile {
    pub(crate) fn new(file: File) -> Self {
        Self { file }
    }
}

impl AsRef<File> for TokioFile {
    fn as_ref(&self) -> &File {
        &self.file
    }
}

impl AsMut<File> for TokioFile {
    fn as_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

impl Write for TokioFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        (
            AsyncWriteExt::write_all(&mut self.file, buf.as_slice())
                .await
                .map_err(Error::from),
            buf,
        )
    }

    async fn flush(&mut self) -> Result<(), Error> {
        AsyncWriteExt::flush(&mut self.file)
            .await
            .map_err(Error::from)
    }

    async fn close(&mut self) -> Result<(), Error> {
        File::shutdown(&mut self.file).await?;
        Ok(())
    }
}

impl Read for TokioFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            let f = self.file.as_fd();
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
            if let Err(e) = AsyncSeekExt::seek(&mut self.file, SeekFrom::Start(pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            match AsyncReadExt::read_exact(&mut self.file, buf.as_slice_mut()).await {
                Ok(_) => (Ok(()), buf),
                Err(e) => (Err(Error::Io(e)), buf),
            }
        }
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        // TODO: Use pread instead of seek + read_exact
        if let Err(e) = AsyncSeekExt::seek(&mut self.file, SeekFrom::Start(pos)).await {
            return (Err(Error::Io(e)), buf);
        }
        match AsyncReadExt::read_to_end(&mut self.file, &mut buf).await {
            Ok(_) => (Ok(()), buf),
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        Ok(self.file.metadata().await?.len())
    }
}

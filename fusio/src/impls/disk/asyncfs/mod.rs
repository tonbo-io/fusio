use std::io::SeekFrom;

use async_fs::File;
use futures_util::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{error::Error, IoBuf, IoBufMut, Read, Write};

#[cfg(feature = "fs")]
pub mod fs;

pub struct AsyncFile {
    file: Option<File>,
}

impl AsyncFile {
    pub(crate) fn new(file: File) -> Self {
        Self { file: Some(file) }
    }
}

impl AsRef<File> for AsyncFile {
    fn as_ref(&self) -> &File {
        self.file.as_ref().unwrap()
    }
}

impl AsMut<File> for AsyncFile {
    fn as_mut(&mut self) -> &mut File {
        self.file.as_mut().unwrap()
    }
}

impl Write for AsyncFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let file = self.file.as_mut().expect("write file after closed");

        (
            AsyncWriteExt::write_all(file, buf.as_slice())
                .await
                .map_err(Error::from),
            buf,
        )
    }

    async fn flush(&mut self) -> Result<(), Error> {
        AsyncWriteExt::flush(self.file.as_mut().unwrap())
            .await
            .map_err(Error::from)
    }

    async fn close(&mut self) -> Result<(), Error> {
        let file = self.file.as_mut().expect("close file after closed");
        File::close(file).await?;
        Ok(())
    }
}

impl Read for AsyncFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let file = self.file.as_mut().expect("read file after closed");
        // TODO: Use pread instead of seek + read_exact
        if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(pos)).await {
            return (Err(Error::Io(e)), buf);
        }
        match AsyncReadExt::read_exact(file, buf.as_slice_mut()).await {
            Ok(_) => (Ok(()), buf),
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn read_to_end_at(
        &mut self,
        mut buf: std::vec::Vec<u8>,
        pos: u64,
    ) -> (Result<(), Error>, std::vec::Vec<u8>) {
        let file = self.file.as_mut().expect("read file after closed");
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
        self.file
            .as_ref()
            .expect("read file after closed")
            .metadata()
            .await
            .map(|metadata| metadata.len())
            .map_err(Error::from)
    }
}

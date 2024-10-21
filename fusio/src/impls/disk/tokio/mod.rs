#[cfg(feature = "fs")]
pub mod fs;

use std::{io::SeekFrom, ptr::slice_from_raw_parts};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{buf::IoBufMut, Error, IoBuf, Read, Write};

impl Write for File {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        (
            AsyncWriteExt::write_all(self, unsafe {
                &*slice_from_raw_parts(buf.as_ptr(), buf.bytes_init())
            })
            .await
            .map_err(Error::from),
            buf,
        )
    }

    async fn complete(&mut self) -> Result<(), Error> {
        AsyncWriteExt::flush(self).await.map_err(Error::from)?;
        File::shutdown(self).await?;
        Ok(())
    }
}

impl Read for File {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        // TODO: Use pread instead of seek + read_exact
        if let Err(e) = AsyncSeekExt::seek(self, SeekFrom::Start(pos)).await {
            return (Err(Error::Io(e)), buf);
        }
        match AsyncReadExt::read_exact(self, buf.as_slice_mut()).await {
            Ok(_) => (Ok(()), buf),
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        // TODO: Use pread instead of seek + read_exact
        if let Err(e) = AsyncSeekExt::seek(self, SeekFrom::Start(pos)).await {
            return (Err(Error::Io(e)), buf);
        }
        match AsyncReadExt::read_to_end(self, &mut buf).await {
            Ok(_) => (Ok(()), buf),
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn size(&mut self) -> Result<u64, Error> {
        Ok(self.metadata().await?.len())
    }
}

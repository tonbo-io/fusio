#[cfg(feature = "fs")]
pub mod fs;

use std::{io::SeekFrom, ptr::slice_from_raw_parts};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{buf::IoBufMut, Error, IoBuf, Read, Seek, Write};

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

    async fn sync_data(&self) -> Result<(), Error> {
        File::sync_data(self).await?;
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        File::sync_all(self).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        File::shutdown(self).await?;
        Ok(())
    }
}

impl Read for File {
    async fn read_exact<B: IoBufMut>(&mut self, mut buf: B) -> Result<B, Error> {
        AsyncReadExt::read_exact(self, buf.as_slice_mut()).await?;
        Ok(buf)
    }

    async fn read_to_end(&mut self, mut buf: Vec<u8>) -> Result<Vec<u8>, Error> {
        let _ = AsyncReadExt::read_to_end(self, &mut buf).await?;
        Ok(buf)
    }

    async fn size(&self) -> Result<u64, Error> {
        Ok(self.metadata().await?.len())
    }
}

impl Seek for File {
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        AsyncSeekExt::seek(self, SeekFrom::Start(pos)).await?;

        Ok(())
    }
}

#[cfg(feature = "fs")]
pub mod fs;

use std::{io::SeekFrom, ptr::slice_from_raw_parts};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{Error, IoBuf, Read, Seek, Write};

impl Write for File {
    async fn write<B: IoBuf>(&mut self, buf: B) -> (Result<usize, Error>, B) {
        (
            AsyncWriteExt::write(self, unsafe {
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
    async fn read(&mut self, len: Option<u64>) -> Result<impl IoBuf, Error> {
        let mut buf = vec![0; len.unwrap_or(0) as usize];

        AsyncReadExt::read_exact(self, &mut buf).await?;

        #[cfg(not(feature = "bytes"))]
        return Ok(buf);
        #[cfg(feature = "bytes")]
        return Ok(bytes::Bytes::from(buf));
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

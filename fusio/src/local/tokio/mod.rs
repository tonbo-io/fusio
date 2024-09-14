#[cfg(feature = "fs")]
pub mod fs;

use std::{io, ptr::slice_from_raw_parts};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{Error, IoBuf, Read, Write};

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

    async fn sync_data(&mut self) -> Result<(), Error> {
        File::sync_data(self).await?;
        Ok(())
    }

    async fn sync_all(&mut self) -> Result<(), Error> {
        File::sync_all(self).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        AsyncWriteExt::flush(self).await?;

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        File::shutdown(self).await?;
        Ok(())
    }
}

impl Read for File {
    async fn read(&mut self, pos: u64, len: Option<u64>) -> Result<impl IoBuf, Error> {
        self.seek(io::SeekFrom::Start(pos)).await?;

        let mut buf = vec![0; len.unwrap_or(0) as usize];

        AsyncReadExt::read(self, &mut buf).await?;

        #[cfg(not(feature = "bytes"))]
        return Ok(buf);
        #[cfg(feature = "bytes")]
        return Ok(bytes::Bytes::from(buf));
    }
}

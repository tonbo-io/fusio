use std::{io, ptr::slice_from_raw_parts};

use bytes::Bytes;
use monoio::buf::IoBuf;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{BoxFuture, Error, Read, Write};

impl Write for File {
    fn write(&mut self, buf: Bytes, pos: u64) -> BoxFuture<(Result<usize, Error>, Bytes)> {
        Box::pin(async move {
            if let Err(error) = self.seek(io::SeekFrom::Start(pos)).await {
                return (Err(error.into()), buf);
            }
            (
                AsyncWriteExt::write(self, unsafe {
                    &*slice_from_raw_parts(buf.as_ptr(), buf.bytes_init())
                })
                .await
                .map_err(Error::from),
                buf,
            )
        })
    }

    fn sync_data(&self) -> BoxFuture<Result<(), Error>> {
        Box::pin(async {
            File::sync_data(self).await?;
            Ok(())
        })
    }

    fn sync_all(&self) -> BoxFuture<Result<(), Error>> {
        Box::pin(async {
            File::sync_all(self).await?;
            Ok(())
        })
    }

    fn close<'s>(mut self) -> BoxFuture<'s, Result<(), Error>> where Self: 's {
        Box::pin(async move {
            File::shutdown(&mut self).await?;
            Ok(())
        })
    }
}

impl Read for File {
    fn read(&mut self, pos: u64, len: Option<u64>) -> BoxFuture<Result<Bytes, Error>> {
        Box::pin(async move {
            self.seek(io::SeekFrom::Start(pos)).await?;

            let mut buf = vec![0; len.unwrap_or(0) as usize];

            AsyncReadExt::read(self, &mut buf).await?;

            return Ok(bytes::Bytes::from(buf));
        })
    }
}

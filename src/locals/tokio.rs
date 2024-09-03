use std::{
    io,
    ptr::{slice_from_raw_parts, slice_from_raw_parts_mut},
};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{Error, IoBuf, IoBufMut, Read, Write};

impl Write for File {
    async fn write<B: IoBuf>(&mut self, buf: B, pos: u64) -> (Result<usize, Error>, B) {
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
    }

    async fn sync_data(&self) -> Result<(), Error> {
        File::sync_data(self).await?;
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        File::sync_all(self).await?;
        Ok(())
    }

    async fn close(mut self) -> Result<(), Error> {
        File::shutdown(&mut self).await?;
        Ok(())
    }
}

impl Read for File {
    async fn read<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<usize, Error>, B) {
        if let Err(error) = self.seek(io::SeekFrom::Start(pos)).await {
            return (Err(error.into()), buf);
        }
        (
            AsyncReadExt::read(self, unsafe {
                &mut *slice_from_raw_parts_mut(buf.as_mut_ptr(), buf.bytes_init())
            })
            .await
            .map_err(Error::from),
            buf,
        )
    }
}

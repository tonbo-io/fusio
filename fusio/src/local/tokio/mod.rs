#[cfg(feature = "fs")]
pub mod fs;

use std::{io::SeekFrom, ptr::slice_from_raw_parts};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{path::Path, Error, FileMeta, IoBuf, Read, Seek, Write};

pub struct PathFile {
    path: Path,
    file: File,
}

impl PathFile {
    pub fn new(path: Path, file: File) -> Self {
        Self { path, file }
    }
}

impl Write for PathFile {
    async fn write<B: IoBuf>(&mut self, buf: B) -> (Result<usize, Error>, B) {
        (
            AsyncWriteExt::write(&mut self.file, unsafe {
                &*slice_from_raw_parts(buf.as_ptr(), buf.bytes_init())
            })
            .await
            .map_err(Error::from),
            buf,
        )
    }

    async fn sync_data(&self) -> Result<(), Error> {
        File::sync_data(&self.file).await?;
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        File::sync_all(&self.file).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        File::shutdown(&mut self.file).await?;
        Ok(())
    }
}

impl Read for PathFile {
    async fn read(&mut self, len: Option<u64>) -> Result<impl IoBuf, Error> {
        let mut buf = vec![0; len.unwrap_or(0) as usize];

        AsyncReadExt::read(&mut self.file, &mut buf).await?;

        #[cfg(not(feature = "bytes"))]
        return Ok(buf);
        #[cfg(feature = "bytes")]
        return Ok(bytes::Bytes::from(buf));
    }

    async fn metadata(&self) -> Result<FileMeta, Error> {
        Ok(FileMeta {
            path: self.path.clone(),
            size: self.file.metadata().await?.len(),
        })
    }
}

impl Seek for PathFile {
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        AsyncSeekExt::seek(&mut self.file, SeekFrom::Start(pos)).await?;

        Ok(())
    }
}

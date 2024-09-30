#[cfg(feature = "fs")]
pub mod fs;

use std::future::Future;

use tokio_uring::fs::File;

use crate::{Error, IoBuf, IoBufMut, MaybeSend, Read, Seek, Write};

#[repr(transparent)]
struct TokioUringBuf<B> {
    buf: B,
}

unsafe impl<B> tokio_uring::buf::IoBuf for TokioUringBuf<B>
where
    B: IoBuf,
{
    fn stable_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.buf.bytes_init()
    }

    fn bytes_total(&self) -> usize {
        self.buf.bytes_init()
    }
}

unsafe impl<B> tokio_uring::buf::IoBufMut for TokioUringBuf<B>
where
    B: IoBufMut,
{
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, _pos: usize) {}
}

pub struct TokioUringFile {
    file: Option<File>,
    pos: u64,
}

impl From<File> for TokioUringFile {
    fn from(file: File) -> Self {
        Self {
            file: Some(file),
            pos: 0,
        }
    }
}

impl Write for TokioUringFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .write_all_at(TokioUringBuf { buf }, self.pos)
            .await;
        self.pos += buf.buf.bytes_init() as u64;
        (result.map_err(Error::from), buf.buf)
    }

    async fn sync_data(&self) -> Result<(), Error> {
        File::sync_data(self.file.as_ref().expect("read file after closed")).await?;
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        File::sync_all(self.file.as_ref().expect("read file after closed")).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        File::close(self.file.take().expect("close file twice")).await?;
        Ok(())
    }
}

impl Read for TokioUringFile {
    async fn read_exact<B: IoBufMut>(&mut self, buf: B) -> Result<B, Error> {
        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .read_exact_at(TokioUringBuf { buf }, self.pos)
            .await;
        result?;
        self.pos += buf.buf.bytes_init() as u64;

        Ok(buf.buf)
    }

    async fn read_to_end(&mut self, mut buf: Vec<u8>) -> Result<Vec<u8>, Error> {
        buf.resize((self.size().await? - self.pos) as usize, 0);

        Ok(self.read_exact(buf).await?)
    }

    async fn size(&self) -> Result<u64, Error> {
        let stat = self
            .file
            .as_ref()
            .expect("read file after closed")
            .statx()
            .await?;
        Ok(stat.stx_size)
    }
}

impl Seek for TokioUringFile {
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        self.pos = pos;

        Ok(())
    }
}

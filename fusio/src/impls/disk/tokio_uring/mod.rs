#[cfg(feature = "fs")]
pub mod fs;

#[allow(unused)]
#[cfg(feature = "fs")]
pub use fs::TokioUringFs;
use tokio_uring::fs::File;

use crate::{Error, IoBuf, IoBufMut, Read, Write};

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

    async fn complete(&mut self) -> Result<(), Error> {
        File::close(self.file.take().expect("close file twice")).await?;
        Ok(())
    }
}

impl Read for TokioUringFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .read_exact_at(TokioUringBuf { buf }, pos)
            .await;

        (result.map_err(Error::from), buf.buf)
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        match self.size().await {
            Ok(size) => {
                buf.resize((size - pos) as usize, 0);
            }
            Err(e) => return (Err(e), buf),
        }

        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .read_exact_at(TokioUringBuf { buf }, pos)
            .await;

        match result {
            Ok(_) => (Ok(()), buf.buf),
            Err(e) => (Err(Error::from(e)), buf.buf),
        }
    }

    async fn size(&mut self) -> Result<u64, Error> {
        let stat = self
            .file
            .as_ref()
            .expect("read file after closed")
            .statx()
            .await?;
        Ok(stat.stx_size)
    }
}

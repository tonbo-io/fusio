#[cfg(feature = "fs")]
pub mod fs;

#[allow(unused)]
#[cfg(feature = "fs")]
pub use fs::TokioUringFs;
use tokio_uring::fs::File;

use crate::{Error, IoBuf, IoBufMut, Read, Seek, Write};

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
    async fn read<B: IoBufMut>(&mut self, buf: B) -> (Result<u64, Error>, B) {
        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .read_at(TokioUringBuf { buf }, self.pos)
            .await;

        match result {
            Ok(result) => {
                self.pos += result as u64;
                (Ok(result as u64), buf.buf)
            }
            Err(err) => (Err(Error::from(err)), buf.buf),
        }
    }

    async fn read_to_end(&mut self, mut buf: Vec<u8>) -> (Result<(), Error>, Vec<u8>) {
        match self.size().await {
            Ok(size) => buf.resize((size - self.pos) as usize, 0),
            Err(err) => return (Err(err), buf),
        }

        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .read_exact_at(TokioUringBuf { buf }, self.pos)
            .await;

        match result {
            Ok(()) => {
                self.pos += buf.buf.len() as u64;
                (Ok(()), buf.buf)
            }
            Err(err) => (Err(Error::from(err)), buf.buf),
        }
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

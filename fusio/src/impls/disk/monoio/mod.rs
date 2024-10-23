#[cfg(feature = "fs")]
pub mod fs;

use monoio::fs::File;

use crate::{buf::IoBufMut, Error, IoBuf, Read, Write};

#[repr(transparent)]
struct MonoioBuf<B> {
    buf: B,
}

unsafe impl<B> monoio::buf::IoBuf for MonoioBuf<B>
where
    B: IoBuf,
{
    fn read_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.buf.bytes_init()
    }
}

unsafe impl<B> monoio::buf::IoBufMut for MonoioBuf<B>
where
    B: IoBufMut,
{
    fn write_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    fn bytes_total(&mut self) -> usize {
        self.buf.bytes_init()
    }

    unsafe fn set_init(&mut self, _pos: usize) {}
}

pub struct MonoioFile {
    file: Option<File>,
    pos: u64,
}

impl From<File> for MonoioFile {
    fn from(file: File) -> Self {
        Self {
            file: Some(file),
            pos: 0,
        }
    }
}

impl Write for MonoioFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .write_all_at(MonoioBuf { buf }, self.pos)
            .await;
        self.pos += buf.buf.bytes_init() as u64;
        (result.map_err(Error::from), buf.buf)
    }

    async fn flush(&mut self) -> Result<(), Error> {
        File::sync_all(self.file.as_ref().expect("read file after closed")).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        File::close(self.file.take().expect("close file twice")).await?;
        Ok(())
    }
}

impl Read for MonoioFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .read_exact_at(MonoioBuf { buf }, pos)
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
            .read_exact_at(MonoioBuf { buf }, pos)
            .await;

        match result {
            Ok(_) => (Ok(()), buf.buf),
            Err(e) => (Err(Error::from(e)), buf.buf),
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        let metadata = File::metadata(self.file.as_ref().expect("read file after closed")).await?;
        Ok(metadata.len())
    }
}

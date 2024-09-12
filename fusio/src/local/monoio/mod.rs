#[cfg(feature = "fs")]
pub mod fs;

use monoio::fs::File;

use crate::{Error, IoBuf, IoBufMut, Read, Write};

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
        IoBufMut::bytes_total(&self.buf)
    }

    unsafe fn set_init(&mut self, pos: usize) {
        IoBufMut::set_init(&mut self.buf, pos)
    }
}

pub struct MonoioFile {
    file: Option<File>,
}

impl From<File> for MonoioFile {
    fn from(file: File) -> Self {
        Self { file: Some(file) }
    }
}

impl Write for MonoioFile {
    async fn write<B: IoBuf>(&mut self, buf: B, pos: u64) -> (Result<usize, Error>, B) {
        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .write_at(MonoioBuf { buf }, pos)
            .await;
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

impl Read for MonoioFile {
    async fn read(&mut self, pos: u64, len: Option<u64>) -> Result<impl IoBuf, Error> {
        let buf = vec![0; len.unwrap_or(0) as usize];

        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .read_at(MonoioBuf { buf }, pos)
            .await;
        result?;

        #[cfg(not(feature = "bytes"))]
        return Ok(buf.buf);
        #[cfg(feature = "bytes")]
        return Ok(bytes::Bytes::from(buf.buf));
    }
}

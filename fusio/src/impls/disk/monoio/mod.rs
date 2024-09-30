#[cfg(feature = "fs")]
pub mod fs;

use monoio::fs::File;

use crate::{buf::IoBufMut, Error, IoBuf, Read, Seek, Write};

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
    async fn read_exact<B: IoBufMut>(&mut self, buf: B) -> Result<B, Error> {
        let (result, buf) = self
            .file
            .as_ref()
            .expect("read file after closed")
            .read_exact_at(MonoioBuf { buf }, self.pos)
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
        let metadata = File::metadata(self.file.as_ref().expect("read file after closed")).await?;
        Ok(metadata.len())
    }
}

impl Seek for MonoioFile {
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        self.pos = pos;

        Ok(())
    }
}

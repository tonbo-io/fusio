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

    unsafe fn set_init(&mut self, pos: usize) {
        IoBufMut::set_init(&mut self.buf, pos)
    }
}

impl Write for File {
    async fn write<B: IoBuf>(&mut self, buf: B, pos: u64) -> (Result<usize, Error>, B) {
        let (result, buf) = self.write_at(TokioUringBuf { buf }, pos).submit().await;
        (result.map_err(Error::from), buf.buf)
    }

    async fn sync_data(&self) -> Result<(), Error> {
        File::sync_data(self).await?;
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        File::sync_all(self).await?;
        Ok(())
    }

    async fn close(self) -> Result<(), Error> {
        File::close(self).await?;
        Ok(())
    }
}

impl Read for File {
    async fn read(&mut self, pos: u64, len: Option<u64>) -> Result<impl IoBuf, Error> {
        let buf = Vec::with_capacity(len.unwrap_or(0) as usize);

        let (result, buf) = self.read_at(TokioUringBuf { buf }, pos).await;
        result?;

        #[cfg(not(feature = "bytes"))]
        return Ok(buf.buf);
        #[cfg(feature = "bytes")]
        return Ok(bytes::Bytes::from(buf.buf));
    }
}

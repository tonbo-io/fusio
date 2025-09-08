use std::cmp;

use fusio_core::Write;

use crate::{
    durability::{FileCommit, FileSync, SupportsDurability},
    error::Error,
    IoBuf, IoBufMut, Read,
};

pub struct BufReader<F> {
    inner: F,
    capacity: usize,
    buf: Option<(Vec<u8>, usize)>,
    size: u64,
    offset: usize, // the offset of first bytes in buf

    #[cfg(test)]
    filling_count: usize,
}

impl<F: Read> BufReader<F> {
    pub async fn new(inner: F, capacity: usize) -> Result<Self, Error> {
        let size = inner.size().await?;

        Ok(Self {
            inner,
            capacity,
            buf: None,
            offset: 0,
            size,
            #[cfg(test)]
            filling_count: 0,
        })
    }
}

impl<F: Read> Read for BufReader<F> {
    async fn read_exact_at<B: IoBufMut>(
        &mut self,
        mut buf: B,
        mut pos: u64,
    ) -> (Result<(), Error>, B) {
        let mut write_pos = 0;
        let buf_slice = buf.as_slice_mut();

        while write_pos < buf_slice.len() {
            if let Err(err) = self.filling_buf(pos).await {
                return (Err(err), buf);
            }
            let (fill_buf, read_pos) = self.buf.as_mut().unwrap();

            let min_len = cmp::min(fill_buf.len() - *read_pos, buf_slice.len() - write_pos);
            let read_end = min_len + *read_pos;
            let write_end = min_len + write_pos;
            buf_slice[write_pos..write_end].copy_from_slice(&fill_buf[*read_pos..read_end]);

            *read_pos += min_len;
            write_pos += min_len;
            pos += min_len as u64;
        }
        (Ok(()), buf)
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        buf.resize((self.size - pos) as usize, 0u8);

        self.read_exact_at(buf, pos).await
    }

    async fn size(&self) -> Result<u64, Error> {
        Ok(self.size)
    }
}

impl<F: Read> BufReader<F> {
    async fn filling_buf(&mut self, pos: u64) -> Result<(), Error> {
        if self.size <= pos {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read unexpected eof",
            )));
        }
        let start = self.offset;
        if self
            .buf
            .as_ref()
            .map(|(buf, current)| buf.len() == *current || start + *current != pos as usize)
            .unwrap_or(true)
        {
            let fill_buf = vec![0u8; cmp::min(self.capacity, (self.size - pos) as usize)];
            let (result, fill_buf) = self.inner.read_exact_at(fill_buf, pos).await;
            if result.is_ok() {
                self.buf = Some((fill_buf, 0));
                self.offset = pos as usize;
            }
            #[cfg(test)]
            {
                self.filling_count += 1;
            }
            result
        } else {
            Ok(())
        }
    }
}

pub struct BufWriter<F> {
    inner: F,
    buf: Option<Vec<u8>>,
}

impl<F> BufWriter<F> {
    pub fn new(file: F, capacity: usize) -> Self {
        Self {
            inner: file,
            buf: Some(Vec::with_capacity(capacity)),
        }
    }
}

impl<F: Write> Write for BufWriter<F> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (len, capacity) = {
            let buf = self.buf.as_ref().expect("no buffer available");
            (buf.len(), buf.capacity())
        };

        let written_size = buf.bytes_init();
        if len + written_size > capacity {
            let result = self.flush().await;
            if result.is_err() {
                return (result, buf);
            }
        }

        // Now there are tow situations here:
        // 1. There is no enough space to hold data, which means buffer is empty and written size >
        //    capacity
        // 2. Data can be written to buffer
        if len + written_size > capacity {
            self.inner.write_all(buf).await
        } else {
            let owned_buf = self.buf.as_mut().unwrap();
            owned_buf.extend_from_slice(buf.as_slice());
            (Ok(()), buf)
        }
    }

    /// Flush buffer to file
    async fn flush(&mut self) -> Result<(), Error> {
        let data = self.buf.take().expect("no buffer available");
        let (result, mut data) = self.inner.write_all(data).await;
        result?;

        data.clear();
        self.buf = Some(data);
        self.inner.flush().await?;

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.flush().await?;
        self.inner.close().await?;

        Ok(())
    }
}

impl<F: Write + FileSync> FileSync for BufWriter<F> {
    async fn sync_data(&mut self) -> Result<(), Error> {
        self.flush().await?;
        self.inner.sync_data().await
    }

    async fn sync_all(&mut self) -> Result<(), Error> {
        self.flush().await?;
        self.inner.sync_all().await
    }

    async fn sync_range(&mut self, offset: u64, len: u64) -> Result<(), Error> {
        self.flush().await?;
        self.inner.sync_range(offset, len).await
    }
}
impl<F: Write + FileCommit> FileCommit for BufWriter<F> {
    async fn commit(&mut self) -> Result<(), Error> {
        self.flush().await?;
        self.inner.commit().await
    }
}

impl FileSync for BufWriter<Box<dyn crate::dynamic::fs::DynFile>> {
    async fn sync_data(&mut self) -> Result<(), Error> {
        self.flush().await?;
        // Attempt downcast to known concrete file types and delegate.
        let any = (&mut *self.inner) as &mut dyn core::any::Any;
        #[cfg(feature = "tokio")]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::tokio::TokioFile>() {
            return f.sync_data().await;
        }
        #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::tokio_uring::TokioUringFile>() {
            return f.sync_data().await;
        }
        #[cfg(all(feature = "opfs", target_arch = "wasm32", feature = "sync"))]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::OPFSSyncFile>() {
            return f.sync_data().await;
        }
        #[cfg(feature = "monoio")]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::monoio::MonoioFile>() {
            return f.sync_data().await;
        }
        #[cfg(feature = "aws")]
        if let Some(f) = any.downcast_mut::<crate::impls::remotes::aws::s3::S3File>() {
            return f.sync_data().await;
        }
        Err(Error::Unsupported {
            message: "Durable sync not supported for this backend".to_string(),
        })
    }

    async fn sync_all(&mut self) -> Result<(), Error> {
        self.flush().await?;
        let any = (&mut *self.inner) as &mut dyn core::any::Any;
        #[cfg(feature = "tokio")]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::tokio::TokioFile>() {
            return f.sync_all().await;
        }
        #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::tokio_uring::TokioUringFile>() {
            return f.sync_all().await;
        }
        #[cfg(all(feature = "opfs", target_arch = "wasm32", feature = "sync"))]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::OPFSSyncFile>() {
            return f.sync_all().await;
        }
        #[cfg(feature = "monoio")]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::monoio::MonoioFile>() {
            return f.sync_all().await;
        }
        #[cfg(feature = "aws")]
        if let Some(f) = any.downcast_mut::<crate::impls::remotes::aws::s3::S3File>() {
            return f.sync_all().await;
        }
        Err(Error::Unsupported {
            message: "Durable sync not supported for this backend".to_string(),
        })
    }

    async fn sync_range(&mut self, offset: u64, len: u64) -> Result<(), Error> {
        self.flush().await?;
        let any = (&mut *self.inner) as &mut dyn core::any::Any;
        #[cfg(feature = "tokio")]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::tokio::TokioFile>() {
            return f.sync_range(offset, len).await;
        }
        #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::tokio_uring::TokioUringFile>() {
            return f.sync_range(offset, len).await;
        }
        #[cfg(all(feature = "opfs", target_arch = "wasm32", feature = "sync"))]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::OPFSSyncFile>() {
            return f.sync_range(offset, len).await;
        }
        #[cfg(feature = "monoio")]
        if let Some(f) = any.downcast_mut::<crate::impls::disk::monoio::MonoioFile>() {
            return f.sync_range(offset, len).await;
        }
        #[cfg(feature = "aws")]
        if let Some(f) = any.downcast_mut::<crate::impls::remotes::aws::s3::S3File>() {
            return f.sync_range(offset, len).await;
        }
        Err(Error::Unsupported {
            message: "Durable sync not supported for this backend".to_string(),
        })
    }
}
// Specialized dynamic BufWriter no longer needed; covered by generic impl above

// Advertise durability support by delegating to the inner handle when possible.
impl<F> SupportsDurability for BufWriter<F>
where
    F: SupportsDurability,
{
    fn supports(&self, op: fusio_core::DurabilityOp) -> bool {
        self.inner.supports(op)
    }

    fn capabilities(&self) -> &'static [fusio_core::Capability] {
        self.inner.capabilities()
    }
}

#[cfg(feature = "tokio")]
#[cfg(test)]
pub(crate) mod tests {

    use super::BufWriter;
    use crate::{buffered::BufReader, error::Error, IoBufMut, Read, Write};

    impl<F: Read> Read for BufWriter<F> {
        async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
            self.inner.read_exact_at(buf, pos).await
        }

        async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
            self.inner.read_to_end_at(buf, pos).await
        }

        async fn size(&self) -> Result<u64, Error> {
            let size = self.inner.size().await?;
            Ok(size)
        }
    }

    #[cfg(all(feature = "tokio", not(feature = "completion-based")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_buf_read() {
        use tempfile::tempfile;

        use crate::disk::tokio::TokioFile;

        let mut file = TokioFile::new(tokio::fs::File::from_std(tempfile().unwrap()));
        let _ = file
            .write_all([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15].as_slice())
            .await;

        let mut reader = BufReader::new(file, 8).await.unwrap();
        {
            let (result, buf) = reader.read_exact_at(vec![0u8; 4], 0).await;
            result.unwrap();
            assert_eq!(buf, vec![0, 1, 2, 3]);

            let (result, buf) = reader.read_exact_at(vec![0u8; 4], 4).await;
            result.unwrap();
            assert_eq!(buf, vec![4, 5, 6, 7]);

            assert_eq!(reader.filling_count, 1);
        }
        {
            let (result, buf) = reader.read_exact_at(vec![0u8; 4], 0).await;
            result.unwrap();
            assert_eq!(buf, vec![0, 1, 2, 3]);

            let (result, buf) = reader.read_exact_at(vec![0u8; 4], 8).await;
            result.unwrap();
            assert_eq!(buf, vec![8, 9, 10, 11]);

            assert_eq!(reader.filling_count, 3);
        }
        {
            let (result, buf) = reader.read_exact_at(vec![0u8; 12], 0).await;
            result.unwrap();
            assert_eq!(buf, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);

            assert_eq!(reader.filling_count, 5);
        }
        {
            let (result, buf) = reader.read_to_end_at(Vec::new(), 1).await;
            result.unwrap();
            assert_eq!(buf, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);

            assert_eq!(reader.filling_count, 7);
        }
    }

    #[cfg(all(feature = "tokio", not(feature = "completion-based")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_buf_read_write() {
        use tempfile::tempfile;

        use crate::{
            impls::{buffered::BufWriter, disk::tokio::TokioFile},
            Read, Write,
        };

        let file = TokioFile::new(tokio::fs::File::from_std(tempfile().unwrap()));
        let mut writer = BufWriter::new(file, 4);
        {
            let _ = writer.write_all("Hello".as_bytes()).await;
            let buf = vec![];
            let (_, buf) = writer.read_to_end_at(buf, 0).await;
            assert_eq!(String::from_utf8(buf).unwrap(), "Hello".to_owned());
        }
        {
            let _ = writer.write_all(" ".as_bytes()).await;
            // " " should not be flushed
            let (_, buf) = writer.read_to_end_at(vec![], 0).await;
            assert_eq!(String::from_utf8(buf).unwrap(), "Hello".to_owned());
        }
        {
            let _ = writer.write_all("fusio".as_bytes()).await;
            let (_, buf) = writer.read_to_end_at(vec![], 0).await;
            assert_eq!(String::from_utf8(buf).unwrap(), "Hello fusio".to_owned());
        }
        {
            // test flush
            let _ = writer.write_all("!".as_bytes()).await;
            writer.flush().await.unwrap();
            let (_, buf) = writer.read_to_end_at(vec![], 0).await;
            assert_eq!(String::from_utf8(buf).unwrap(), "Hello fusio!".to_owned());
        }
        {
            // test read_exact_at
            let mut buf = [0; 5];
            let (_, buf) = writer.read_exact_at(buf.as_mut(), 2).await;
            assert_eq!(String::from_utf8_lossy(buf).to_string(), "llo f".to_owned());

            let mut buf = [0; 5];
            let (result, _) = writer.read_exact_at(buf.as_mut(), 8).await;
            assert!(result.is_err());
        }
    }

    #[cfg(all(feature = "tokio", not(feature = "completion-based")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_buf_read_eof() {
        use tempfile::tempfile;

        use crate::disk::tokio::TokioFile;

        let mut file = TokioFile::new(tokio::fs::File::from_std(tempfile().unwrap()));
        let _ = file.write_all([0, 1, 2].as_slice()).await;

        let mut reader = BufReader::new(file, 8).await.unwrap();
        {
            let (result, buf) = reader.read_exact_at(vec![0u8; 1], 0).await;
            result.unwrap();
            assert_eq!(buf, vec![0]);

            let (result, _) = reader.read_exact_at(vec![0u8; 4], 4).await;
            assert!(result.is_err());
        }
        {
            let (result, buf) = reader.read_exact_at(vec![0u8; 3], 0).await;
            result.unwrap();
            assert_eq!(buf, vec![0, 1, 2]);
        }
        {
            let (result, _) = reader.read_exact_at(vec![0u8; 4], 0).await;
            assert!(result.is_err());
        }
    }
}

use crate::{Error, IoBuf, IoBufMut, Read, Write};

pub struct BufWriter<F> {
    inner: F,
    buf: Option<Vec<u8>>,
    capacity: usize,
    pos: usize,
}

impl<F> BufWriter<F> {
    pub fn new(file: F, capacity: usize) -> Self {
        Self {
            inner: file,
            buf: Some(Vec::with_capacity(capacity)),
            capacity,
            pos: 0,
        }
    }
}

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

impl<F: Write> Write for BufWriter<F> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let written_size = buf.bytes_init();
        if self.pos + written_size > self.capacity {
            let result = self.flush().await;
            if result.is_err() {
                return (result, buf);
            }
        }

        // Now there are tow situations here:
        // 1. There is no enough space to hold data, which means buffer is empty and written size >
        //    capacity
        // 2. Data can be written to buffer
        if self.pos + written_size > self.capacity {
            self.inner.write_all(buf).await
        } else {
            let owned_buf = self.buf.as_mut().unwrap();
            owned_buf.extend_from_slice(buf.as_slice());
            self.pos += written_size;
            (Ok(()), buf)
        }
    }

    /// Flush buffer to file
    async fn flush(&mut self) -> Result<(), Error> {
        let data = self.buf.take().expect("no buffer available");
        let (result, mut data) = self.inner.write_all(data).await;
        result?;

        data.drain(..self.pos);
        self.buf = Some(data);
        self.pos = 0;
        self.inner.flush().await?;

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.flush().await?;
        self.inner.close().await?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {

    #[cfg(all(feature = "tokio", not(feature = "completion-based")))]
    #[tokio::test]
    async fn test_buf_read_write() {
        use tempfile::tempfile;

        use crate::{impls::buffered::BufWriter, Read, Write};

        let file = tokio::fs::File::from_std(tempfile().unwrap());
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
}

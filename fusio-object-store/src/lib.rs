pub mod fs;

use std::{ops::Range, sync::Arc};

use fusio::{Error, IoBuf, IoBufMut, Read, Write};
use object_store::{buffered::BufWriter, path::Path, GetOptions, GetRange, ObjectStore};
use parquet::arrow::async_writer::{AsyncFileWriter, ParquetObjectWriter};

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct S3File<O: ObjectStore> {
    inner: Arc<O>,
    path: Path,
    buf: Option<ParquetObjectWriter>,
}

impl<O: ObjectStore> S3File<O> {
    async fn read_with_range<B: IoBufMut>(
        &mut self,
        range: GetRange,
        mut buf: B,
    ) -> (Result<(), Error>, B) {
        let opts = GetOptions {
            range: Some(range),
            ..Default::default()
        };
        let result = match self
            .inner
            .get_opts(&self.path, opts)
            .await
            .map_err(BoxedError::from)
        {
            Ok(result) => result,
            Err(e) => return (Err(e.into()), buf),
        };

        let bytes = match result.bytes().await.map_err(BoxedError::from) {
            Ok(bytes) => bytes,
            Err(e) => return (Err(e.into()), buf),
        };

        buf.as_slice_mut().copy_from_slice(&bytes);
        (Ok(()), buf)
    }
}

impl<O: ObjectStore> Read for S3File<O> {
    async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let range = GetRange::Bounded(Range {
            start: pos as usize,
            end: pos as usize + buf.bytes_init(),
        });

        self.read_with_range(range, buf).await
    }

    async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let range = GetRange::Offset(pos as usize);

        let (result, buf) = self.read_with_range(range, buf).await;
        match result {
            Ok(_) => (Ok(()), buf),
            Err(e) => (Err(e), buf),
        }
    }

    async fn size(&mut self) -> Result<u64, Error> {
        let options = GetOptions {
            head: true,
            ..Default::default()
        };
        let response = self
            .inner
            .get_opts(&self.path, options)
            .await
            .map_err(BoxedError::from)?;
        Ok(response.meta.size as u64)
    }
}

impl<O: ObjectStore> Write for S3File<O> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let buf_writer = match self.buf {
            Some(ref mut buf) => buf,
            None => {
                self.buf = Some(BufWriter::new(self.inner.clone(), self.path.clone()).into());
                self.buf.as_mut().unwrap()
            }
        };
        if let Err(e) = buf_writer.write(buf.as_bytes()).await {
            return (Err(Error::Other(e.into())), buf);
        }

        (Ok(()), buf)
    }

    async fn complete(&mut self) -> Result<(), Error> {
        if let Some(mut buf) = self.buf.take() {
            buf.complete().await.map_err(|e| Error::Other(e.into()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_s3() {
        use std::{env, env::VarError, sync::Arc};

        use bytes::Bytes;
        use object_store::{aws::AmazonS3Builder, ObjectStore};

        use crate::{Read, S3File, Write};

        let fn_env = || {
            let region = env::var("TEST_INTEGRATION")?;
            let bucket_name = env::var("TEST_INTEGRATION")?;
            let access_key_id = env::var("TEST_INTEGRATION")?;
            let secret_access_key = env::var("TEST_INTEGRATION")?;

            Ok::<(String, String, String, String), VarError>((
                region,
                bucket_name,
                access_key_id,
                secret_access_key,
            ))
        };
        if let Ok((region, bucket_name, access_key_id, secret_access_key)) = fn_env() {
            let path = object_store::path::Path::parse("/test_file").unwrap();
            let s3 = AmazonS3Builder::new()
                .with_region(region)
                .with_bucket_name(bucket_name)
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key)
                .build()
                .unwrap();
            let _ = s3.delete(&path).await;

            let mut store = S3File {
                inner: Arc::new(s3),
                path,
                buf: None,
            };
            let (result, bytes) = store.write_all(Bytes::from("hello! Fusio!")).await;
            result.unwrap();

            let mut buf = vec![0_u8; bytes.len()];
            let (result, buf) = store.read_exact_at(&mut buf[..], 0).await;
            result.unwrap();
            assert_eq!(buf, &bytes[..]);
        }
    }
}

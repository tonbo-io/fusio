mod fs;

use std::{ops::Range, sync::Arc};

use object_store::{
    aws::AmazonS3, buffered::BufWriter, path::Path, GetOptions, GetRange, ObjectStore,
};
use tokio::io::AsyncWriteExt;

use crate::{Error, IoBuf, Read, Write};

pub struct S3File {
    inner: Arc<AmazonS3>,
    path: Path,
}

pub struct S3FileWriter {
    inner: BufWriter,
}

impl S3File {
    pub fn writer(&self) -> S3FileWriter {
        S3FileWriter {
            inner: BufWriter::new(self.inner.clone(), self.path.clone().into()),
        }
    }
}

impl Read for S3File {
    async fn read(&mut self, pos: u64, len: Option<u64>) -> Result<impl IoBuf, Error> {
        let pos = pos as usize;

        let mut opts = GetOptions::default();

        let range = if let Some(len) = len {
            GetRange::Bounded(Range {
                start: pos,
                end: pos + len as usize,
            })
        } else {
            GetRange::Offset(pos)
        };
        opts.range = Some(range);
        let result = self.inner.get_opts(&self.path, opts).await?;
        let bytes = result.bytes().await?;

        Ok(bytes)
    }
}

impl Write for S3FileWriter {
    async fn write<B: IoBuf>(&mut self, buf: B) -> (Result<usize, Error>, B) {
        let result = self.inner.write(buf.as_slice()).await.map_err(Error::from);

        (result, buf)
    }

    async fn sync_data(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn sync_all(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        self.inner.flush().await?;

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.inner.shutdown().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{env, env::VarError, sync::Arc};

    use bytes::Bytes;
    use object_store::{aws::AmazonS3Builder, ObjectStore};

    use crate::{buf::IoBuf, remotes::object_store::S3File, Read, Write};

    #[tokio::test]
    async fn test_s3() {
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
            };
            let mut writer = store.writer();
            let (result, bytes) = writer.write(Bytes::from("hello! Fusio!")).await;
            assert_eq!(result.unwrap(), bytes.len());
            assert_eq!(bytes, Bytes::from("hello! Fusio!"));
            writer.flush().await.unwrap();
            writer.close().await.unwrap();

            let buf = store.read(0, None).await.unwrap();
            assert_eq!(buf.as_bytes(), bytes);
            drop(buf);
            let buf = store.read(0, Some(6)).await.unwrap();
            assert_eq!(buf.as_bytes(), Bytes::from("hello!"));
            drop(buf);
            let buf = store.read(7, Some(6)).await.unwrap();
            assert_eq!(buf.as_bytes(), Bytes::from("Fusio!"));
            drop(buf);
        }
    }
}

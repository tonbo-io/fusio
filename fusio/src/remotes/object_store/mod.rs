mod fs;

use std::{ops::Range, sync::Arc};

use object_store::{aws::AmazonS3, path::Path, GetOptions, GetRange, ObjectStore, PutPayload};

use crate::{Error, FileMeta, IoBuf, Read, Seek, Write};

pub struct S3File {
    inner: Arc<AmazonS3>,
    path: Path,
    pos: u64,
}

impl Read for S3File {
    async fn read(&mut self, len: Option<u64>) -> Result<impl IoBuf, Error> {
        let pos = self.pos as usize;

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

        self.pos += bytes.len() as u64;
        Ok(bytes)
    }

    async fn metadata(&self) -> Result<FileMeta, Error> {
        let mut options = GetOptions::default();
        options.head = true;
        let response = self.inner.get_opts(&self.path, options).await?;
        Ok(FileMeta {
            path: self.path.clone().into(),
            size: response.meta.size as u64,
        })
    }
}

impl Seek for S3File {
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        self.pos = pos;
        Ok(())
    }
}

impl Write for S3File {
    async fn write<B: IoBuf>(&mut self, buf: B) -> (Result<usize, Error>, B) {
        let payload = PutPayload::from_bytes(buf.as_bytes());
        let result = self
            .inner
            .put(&self.path, payload)
            .await
            .map(|_| buf.as_slice().len())
            .map_err(Error::ObjectStore);

        (result, buf)
    }

    async fn sync_data(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
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
                pos: 0,
            };
            let (result, bytes) = store.write(Bytes::from("hello! Fusio!")).await;
            assert_eq!(result.unwrap(), bytes.len());
            assert_eq!(bytes, Bytes::from("hello! Fusio!"));

            let buf = store.read(None).await.unwrap();
            assert_eq!(buf.as_bytes(), bytes);
            drop(buf);
            let buf = store.read(Some(6)).await.unwrap();
            assert_eq!(buf.as_bytes(), Bytes::from("hello!"));
            drop(buf);
            let buf = store.read(Some(6)).await.unwrap();
            assert_eq!(buf.as_bytes(), Bytes::from("Fusio!"));
            drop(buf);
        }
    }
}

pub mod fs;

use std::{ops::Range, sync::Arc};

use fusio::{Error, IoBuf, IoBufMut, Read, Seek, Write};
use object_store::{path::Path, GetOptions, GetRange, ObjectStore, PutPayload};

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct S3File<O: ObjectStore> {
    inner: Arc<O>,
    path: Path,
    pos: u64,
}

impl<O: ObjectStore> S3File<O> {
    async fn read_with_range<B: IoBufMut>(
        &mut self,
        range: GetRange,
        mut buf: B,
    ) -> (Result<u64, Error>, B) {
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

        buf.set_init(bytes.len());

        buf.as_slice_mut().copy_from_slice(&bytes);
        (Ok(bytes.len() as u64), buf)
    }
}

impl<O: ObjectStore> Read for S3File<O> {
    async fn read<B: IoBufMut>(&mut self, buf: B) -> (Result<u64, Error>, B) {
        let pos = self.pos as usize;

        let range = GetRange::Bounded(Range {
            start: pos,
            end: pos + buf.bytes_init(),
        });

        self.read_with_range(range, buf).await
    }

    async fn read_to_end(&mut self, buf: Vec<u8>) -> (Result<(), Error>, Vec<u8>) {
        let pos = self.pos as usize;
        let range = GetRange::Offset(pos);

        let (result, buf) = self.read_with_range(range, buf).await;
        match result {
            Ok(size) => {
                self.pos += size;
                (Ok(()), buf)
            }
            Err(e) => (Err(e), buf),
        }
    }

    async fn size(&self) -> Result<u64, Error> {
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

impl<O: ObjectStore> Seek for S3File<O> {
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        self.pos = pos;
        Ok(())
    }
}

impl<O: ObjectStore> Write for S3File<O> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let result = self
            .inner
            .put(&self.path, PutPayload::from_bytes(buf.as_bytes()))
            .await
            .map(|_| ())
            .map_err(|e| BoxedError::from(e).into());

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
                pos: 0,
            };
            let (result, bytes) = store.write_all(Bytes::from("hello! Fusio!")).await;
            result.unwrap();

            let mut buf = vec![0_u8; bytes.len()];
            let (result, buf) = store.read(&mut buf[..]).await;
            result.unwrap();
            assert_eq!(buf, &bytes[..]);
        }
    }
}

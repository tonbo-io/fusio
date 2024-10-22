use std::{mem, pin::Pin, sync::Arc};

use bytes::{BufMut, BytesMut};
use futures_util::{stream::FuturesUnordered, StreamExt};
use http_body_util::Full;

use crate::{
    dynamic::MaybeSendFuture,
    remotes::{aws::multipart_upload::MultipartUpload, serde::MultipartPart},
    Error, IoBuf, Write,
};

const S3_PART_MINIMUM_SIZE: usize = 5 * 1024 * 1024;

pub struct S3Writer {
    inner: Arc<MultipartUpload>,
    upload_id: Option<Arc<String>>,
    next_part_numer: usize,
    buf: BytesMut,

    handlers:
        FuturesUnordered<Pin<Box<dyn MaybeSendFuture<Output = Result<MultipartPart, Error>>>>>,
}

impl S3Writer {
    pub fn new(inner: Arc<MultipartUpload>) -> Self {
        Self {
            inner,
            upload_id: None,
            next_part_numer: 0,
            buf: BytesMut::with_capacity(S3_PART_MINIMUM_SIZE),
            handlers: FuturesUnordered::new(),
        }
    }

    async fn upload_part<F>(&mut self, fn_bytes_init: F) -> Result<(), Error>
    where
        F: FnOnce() -> BytesMut,
    {
        let upload_id = match self.upload_id.clone() {
            None => {
                let upload_id = Arc::new(self.inner.initiate().await?);
                self.upload_id = Some(upload_id.clone());
                upload_id
            }
            Some(upload_id) => upload_id,
        };
        let part_num = self.next_part_numer;
        self.next_part_numer += 1;

        let upload = self.inner.clone();
        let bytes = mem::replace(&mut self.buf, fn_bytes_init()).freeze();
        self.handlers.push(Box::pin(async move {
            upload
                .upload_part(&upload_id, part_num, bytes.len(), Full::new(bytes))
                .await
        }));

        Ok(())
    }
}

impl Write for S3Writer {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        if self.buf.len() > S3_PART_MINIMUM_SIZE {
            if let Err(e) = self
                .upload_part(|| BytesMut::with_capacity(S3_PART_MINIMUM_SIZE))
                .await
            {
                return (Err(e), buf);
            }
        }
        self.buf.put(buf.as_slice());

        (Ok(()), buf)
    }

    async fn complete(&mut self) -> Result<(), Error> {
        let Some(upload_id) = self.upload_id.clone() else {
            if !self.buf.is_empty() {
                let bytes = mem::replace(&mut self.buf, BytesMut::new()).freeze();

                self.inner
                    .upload_once(bytes.len(), Full::new(bytes))
                    .await?;
            }
            return Ok(());
        };
        if !self.buf.is_empty() {
            self.upload_part(BytesMut::new).await?;
        }
        let mut parts = Vec::with_capacity(self.handlers.len());
        while let Some(handle) = self.handlers.next().await {
            parts.push(handle?);
        }
        assert_eq!(self.next_part_numer, parts.len());
        self.inner.complete_part(&upload_id, &parts).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[ignore]
    #[cfg(all(
        feature = "aws",
        feature = "tokio-http",
        not(feature = "completion-based")
    ))]
    #[tokio::test]
    async fn test_s3() {
        use std::sync::Arc;

        use bytes::Bytes;

        use crate::{
            remotes::aws::{
                multipart_upload::MultipartUpload, options::S3Options, writer::S3Writer,
                AwsCredential,
            },
            Write,
        };

        let region = "ap-southeast-2";
        let options = Arc::new(S3Options {
            endpoint: "endpoint".into(),
            credential: Some(AwsCredential {
                key_id: "key".to_string(),
                secret_key: "secret_key".to_string(),
                token: None,
            }),
            region: region.into(),
            sign_payload: true,
            checksum: false,
        });
        let client = Arc::new(crate::impls::remotes::http::tokio::TokioClient::new());
        let upload = MultipartUpload::new(options, "read-write.txt".into(), client);
        let mut writer = S3Writer::new(Arc::new(upload));

        let (result, _) = Write::write_all(&mut writer, Bytes::from("hello! Fusio!")).await;
        result.unwrap();
        let (result, _) = Write::write_all(&mut writer, Bytes::from("hello! World!")).await;
        result.unwrap();
        writer.complete().await.unwrap();
    }
}

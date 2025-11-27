use std::{borrow::ToOwned, mem, pin::Pin, sync::Arc};

use bytes::{BufMut, BytesMut};
use fusio_core::MaybeSendFuture;
use futures_util::{stream::FuturesOrdered, StreamExt};
use http::{header, HeaderMap};
use http_body_util::Full;

use crate::{
    error::Error,
    remotes::{
        aws::multipart_upload::{MultipartUpload, UploadType},
        serde::MultipartPart,
    },
    IoBuf, Write,
};

pub(crate) const S3_PART_MINIMUM_SIZE: usize = 5 * 1024 * 1024;
const DEFAULT_COPY_PART_SIZE: usize = 16 * 1024 * 1024;
const S3_PART_MAXIMUM_SIZE: u64 = 5 * 1024 * 1024 * 1024; // 5 GiB UploadPartCopy limit
const S3_MAX_PARTS: u64 = 10_000;

const PRESERVED_HEADER_NAMES: &[&str] = &[
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-language",
    "content-type",
    "expires",
    "x-amz-checksum-algorithm",
    "x-amz-object-lock-legal-hold",
    "x-amz-object-lock-mode",
    "x-amz-object-lock-retain-until-date",
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-bucket-key-enabled",
    "x-amz-server-side-encryption-context",
    "x-amz-storage-class",
    "x-amz-website-redirect-location",
];

const PRESERVED_HEADER_PREFIXES: &[&str] = &["x-amz-meta-"];

fn should_preserve_header(lower_name: &str) -> bool {
    let lower_ref: &str = lower_name;
    PRESERVED_HEADER_NAMES.contains(&lower_ref)
        || PRESERVED_HEADER_PREFIXES
            .iter()
            .any(|prefix| lower_name.starts_with(prefix))
}

pub struct S3Writer {
    inner: Arc<MultipartUpload>,
    upload_id: Option<Arc<String>>,
    next_part_numer: usize,
    buf: BytesMut,
    object_headers: Option<HeaderMap>,

    handlers: FuturesOrdered<Pin<Box<dyn MaybeSendFuture<Output = Result<MultipartPart, Error>>>>>,
}

unsafe impl Sync for S3Writer {}

impl S3Writer {
    pub fn new(inner: Arc<MultipartUpload>) -> Self {
        Self {
            inner,
            upload_id: None,
            next_part_numer: 0,
            buf: BytesMut::with_capacity(S3_PART_MINIMUM_SIZE),
            object_headers: None,
            handlers: FuturesOrdered::new(),
        }
    }

    pub fn copy_object_headers(&mut self, headers: &[(String, String)]) -> Result<(), Error> {
        if headers.iter().any(|(name, _)| {
            name.eq_ignore_ascii_case("x-amz-server-side-encryption-customer-algorithm")
        }) {
            return Err(Error::Unsupported {
                message: "appending to SSE-C encrypted S3 objects is not supported".into(),
            });
        }

        let mut filtered = HeaderMap::new();
        for (name, value) in headers.iter() {
            let lower = name.to_ascii_lowercase();
            if should_preserve_header(&lower) {
                if let (Ok(header_name), Ok(header_value)) = (
                    name.parse::<header::HeaderName>(),
                    value.parse::<header::HeaderValue>(),
                ) {
                    filtered.insert(header_name, header_value);
                }
            }
        }

        self.object_headers = if filtered.is_empty() {
            None
        } else {
            Some(filtered)
        };

        Ok(())
    }

    async fn ensure_upload_id(&mut self) -> Result<Arc<String>, Error> {
        match self.upload_id.clone() {
            Some(upload_id) => Ok(upload_id),
            None => {
                let upload_id = Arc::new(
                    self.inner
                        .initiate(self.object_headers.as_ref())
                        .await
                        .map_err(|err| Error::Remote(Box::new(err)))?,
                );
                self.upload_id = Some(upload_id.clone());
                Ok(upload_id)
            }
        }
    }

    pub async fn copy_existing_object(
        &mut self,
        object_size: u64,
        etag: Option<&str>,
    ) -> Result<bool, Error> {
        if object_size < S3_PART_MINIMUM_SIZE as u64 {
            return Ok(false);
        }

        let chunk_size = Self::determine_copy_part_size(object_size)?;
        let upload_id = self.ensure_upload_id().await?;
        let mut remaining = object_size;
        let mut offset = 0u64;
        let min_size = S3_PART_MINIMUM_SIZE as u64;
        let mut ranges: Vec<(u64, u64)> = Vec::new();

        while remaining > 0 {
            if remaining <= min_size {
                if ranges.is_empty() {
                    ranges.push((offset, remaining));
                } else if let Some(last) = ranges.last_mut() {
                    last.1 += remaining;
                }
                break;
            }

            let mut take = remaining.min(chunk_size);
            if take < min_size {
                take = min_size;
            }

            if remaining > take && remaining - take < min_size {
                take = remaining;
            }

            ranges.push((offset, take));
            offset = offset.saturating_add(take);
            remaining = remaining.saturating_sub(take);
        }

        let if_match = etag.map(ToOwned::to_owned);
        let part_count = ranges.len();
        for (idx, (start, len)) in ranges.into_iter().enumerate() {
            let upload = self.inner.clone();
            let upload_id = upload_id.clone();
            let range_start = start;
            let range_end = start + len - 1;
            let part_num = self.next_part_numer + idx;
            let if_match = if_match.clone();

            self.handlers.push_back(Box::pin(async move {
                upload
                    .copy_part(
                        &upload_id,
                        part_num,
                        range_start..=range_end,
                        if_match.as_deref(),
                    )
                    .await
            }));
        }
        self.next_part_numer += part_count;

        Ok(true)
    }

    fn determine_copy_part_size(object_size: u64) -> Result<u64, Error> {
        let min_part = S3_PART_MINIMUM_SIZE as u64;
        let default_part = DEFAULT_COPY_PART_SIZE as u64;
        let required_part = object_size.div_ceil(S3_MAX_PARTS);
        let chunk_size = min_part.max(default_part).max(required_part);

        if chunk_size > S3_PART_MAXIMUM_SIZE {
            return Err(Error::Unsupported {
                message: format!(
                    "S3 UploadPartCopy would need more than {S3_MAX_PARTS} parts even with 5 GiB \
                     chunks for an object of size {object_size} bytes",
                ),
            });
        }

        Ok(chunk_size)
    }
    async fn upload_part<F>(&mut self, fn_bytes_init: F) -> Result<(), Error>
    where
        F: FnOnce() -> BytesMut,
    {
        let upload_id = self.ensure_upload_id().await?;
        let part_num = self.next_part_numer;
        self.next_part_numer += 1;

        let upload = self.inner.clone();
        let bytes = mem::replace(&mut self.buf, fn_bytes_init()).freeze();
        self.handlers.push_back(Box::pin(async move {
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

    async fn flush(&mut self) -> Result<(), Error> {
        if self.buf.len() > S3_PART_MINIMUM_SIZE {
            self.upload_part(BytesMut::new).await?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        let Some(upload_id) = self.upload_id.clone() else {
            let bytes = mem::replace(&mut self.buf, BytesMut::new()).freeze();

            self.inner
                .upload_once(
                    UploadType::Write {
                        size: bytes.len(),
                        body: Full::new(bytes),
                    },
                    self.object_headers.as_ref(),
                )
                .await?;
            return Ok(());
        };
        if !self.buf.is_empty() {
            self.upload_part(BytesMut::new).await?;
        }
        let mut parts = Vec::with_capacity(self.handlers.len());
        while let Some(handle) = self.handlers.next().await {
            parts.push(handle.map_err(|err| Error::Remote(Box::new(err)))?);
        }
        assert_eq!(self.next_part_numer, parts.len());
        self.inner
            .complete_part(&upload_id, &parts)
            .await
            .map_err(|err| Error::Remote(Box::new(err)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_chunk_size_is_used_for_small_objects() {
        let chunk = S3Writer::determine_copy_part_size(1 * 1024 * 1024 * 1024).unwrap();
        assert_eq!(chunk, DEFAULT_COPY_PART_SIZE as u64);
    }

    #[test]
    fn chunk_size_scales_to_respect_part_limit() {
        let large_object = 200_u64 * 1024 * 1024 * 1024;
        let chunk = S3Writer::determine_copy_part_size(large_object).unwrap();
        assert!(chunk > DEFAULT_COPY_PART_SIZE as u64);
        let part_count = (large_object + chunk - 1) / chunk;
        assert!(part_count <= S3_MAX_PARTS);
    }

    #[test]
    fn exceeding_maximum_parts_returns_unsupported() {
        let too_large = S3_MAX_PARTS * S3_PART_MAXIMUM_SIZE + 1;
        let err = S3Writer::determine_copy_part_size(too_large).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }));
    }

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
            remotes::{
                aws::{
                    fs::{AmazonS3, AmazonS3Inner},
                    multipart_upload::MultipartUpload,
                    options::S3Options,
                    writer::S3Writer,
                    AwsCredential,
                },
                http::DynHttpClient,
            },
            Write,
        };

        let region = "ap-southeast-2";
        let options = S3Options {
            endpoint: "http://localhost:9000/data".into(),
            bucket: "data".to_string(),
            credential: Some(AwsCredential {
                key_id: "user".to_string(),
                secret_key: "password".to_string(),
                token: None,
            }),
            region: region.into(),
            sign_payload: true,
            checksum: false,
        };
        let client = crate::impls::remotes::http::tokio::TokioClient::new();

        let s3 = AmazonS3 {
            inner: Arc::new(AmazonS3Inner {
                options,
                client: Box::new(client) as Box<dyn DynHttpClient>,
            }),
        };

        let upload = MultipartUpload::new(s3, "read-write.txt".into());
        let mut writer = S3Writer::new(Arc::new(upload));

        let (result, _) = Write::write_all(&mut writer, Bytes::from("hello! Fusio!")).await;
        result.unwrap();
        let (result, _) = Write::write_all(&mut writer, Bytes::from("hello! World!")).await;
        result.unwrap();
        writer.close().await.unwrap();
    }
}

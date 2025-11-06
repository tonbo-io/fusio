use std::sync::Arc;

use bytes::Buf;
use http::{
    header::{CONTENT_LENGTH, RANGE},
    request::Builder,
    Method, Request,
};
use http_body_util::{BodyExt, Empty};
use percent_encoding::utf8_percent_encode;

use super::{
    fs::AmazonS3,
    sign::Sign,
    writer::{S3Writer, S3_PART_MINIMUM_SIZE},
    S3Error, STRICT_PATH_ENCODE_SET,
};
use crate::{
    durability::{FileCommit, FileSync},
    error::Error,
    path::Path,
    remotes::{
        aws::multipart_upload::MultipartUpload,
        http::{HttpClient, HttpError},
    },
    IoBuf, IoBufMut, Read, Write,
};

pub struct S3File {
    fs: AmazonS3,
    path: Path,
    writer: Option<S3Writer>,
    prefilled: bool,
}

impl S3File {
    #[cfg_attr(feature = "no-send", allow(clippy::arc_with_non_send_sync))]
    pub(crate) fn new(fs: AmazonS3, path: Path, create: bool) -> Self {
        Self {
            writer: create
                .then(|| S3Writer::new(Arc::new(MultipartUpload::new(fs.clone(), path.clone())))),
            fs,
            path,
            prefilled: false,
        }
    }

    pub(crate) async fn prefill_existing(&mut self) -> Result<(), Error> {
        if self.prefilled {
            return Ok(());
        }

        if self.writer.is_none() {
            self.prefilled = true;
            return Ok(());
        }

        let Some(existing) = self.fs.head_object(&self.path).await? else {
            self.prefilled = true;
            return Ok(());
        };

        let copied = {
            let writer = self.writer.as_mut().expect("writer checked above");
            writer.copy_object_headers(&existing.headers)?;

            if existing.size >= S3_PART_MINIMUM_SIZE as u64 {
                writer
                    .copy_existing_object(existing.size, existing.etag.as_deref())
                    .await?
            } else {
                false
            }
        };

        if !copied {
            let writer = self.writer.as_mut().expect("writer checked above");
            Self::download_existing_into_writer(
                self.fs.clone(),
                self.path.clone(),
                writer,
                existing.size,
            )
            .await?;
        }

        self.prefilled = true;
        Ok(())
    }

    async fn download_existing_into_writer(
        fs: AmazonS3,
        path: Path,
        writer: &mut S3Writer,
        existing_size: u64,
    ) -> Result<(), Error> {
        if existing_size == 0 {
            return Ok(());
        }

        let mut reader = S3File::new(fs, path, false);
        let mut offset: u64 = 0;
        let mut remaining = existing_size;
        const CHUNK_SIZE: u64 = 8 * 1024 * 1024;

        while remaining > 0 {
            let to_read = remaining.min(CHUNK_SIZE) as usize;
            let buf = vec![0u8; to_read];
            let (read_res, buf) = reader.read_exact_at(buf, offset).await;
            read_res?;
            let (write_res, _buf) = writer.write_all(buf).await;
            write_res?;

            offset = offset.saturating_add(to_read as u64);
            remaining = remaining.saturating_sub(to_read as u64);
        }

        writer.flush().await?;
        Ok(())
    }

    fn build_request(&self, method: Method) -> Builder {
        let endpoint = self.fs.as_ref().options.endpoint.trim_end_matches('/');
        let path_str = self.path.as_ref();
        let encoded = utf8_percent_encode(path_str, &STRICT_PATH_ENCODE_SET);
        let url = format!("{}/{}", endpoint, encoded);

        Request::builder().method(method).uri(url)
    }
}

impl Read for S3File {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let request = self
            .build_request(Method::GET)
            .header(
                RANGE,
                format!("bytes={}-{}", pos, pos + buf.as_slice().len() as u64 - 1),
            )
            .header(CONTENT_LENGTH, 0)
            .body(Empty::new())
            .map_err(|e| S3Error::from(HttpError::from(e)));

        let mut request = match request {
            Ok(request) => request,
            Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
        };

        if let Err(e) = request
            .sign(&self.fs.as_ref().options)
            .await
            .map_err(S3Error::from)
        {
            return (Err(Error::Remote(e.into())), buf);
        }

        let response = match self
            .fs
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(S3Error::from)
        {
            Ok(response) => response,
            Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
        };

        if !response.status().is_success() {
            (
                Err(Error::Remote(Box::new(HttpError::HttpNotSuccess {
                    status: response.status(),
                    body: String::from_utf8_lossy(
                        &response
                            .into_body()
                            .collect()
                            .await
                            .map(|b| b.to_bytes())
                            .unwrap_or_default(),
                    )
                    .to_string(),
                }))),
                buf,
            )
        } else {
            match response.into_body().collect().await.map_err(S3Error::from) {
                Ok(body) => {
                    if let Err(e) = std::io::Read::read_exact(
                        &mut body.aggregate().reader(),
                        buf.as_slice_mut(),
                    ) {
                        return (Err(e.into()), buf);
                    }
                }
                Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
            }

            (Ok(()), buf)
        }
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let mut request = match self
            .build_request(Method::GET)
            .header(RANGE, format!("bytes={}-", pos))
            .header(CONTENT_LENGTH, 0)
            .body(Empty::new())
            .map_err(|e| S3Error::from(HttpError::from(e)))
        {
            Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
            Ok(request) => request,
        };

        if let Err(e) = request
            .sign(&self.fs.as_ref().options)
            .await
            .map_err(S3Error::from)
        {
            return (Err(Error::Other(Box::new(e))), buf);
        }

        let response = match self
            .fs
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(S3Error::from)
        {
            Ok(response) => response,
            Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
        };

        if !response.status().is_success() {
            (
                Err(Error::Remote(Box::new(HttpError::HttpNotSuccess {
                    status: response.status(),
                    body: String::from_utf8_lossy(
                        &response
                            .into_body()
                            .collect()
                            .await
                            .map(|b| b.to_bytes())
                            .unwrap_or_default(),
                    )
                    .to_string(),
                }))),
                buf,
            )
        } else {
            match response.into_body().collect().await.map_err(S3Error::from) {
                Ok(body) => {
                    let mut body = body.to_bytes();
                    buf.resize(body.len(), 0);
                    body.copy_to_slice(&mut buf[..]);
                    (Ok(()), buf)
                }
                Err(e) => (Err(Error::Remote(Box::new(e))), buf),
            }
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        let mut request = self
            .build_request(Method::HEAD)
            .header(CONTENT_LENGTH, 0)
            .body(Empty::new())
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;
        request
            .sign(&self.fs.as_ref().options)
            .await
            .map_err(|err| Error::Remote(Box::new(err)))?;

        let response = self
            .fs
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(|err| Error::Remote(Box::new(err)))?;

        if !response.status().is_success() {
            Err(Error::Other(Box::new(HttpError::HttpNotSuccess {
                status: response.status(),
                body: String::from_utf8_lossy(
                    &response
                        .into_body()
                        .collect()
                        .await
                        .map_err(|err| Error::Remote(Box::new(err)))?
                        .to_bytes(),
                )
                .to_string(),
            })))
        } else {
            let size = response
                .headers()
                .get(CONTENT_LENGTH)
                .ok_or_else(|| Error::Other("missing content-length header".into()))
                .map_err(|err| Error::Other(Box::new(err)))?
                .to_str()
                .map_err(|e| Error::Remote(e.into()))?
                .parse::<u64>()
                .map_err(|e| Error::Remote(e.into()))?;
            Ok(size)
        }
    }
}

impl Write for S3File {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        self.writer
            .as_mut()
            .expect("write file after closed")
            .write_all(buf)
            .await
    }

    async fn flush(&mut self) -> Result<(), Error> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(mut writer) = self.writer.take() {
            writer.close().await?;
        }
        Ok(())
    }
}

impl FileSync for S3File {
    async fn sync_data(&mut self) -> Result<(), Error> {
        // Best-effort: flush any buffered parts to the service.
        self.flush().await
    }

    async fn sync_all(&mut self) -> Result<(), Error> {
        // There is no distinct fsync concept; rely on flush.
        self.flush().await
    }

    async fn sync_range(&mut self, _offset: u64, _len: u64) -> Result<(), Error> {
        self.flush().await
    }
}

impl FileCommit for S3File {
    async fn commit(&mut self) -> Result<(), Error> {
        if let Some(mut writer) = self.writer.take() {
            writer.close().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::{Arc, Mutex},
    };

    use bytes::Bytes;
    use fusio_core::MaybeSendFuture;
    use futures_executor::block_on;
    use http::{HeaderMap, Method, StatusCode};
    use http_body_util::{BodyExt, Full};

    use super::*;
    use crate::{
        fs::{Fs, OpenOptions},
        impls::remotes::http::{BoxBody, DynHttpClient, HttpError},
    };

    #[derive(Clone, Default)]
    struct RecordedRequest {
        method: Method,
        uri: String,
        headers: HeaderMap,
    }

    #[derive(Clone)]
    struct MockHttpClient {
        responses: Arc<Mutex<VecDeque<http::Response<BoxBody>>>>,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
    }

    impl MockHttpClient {
        fn new(responses: Vec<http::Response<BoxBody>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect::<VecDeque<_>>())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn record(&self, request: RecordedRequest) {
            self.requests.lock().unwrap().push(request);
        }

        fn take_requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl DynHttpClient for MockHttpClient {
        fn dyn_send_request(
            &self,
            request: http::Request<BoxBody>,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<http::Response<BoxBody>, HttpError>> + '_>>
        {
            let (parts, _body) = request.into_parts();
            let recorded = RecordedRequest {
                method: parts.method.clone(),
                uri: parts.uri.to_string(),
                headers: parts.headers.clone(),
            };
            self.record(recorded);

            let response = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("mock response exhausted");

            Box::pin(async move { Ok(response) })
        }
    }

    fn mock_response(
        status: StatusCode,
        headers: &[(&str, &str)],
        body: &str,
    ) -> http::Response<BoxBody> {
        let mut builder = http::Response::builder().status(status);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder
            .body(BoxBody::new(
                Full::new(Bytes::from(body.to_owned()))
                    .map_err(|_| HttpError::Other("infallible".into())),
            ))
            .expect("failed to build mock response")
    }

    #[test]
    fn appending_large_object_uses_server_side_copy() {
        let copy_size = (S3_PART_MINIMUM_SIZE as u64) * 2;
        let etag = "\"etag-123\"";

        let responses = vec![
            mock_response(
                StatusCode::OK,
                &[
                    ("content-length", &copy_size.to_string()),
                    ("etag", etag),
                    ("x-amz-meta-foo", "bar"),
                    ("x-amz-server-side-encryption", "AES256"),
                ],
                "",
            ),
            mock_response(
                StatusCode::OK,
                &[],
                "<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></\
                 InitiateMultipartUploadResult>",
            ),
            mock_response(
                StatusCode::OK,
                &[],
                "<CopyPartResult><ETag>\"copy-etag\"</ETag><LastModified>2024-01-01T00:00:00Z</\
                 LastModified></CopyPartResult>",
            ),
            mock_response(StatusCode::OK, &[("etag", "\"append-etag\"")], ""),
            mock_response(StatusCode::OK, &[], "<CompleteMultipartUploadResult/>"),
        ];

        let client = MockHttpClient::new(responses);

        let options = super::super::options::S3Options {
            endpoint: "https://s3.test".into(),
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            credential: None,
            sign_payload: false,
            checksum: false,
        };

        let s3 = super::super::fs::AmazonS3 {
            inner: Arc::new(super::super::fs::AmazonS3Inner {
                options,
                client: Box::new(client.clone()) as Box<dyn DynHttpClient>,
            }),
        };

        let path: Path = "data.bin".into();
        block_on(async {
            let mut file = Fs::open_options(&s3, &path, OpenOptions::default().write(true))
                .await
                .expect("open file");

            let append = vec![1u8; 1_024];
            let (res, _buf) = file.write_all(append).await;
            res.expect("write append data");
            file.close().await.expect("close");
        });

        let requests = client.take_requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0].method, Method::HEAD);
        assert!(requests[0].uri.contains("data.bin"));

        assert_eq!(requests[1].method, Method::POST);
        assert!(requests[1].uri.contains("uploads"));
        assert_eq!(
            requests[1]
                .headers
                .get("x-amz-server-side-encryption")
                .expect("sse header")
                .to_str()
                .unwrap(),
            "AES256"
        );

        assert_eq!(requests[2].method, Method::PUT);
        assert!(requests[2].uri.contains("partNumber=1"));
        assert_eq!(
            requests[2]
                .headers
                .get("x-amz-copy-source-if-match")
                .expect("if-match")
                .to_str()
                .unwrap(),
            etag
        );
        assert_eq!(
            requests[2]
                .headers
                .get("x-amz-copy-source-range")
                .unwrap()
                .to_str()
                .unwrap(),
            format!("bytes=0-{}", copy_size - 1)
        );

        assert_ne!(requests[2].method, Method::GET);
        assert_ne!(requests[3].method, Method::GET);
        assert_ne!(requests[4].method, Method::GET);
    }

    #[test]
    fn small_objects_fallback_to_download() {
        let existing = S3_PART_MINIMUM_SIZE as u64 - 1;
        let responses = vec![
            mock_response(
                StatusCode::OK,
                &[
                    ("content-length", &existing.to_string()),
                    ("etag", "\"small\""),
                    ("x-amz-meta-foo", "bar"),
                ],
                "",
            ),
            mock_response(
                StatusCode::PARTIAL_CONTENT,
                &[("content-length", &existing.to_string())],
                &"x".repeat(existing as usize),
            ),
            mock_response(StatusCode::OK, &[("etag", "\"upload\"")], ""),
        ];

        let client = MockHttpClient::new(responses);
        let options = super::super::options::S3Options {
            endpoint: "https://s3.test".into(),
            bucket: "bucket".into(),
            region: "us-east-1".into(),
            credential: None,
            sign_payload: false,
            checksum: false,
        };

        let s3 = super::super::fs::AmazonS3 {
            inner: Arc::new(super::super::fs::AmazonS3Inner {
                options,
                client: Box::new(client.clone()) as Box<dyn DynHttpClient>,
            }),
        };

        let path: Path = "small.bin".into();
        block_on(async {
            let mut file = Fs::open_options(&s3, &path, OpenOptions::default().write(true))
                .await
                .expect("open file");
            let (res, _buf) = file.write_all(vec![2u8; 4]).await;
            res.expect("append");
            file.close().await.expect("close");
        });

        let requests = client.take_requests();
        assert_eq!(requests[0].method, Method::HEAD);
        assert!(requests.iter().any(|req| req.method == Method::GET));
        let put_request = requests
            .iter()
            .find(|req| req.method == Method::PUT)
            .expect("expected final PUT");
        assert!(put_request
            .headers
            .get("content-length")
            .and_then(|hv| hv.to_str().ok())
            .is_some());
        assert_eq!(
            put_request
                .headers
                .get("x-amz-meta-foo")
                .and_then(|hv| hv.to_str().ok())
                .unwrap(),
            "bar"
        );
    }

    #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
    #[tokio::test]
    async fn write_and_read_s3_file() {
        use std::sync::Arc;

        use crate::{
            fs::{Fs, OpenOptions},
            path::Path,
            remotes::{
                aws::{
                    credential::AwsCredential,
                    fs::{AmazonS3, AmazonS3Inner},
                    options::S3Options,
                    s3::S3File,
                },
                http::{tokio::TokioClient, DynHttpClient},
            },
            Read, Write,
        };

        if option_env!("AWS_ACCESS_KEY_ID").is_none()
            || option_env!("AWS_SECRET_ACCESS_KEY").is_none()
        {
            eprintln!("can not get `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`");
            return;
        }
        let key_id = std::option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = std::option_env!("AWS_SECRET_ACCESS_KEY")
            .unwrap()
            .to_string();

        let client = TokioClient::new();
        let bucket = std::option_env!("BUCKET_NAME")
            .expect("expected bucket not to be empty")
            .to_string();
        let region = std::option_env!("AWS_REGION")
            .expect("expected region not to be empty")
            .to_string();
        let token = std::option_env!("AWS_SESSION_TOKEN").map(|v| v.to_string());

        let options = S3Options {
            endpoint: format!("https://{}.s3.{}.amazonaws.com", &bucket, &region),
            bucket,
            credential: Some(AwsCredential {
                key_id,
                secret_key,
                token,
            }),
            region,
            sign_payload: true,
            checksum: false,
        };

        let s3 = AmazonS3 {
            inner: Arc::new(AmazonS3Inner {
                options,
                client: Box::new(client) as Box<dyn DynHttpClient>,
            }),
        };

        let path: Path = "read-write.txt".into();
        let initial = b"The answer of life, universe and everthing";
        let appended = b" (revisited)";

        {
            let mut file = s3
                .open_options(&path, OpenOptions::default().create(true).truncate(true))
                .await
                .unwrap();

            let (result, _) = file.write_all(&initial[..]).await;
            result.unwrap();
            file.close().await.unwrap();
        }

        {
            let mut file = s3
                .open_options(&path, OpenOptions::default().write(true))
                .await
                .unwrap();

            let (result, _) = file.write_all(&appended[..]).await;
            result.unwrap();
            file.close().await.unwrap();
        }

        let mut reader = S3File::new(s3, path.clone(), false);

        let size = reader.size().await.unwrap();
        assert_eq!(size, (initial.len() + appended.len()) as u64);
        let buf = Vec::new();
        let (result, buf) = reader.read_to_end_at(buf, 0).await;
        result.unwrap();
        let mut expected = initial.to_vec();
        expected.extend_from_slice(appended);
        assert_eq!(buf, expected);
    }

    #[ignore]
    #[cfg(all(feature = "monoio-http", feature = "completion-based"))]
    #[monoio::test(enable_timer = true)]
    async fn monoio_write_and_read_s3_file() {
        use crate::{
            remotes::aws::{credential::AwsCredential, fs::AmazonS3Builder, s3::S3File},
            Read, Write,
        };

        if option_env!("AWS_ACCESS_KEY_ID").is_none()
            || option_env!("AWS_SECRET_ACCESS_KEY").is_none()
        {
            eprintln!("can not get `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`");
            return;
        }
        let key_id = std::option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = std::option_env!("AWS_SECRET_ACCESS_KEY")
            .unwrap()
            .to_string();

        let bucket = std::option_env!("BUCKET_NAME")
            .expect("expected bucket not to be empty")
            .to_string();
        let region = std::option_env!("AWS_REGION")
            .expect("expected region not to be empty")
            .to_string();
        let token = std::option_env!("AWS_SESSION_TOKEN").map(|v| v.to_string());
        let s3 = AmazonS3Builder::new(bucket)
            .credential(AwsCredential {
                key_id,
                secret_key,
                token,
            })
            .region(region)
            .sign_payload(true)
            .build();

        {
            let mut s3 = S3File::new(s3.clone(), "read-write.txt".into(), true);

            let (result, _) = s3
                .write_all(&b"The answer of life, universe and everthing"[..])
                .await;
            result.unwrap();
            s3.close().await.unwrap();
        }
        let mut s3 = S3File::new(s3, "read-write.txt".into(), false);

        let size = s3.size().await.unwrap();
        assert_eq!(size, 42);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, b"The answer of life, universe and everthing");
    }
}

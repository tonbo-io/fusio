use core::pin::Pin;
use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_stream::stream;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use fusio_core::MaybeSendFuture;
use futures_core::Stream;
use http::{
    header::{self},
    Method, Request, StatusCode,
};
use http_body_util::{BodyExt, Empty};
use serde::{Deserialize, Serialize};
use url::Url;

use super::{credential::AwsCredential, options::S3Options, S3Error, S3File};
use crate::{
    error::Error,
    fs::{CasCondition, FileMeta, FileSystemTag, Fs, FsCas, OpenOptions},
    impls::remotes::aws::head::ETag,
    path::Path,
    remotes::{
        aws::{
            multipart_upload::{MultipartUpload, UploadType},
            sign::Sign,
        },
        http::{DynHttpClient, HttpClient, HttpError},
    },
};

pub struct AmazonS3Builder {
    endpoint: Option<String>,
    region: String,
    bucket: String,
    credential: Option<AwsCredential>,
    sign_payload: bool,
    checksum: bool,
    client: Box<dyn DynHttpClient>,
}

impl AmazonS3Builder {
    #[allow(unreachable_code, unused_variables)]
    pub fn new(bucket: String) -> Self {
        #[allow(clippy::needless_late_init, unused_variables)]
        let client: Box<dyn DynHttpClient>;
        cfg_if::cfg_if! {
            if #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))] {
                client = Box::new(crate::remotes::http::tokio::TokioClient::new());
            } else if #[cfg(all(feature = "web-http", not(feature = "completion-based")))]{
                client = Box::new(crate::remotes::http::wasm::WasmClient::new());
            } else if #[cfg(all(feature = "monoio-http", feature = "completion-based"))]{
                client = Box::new(crate::remotes::http::monoio::MonoioClient::new());
            } else {
                unreachable!()
            }
        }

        Self {
            endpoint: None,
            region: "us-east-1".into(),
            bucket,
            credential: None,
            sign_payload: false,
            checksum: false,
            client,
        }
    }
}

impl AmazonS3Builder {
    pub fn region(mut self, region: String) -> Self {
        self.region = region;
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn credential(mut self, credential: AwsCredential) -> Self {
        self.credential = Some(credential);
        self
    }

    pub fn sign_payload(mut self, sign_payload: bool) -> Self {
        self.sign_payload = sign_payload;
        self
    }

    pub fn checksum(mut self, checksum: bool) -> Self {
        self.checksum = checksum;
        self
    }

    pub fn build(self) -> AmazonS3 {
        let trimmed_bucket = self.bucket.trim_start_matches('/');
        let endpoint = if let Some(endpoint) = self.endpoint {
            let trimmed_endpoint = endpoint.trim_end_matches('/');
            format!("{}/{}/", trimmed_endpoint, trimmed_bucket)
        } else {
            format!(
                "https://{}.s3.{}.amazonaws.com",
                trimmed_bucket, self.region
            )
        };

        AmazonS3 {
            #[allow(clippy::arc_with_non_send_sync)]
            inner: Arc::new(AmazonS3Inner {
                options: S3Options {
                    endpoint,
                    bucket: self.bucket,
                    region: self.region,
                    credential: self.credential,
                    sign_payload: self.sign_payload,
                    checksum: self.checksum,
                },
                client: self.client,
            }),
        }
    }
}

#[derive(Clone)]
pub struct AmazonS3 {
    pub(super) inner: Arc<AmazonS3Inner>,
}

#[derive(Debug, Clone)]
pub struct HeadObject {
    pub size: u64,
    pub etag: Option<String>,
    pub metadata: HashMap<String, String>,
    pub headers: Vec<(String, String)>,
}

impl AsRef<AmazonS3Inner> for AmazonS3 {
    fn as_ref(&self) -> &AmazonS3Inner {
        self.inner.as_ref()
    }
}

pub(super) struct AmazonS3Inner {
    pub(super) options: S3Options,
    pub(super) client: Box<dyn DynHttpClient>,
}

impl AmazonS3 {
    #[allow(dead_code)]
    pub(crate) fn new(client: Box<dyn DynHttpClient>, options: S3Options) -> Self {
        AmazonS3 {
            #[allow(clippy::arc_with_non_send_sync)]
            inner: Arc::new(AmazonS3Inner { options, client }),
        }
    }

    pub async fn head_object(&self, path: &Path) -> Result<Option<HeadObject>, Error> {
        let mut url = Url::from_str(self.as_ref().options.endpoint.as_str())
            .map_err(|e| S3Error::from(HttpError::from(e)))
            .map_err(|e| Error::Remote(Box::new(e)))?;
        url = url
            .join(path.as_ref())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;

        let mut request = Request::builder()
            .method(Method::HEAD)
            .uri(url.as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;
        request
            .sign(&self.as_ref().options)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        let response = self
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        if response.status() == StatusCode::NOT_FOUND {
            let _ = response
                .into_body()
                .collect()
                .await
                .map_err(|e| Error::Remote(e.into()))?;
            return Ok(None);
        }

        if !response.status().is_success() {
            return Err(Error::Remote(
                HttpError::HttpNotSuccess {
                    status: response.status(),
                    body: String::from_utf8_lossy(
                        &response
                            .into_body()
                            .collect()
                            .await
                            .map_err(|e| Error::Remote(e.into()))?
                            .to_bytes(),
                    )
                    .to_string(),
                }
                .into(),
            ));
        }

        let headers = response.headers().clone();
        let size = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|hv| hv.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let etag = headers
            .get(header::ETAG)
            .and_then(|hv| hv.to_str().ok())
            .map(|s| s.to_string());

        let mut metadata = HashMap::new();
        for (name, value) in headers.iter() {
            let lower = name.as_str().to_ascii_lowercase();
            if lower.starts_with("x-amz-meta-") {
                if let Ok(val) = value.to_str() {
                    metadata.insert(lower.clone(), val.to_string());
                }
            }
        }

        let mut header_pairs = Vec::new();
        for (name, value) in headers.iter() {
            if let Ok(val) = value.to_str() {
                header_pairs.push((name.as_str().to_string(), val.to_string()));
            }
        }

        let _ = response
            .into_body()
            .collect()
            .await
            .map_err(|e| Error::Remote(e.into()))?;

        Ok(Some(HeadObject {
            size,
            etag,
            metadata,
            headers: header_pairs,
        }))
    }
}

impl Fs for AmazonS3 {
    type File = S3File;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::S3
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let mut file = S3File::new(self.clone(), path.clone(), options.create || options.write);

        if options.write && !options.truncate {
            file.prefill_existing().await?;
        }

        Ok(file)
    }

    async fn create_dir_all(_path: &Path) -> Result<(), Error> {
        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        Ok(stream! {
            let mut next_token = None::<String>;
            loop {
                let path = path.to_string();
                let mut query = vec![("list-type", "2"), ("prefix", path.as_str())];
                if let Some(token) = next_token.as_ref() {
                    query.push(("continuation-token", token.as_str()));
                }

                let mut url = Url::from_str(self.as_ref().options.endpoint.as_str())
                    .map_err(|e| S3Error::from(HttpError::from(e)))
                    .map_err(|err| Error::Remote(Box::new(err)))?;
                let result = {
                    let mut pairs = url.query_pairs_mut();
                    let serializer = serde_urlencoded::Serializer::new(&mut pairs);
                    query
                        .serialize(serializer)
                        .map(|_| ())
                };
                result.map_err(|e| S3Error::from(HttpError::from(e))).map_err(|err| Error::Remote(Box::new(err)))?;

                let mut request = Request::builder()
                    .method(Method::GET)
                    .uri(url.as_str())
                    .body(Empty::<Bytes>::new())
                    .map_err(|e| S3Error::from(HttpError::from(e)))
                    .map_err(|err| Error::Remote(Box::new(err)))?;
                request.sign(&self.as_ref().options).await
                    .map_err(S3Error::from)
                    .map_err(|err| Error::Path(Box::new(err)))?;
                let response = self.as_ref().client.send_request(request).await
                    .map_err(S3Error::from)
                    .map_err(|err| Error::Path(Box::new(err)))?;

                if !response.status().is_success() {
                    yield Err(Error::Other(Box::new(HttpError::HttpNotSuccess {
                        status: response.status(),
                        body: String::from_utf8_lossy(
                            &response
                                .collect()
                                .await
                                .map_err(|e| Error::Remote(e.into()))?
                                .to_bytes()
                        ).to_string()
                    })));
                    return;
                }

                let mut response: ListResponse = quick_xml::de::from_reader(
                    response
                    .collect()
                    .await
                    .map_err(|e| Error::Remote(e.into()))?
                    .aggregate().reader()
                ).map_err(|err| Error::Remote(err.into()))?;

                next_token = response.next_continuation_token.take();

                for content in &response.contents {
                    yield Ok(FileMeta {
                        path: Path::parse(&content.key).map_err(|err| Error::Path(Box::new(err)))?,
                        size: content.size as u64
                    });
                }

                if next_token.is_none() {
                    break;
                }
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let mut url = Url::from_str(self.as_ref().options.endpoint.as_str())
            .map_err(|e| S3Error::from(HttpError::from(e)))
            .map_err(|e| Error::Remote(e.into()))?;
        url = url
            .join(path.as_ref())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;

        let mut request = Request::builder()
            .method(Method::DELETE)
            .uri(url.as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;
        request
            .sign(&self.as_ref().options)
            .await
            .map_err(|err| Error::Remote(err.into()))?;
        let response = self
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        if !response.status().is_success() {
            return Err(Error::Remote(
                HttpError::HttpNotSuccess {
                    status: response.status(),
                    body: String::from_utf8_lossy(
                        &response
                            .collect()
                            .await
                            .map_err(|e| Error::Remote(e.into()))?
                            .to_bytes(),
                    )
                    .to_string(),
                }
                .into(),
            ));
        }

        Ok(())
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let upload = MultipartUpload::new(self.clone(), to.clone());
        upload
            .upload_once(
                UploadType::Copy {
                    bucket: self.inner.options.bucket.clone(),
                    from: from.clone(),
                    body: Empty::<Bytes>::new(),
                },
                None,
            )
            .await?;

        Ok(())
    }

    async fn link(&self, _: &Path, _: &Path) -> Result<(), Error> {
        Err(Error::Unsupported {
            message: "s3 does not support link file".to_string(),
        })
    }

    async fn exists(&self, path: &Path) -> Result<bool, Error> {
        let mut url = Url::from_str(self.as_ref().options.endpoint.as_str())
            .map_err(|e| S3Error::from(HttpError::from(e)))
            .map_err(|e| Error::Remote(Box::new(e)))?;
        url = url
            .join(path.as_ref())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;

        let mut request = Request::builder()
            .method(Method::HEAD)
            .uri(url.as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;

        request
            .sign(&self.as_ref().options)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        let response = self
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        let is_not_found = response.status() == StatusCode::NOT_FOUND;

        let _ = response
            .into_body()
            .collect()
            .await
            .map_err(|e| Error::Remote(e.into()))?;

        Ok(is_not_found)
    }
}

impl FsCas for AmazonS3 {
    fn load_with_tag(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(Vec<u8>, String)>, Error>> + '_>> {
        let key = path.to_string();
        Box::pin(async move {
            match self.get_with_etag(&key).await? {
                Some((bytes, etag)) => Ok(Some((bytes.to_vec(), etag.0))),
                None => Ok(None),
            }
        })
    }

    fn put_conditional(
        &self,
        path: &Path,
        payload: &[u8],
        content_type: Option<&str>,
        metadata: Option<Vec<(String, String)>>,
        condition: CasCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<String, Error>> + '_>> {
        let key = path.to_string();
        let ct = content_type.map(|s| s.to_string());
        let payload = Bytes::copy_from_slice(payload);
        let metadata = metadata.unwrap_or_default();
        Box::pin(async move {
            let ct_ref = ct.as_deref();
            let metadata_ref = if metadata.is_empty() {
                None
            } else {
                Some(metadata.as_slice())
            };
            let result = match condition {
                CasCondition::IfNotExists => {
                    self.put_if_none_match(&key, payload, ct_ref, metadata_ref)
                        .await
                }
                CasCondition::IfMatch(tag) => {
                    let etag = ETag(tag);
                    self.put_if_match(&key, payload, &etag, ct_ref, metadata_ref)
                        .await
                }
            }?;
            Ok(result.0)
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPrefix {
    pub prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListContents {
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
    #[serde(rename = "ETag")]
    pub e_tag: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListResponse {
    #[serde(default)]
    pub contents: Vec<ListContents>,
    #[serde(default)]
    pub common_prefixes: Vec<ListPrefix>,
    #[serde(default)]
    pub next_continuation_token: Option<String>,
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "tokio-http")]
    use crate::{fs::Fs, path::Path};

    #[cfg(feature = "tokio-http")]
    #[tokio::test]
    async fn list_and_remove() {
        use std::{env, pin::pin};

        use fusio_core::Write;
        use futures_util::StreamExt;

        use super::*;

        if env::var("AWS_ACCESS_KEY_ID").is_err() {
            eprintln!("skipping AWS s3 test");
            return;
        }
        let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();
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

        let dir = Path::parse("list").unwrap();
        {
            let file_path = dir.child("file");
            let mut file = s3
                .open_options(
                    &file_path,
                    OpenOptions::default().create(true).truncate(true),
                )
                .await
                .unwrap();
            file.close().await.unwrap();
        }
        let mut stream = pin!(s3.list(&dir).await.unwrap());
        while let Some(meta) = stream.next().await {
            let meta = meta.unwrap();
            s3.remove(&meta.path).await.unwrap();
        }
    }

    #[ignore]
    #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
    #[tokio::test]
    async fn copy() {
        use std::sync::Arc;

        use crate::{
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

        let key_id = "user".to_string();
        let secret_key = "password".to_string();

        let client = TokioClient::new();
        let region = "ap-southeast-1";
        let options = S3Options {
            endpoint: "http://localhost:9000/data".into(),
            bucket: "data".to_string(),
            credential: Some(AwsCredential {
                key_id,
                secret_key,
                token: None,
            }),
            region: region.into(),
            sign_payload: true,
            checksum: true,
        };

        let s3 = AmazonS3 {
            inner: Arc::new(AmazonS3Inner {
                options,
                client: Box::new(client) as Box<dyn DynHttpClient>,
            }),
        };

        let from_path: Path = "read-write.txt".into();
        let to_path: Path = "read-write-copy.txt".into();
        {
            let mut s3 = S3File::new(s3.clone(), from_path.clone(), false);

            let (result, _) = s3
                .write_all(&b"The answer of life, universe and everthing"[..])
                .await;
            result.unwrap();
            s3.close().await.unwrap();
        }
        s3.copy(&from_path, &to_path).await.unwrap();
        let mut s3 = S3File::new(s3, to_path.clone(), false);

        let size = s3.size().await.unwrap();
        assert_eq!(size, 42);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, b"The answer of life, universe and everthing");
    }
}

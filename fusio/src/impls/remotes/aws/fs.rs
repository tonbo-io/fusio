use std::{str::FromStr, sync::Arc};

use async_stream::stream;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use futures_core::Stream;
use http::{Method, Request};
use http_body_util::{BodyExt, Empty};
use serde::{Deserialize, Serialize};
use url::Url;

use super::{credential::AwsCredential, options::S3Options, S3Error, S3File};
use crate::{
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::Path,
    remotes::{
        aws::{
            multipart_upload::{MultipartUpload, UploadType},
            sign::Sign,
        },
        http::{DynHttpClient, HttpClient, HttpError},
    },
    Error,
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
    #[allow(unused_variables)]
    pub fn new(bucket: String) -> Self {
        let client: Box<dyn DynHttpClient>;
        cfg_if::cfg_if! {
            if #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))] {
                client = Box::new(crate::remotes::http::tokio::TokioClient::new());
            } else if #[cfg(all(feature = "wasm-http", not(feature = "completion-based")))]{
                client = Box::new(crate::remotes::http::wasm::WasmClient::new());
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
}

impl Fs for AmazonS3 {
    type File = S3File;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::S3
    }

    async fn open_options(&self, path: &Path, _: OpenOptions) -> Result<Self::File, crate::Error> {
        Ok(S3File::new(self.clone(), path.clone()))
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

                let mut url = Url::from_str(self.as_ref().options.endpoint.as_str()).map_err(|e| S3Error::from(HttpError::from(e)))?;
                let result = {
                    let mut pairs = url.query_pairs_mut();
                    let serializer = serde_urlencoded::Serializer::new(&mut pairs);
                    query
                        .serialize(serializer)
                        .map(|_| ())
                };
                result.map_err(|e| S3Error::from(HttpError::from(e)))?;

                let mut request = Request::builder()
                    .method(Method::GET)
                    .uri(url.as_str())
                    .body(Empty::<Bytes>::new()).map_err(|e| S3Error::from(HttpError::from(e)))?;
                request.sign(&self.as_ref().options).await.map_err(S3Error::from)?;
                let response = self.as_ref().client.send_request(request).await.map_err(S3Error::from)?;

                if !response.status().is_success() {
                    yield Err(S3Error::from(HttpError::HttpNotSuccess { status: response.status(), body: String::from_utf8_lossy(
                        &response
                        .collect()
                        .await
                        .map_err(|e| Error::Other(e.into()))?.to_bytes()).to_string()
                    }).into());
                    return;
                }

                let mut response: ListResponse = quick_xml::de::from_reader(
                    response
                    .collect()
                    .await
                    .map_err(S3Error::from)?
                    .aggregate().reader()
                ).map_err(S3Error::from)?;

                next_token = response.next_continuation_token.take();

                for content in &response.contents {
                    yield Ok(FileMeta {
                        path: Path::parse(&content.key)?,
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
            .map_err(|e| S3Error::from(HttpError::from(e)))?;
        url = url
            .join(path.as_ref())
            .map_err(|e| S3Error::from(HttpError::from(e)))?;

        let mut request = Request::builder()
            .method(Method::DELETE)
            .uri(url.as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|e| S3Error::from(HttpError::from(e)))?;
        request
            .sign(&self.as_ref().options)
            .await
            .map_err(S3Error::from)?;
        let response = self
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(S3Error::from)?;

        if !response.status().is_success() {
            return Err(S3Error::from(HttpError::HttpNotSuccess {
                status: response.status(),
                body: String::from_utf8_lossy(
                    &response
                        .collect()
                        .await
                        .map_err(|e| Error::Other(e.into()))?
                        .to_bytes(),
                )
                .to_string(),
            })
            .into());
        }

        Ok(())
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let upload = MultipartUpload::new(self.clone(), to.clone());
        upload
            .upload_once(UploadType::Copy {
                bucket: self.inner.options.bucket.clone(),
                from: from.clone(),
                body: Empty::<Bytes>::new(),
            })
            .await?;

        Ok(())
    }

    async fn link(&self, _: &Path, _: &Path) -> Result<(), Error> {
        Err(Error::Unsupported {
            message: "s3 does not support link file".to_string(),
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

        use futures_util::StreamExt;

        use super::*;

        if env::var("AWS_ACCESS_KEY_ID").is_err() {
            eprintln!("skipping AWS s3 test");
            return;
        }
        let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        let s3 = AmazonS3Builder::new("fusio-test".into())
            .credential(AwsCredential {
                key_id,
                secret_key,
                token: None,
            })
            .region("ap-southeast-1".into())
            .sign_payload(true)
            .build();

        let path = Path::parse("test").unwrap();
        let mut stream = pin!(s3.list(&path).await.unwrap());
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
            checksum: false,
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
            let mut s3 = S3File::new(s3.clone(), from_path.clone());

            let (result, _) = s3
                .write_all(&b"The answer of life, universe and everthing"[..])
                .await;
            result.unwrap();
            s3.close().await.unwrap();
        }
        s3.copy(&from_path, &to_path).await.unwrap();
        let mut s3 = S3File::new(s3, to_path.clone());

        let size = s3.size().await.unwrap();
        assert_eq!(size, 42);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, b"The answer of life, universe and everthing");
    }
}

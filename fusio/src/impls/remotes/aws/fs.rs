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
    fs::{FileMeta, Fs, OpenOptions},
    path::Path,
    remotes::{
        aws::sign::Sign,
        http::{DynHttpClient, HttpClient, HttpError},
    },
    Error,
};

pub struct AmazonS3Builder {
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
        cfg_if::cfg_if! {
            if #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))] {
                let client = Box::new(crate::remotes::http::tokio::TokioClient::new());
                Self {
                    region: "us-east-1".into(),
                    bucket,
                    credential: None,
                    sign_payload: false,
                    checksum: false,
                    client,
                }
            } else {
                unreachable!()
            }
        }
    }
}

impl AmazonS3Builder {
    pub fn region(mut self, region: String) -> Self {
        self.region = region;
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
        AmazonS3 {
            inner: Arc::new(AmazonS3Inner {
                options: S3Options {
                    endpoint: format!("https://{}.s3.{}.amazonaws.com", self.bucket, self.region),
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

impl Fs for AmazonS3 {
    type File = S3File;

    async fn open_options(
        &self,
        path: &Path,
        _: OpenOptions,
    ) -> Result<Self::File, crate::Error> {
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
                {
                    let mut pairs = url.query_pairs_mut();
                    let serializer = serde_urlencoded::Serializer::new(&mut pairs);
                    query
                        .serialize(serializer)
                        .map_err(|e| S3Error::from(HttpError::from(e)))?;
                }

                let mut request = Request::builder()
                    .method(Method::GET)
                    .uri(url.as_str())
                    .body(Empty::<Bytes>::new()).map_err(|e| S3Error::from(HttpError::from(e)))?;
                request.sign(&self.as_ref().options).await.map_err(S3Error::from)?;
                let response = self.as_ref().client.as_ref().send_request(request).await.map_err(S3Error::from)?;

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
        url.set_path(path.as_ref());

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
            .as_ref()
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
}

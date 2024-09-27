use std::{str::FromStr, sync::Arc};

use async_stream::stream;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use futures_core::Stream;
use http::{Method, Request};
use http_body_util::{BodyExt, Empty};
use serde::{Deserialize, Serialize};
use url::Url;

use super::{credential::AwsCredential, options::S3Options, S3File};
use crate::{
    fs::{FileMeta, Fs, OpenOptions, WriteMode},
    path::Path,
    remotes::{
        aws::sign::Sign,
        http::{DynHttpClient, HttpClient},
    },
    Error,
};

pub struct AmazonS3Builder {
    region: String,
    bucket: String,
    credential: Option<AwsCredential>,
    sign_payload: bool,
    checksum: bool,
    client: Arc<dyn DynHttpClient>,
}

impl AmazonS3Builder {
    fn new<C: HttpClient>(bucket: String, client: C) -> Self {
        Self {
            region: "us-east-1".into(),
            bucket,
            credential: None,
            sign_payload: false,
            checksum: false,
            client: Arc::new(client) as Arc<dyn DynHttpClient>,
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
            options: Arc::new(S3Options {
                endpoint: format!("https://{}.s3.{}.amazonaws.com", self.bucket, self.region),
                region: self.region,
                credential: self.credential,
                sign_payload: self.sign_payload,
                checksum: self.checksum,
            }),
            client: self.client,
        }
    }
}

pub struct AmazonS3 {
    options: Arc<S3Options>,
    client: Arc<dyn DynHttpClient>,
}

impl Fs for AmazonS3 {
    type File = S3File;

    async fn open_options(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> Result<Self::File, crate::Error> {
        if let Some(WriteMode::Append) = options.write {
            return Err(Error::Unsupported);
        }

        Ok(S3File::new(
            self.options.clone(),
            path.clone(),
            self.client.clone(),
        ))
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

                let mut url = Url::from_str(self.options.endpoint.as_str()).map_err(|e| Error::InvalidUrl(e.into()))?;
                {
                    let mut pairs = url.query_pairs_mut();
                    let serializer = serde_urlencoded::Serializer::new(&mut pairs);
                    query
                        .serialize(serializer)
                        .map_err(|e| Error::InvalidUrl(e.into()))?;
                }

                let mut request = Request::builder()
                    .method(Method::GET)
                    .uri(url.as_str())
                    .body(Empty::<Bytes>::new())?;
                request.sign(&self.options).await?;
                let response = self.client.send_request(request).await?;

                if !response.status().is_success() {
                    yield Err(Error::HttpNotSuccess { status_code: response.status(), body: String::from_utf8_lossy(
                        &response
                        .collect()
                        .await
                        .map_err(|e| Error::Other(e.into()))?.to_bytes()).to_string()
                    });
                    return;
                }

                let mut response: ListResponse = quick_xml::de::from_reader(
                    response
                    .collect()
                    .await
                    .map_err(|e| Error::Other(e.into()))?
                    .aggregate().reader()
                ).map_err(|e| Error::Other(e.into()))?;

                println!("{:?}", response);

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

    async fn remove(&self, path: &crate::path::Path) -> Result<(), crate::Error> {
        todo!()
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
    #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
    #[tokio::test]
    async fn test_list() {
        use std::pin::pin;

        use futures_util::StreamExt;

        use super::*;
        use crate::remotes::http::tokio::TokioClient;

        if env::var("AWS_ACCESS_KEY_ID").is_err() {
            eprintln!("skipping AWS s3 test");
            return;
        }
        let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        let client = TokioClient::new();
        let s3 = AmazonS3Builder::new("fusio-test".into(), client)
            .credential(AwsCredential {
                key_id,
                secret_key,
                token: None,
            })
            .region("ap-southeast-1".into())
            // .sign_payload(true)
            .build();

        let path = Path::parse("/test/").unwrap();
        let mut stream = pin!(s3.list(&path).await.unwrap());
        while let Some(meta) = stream.next().await {
            println!("{:?}", meta);
        }
    }
}

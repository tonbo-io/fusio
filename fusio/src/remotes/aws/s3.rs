use base64::{prelude::BASE64_STANDARD, Engine};
use bytes::{Buf, Bytes};
use http::{
    header::{CONTENT_LENGTH, RANGE},
    request::Builder,
    Method, Request,
};
use http_body::Body;
use http_body_util::{BodyExt, Empty, Full};
use percent_encoding::utf8_percent_encode;
use ring::digest::{self, Context};

use super::{
    credential::{AwsAuthorizer, AwsCredential},
    STRICT_PATH_ENCODE_SET,
};
use crate::{
    buf::IoBufMut, path::Path, remotes::http::HttpClient, Error, IoBuf, Read, Seek, Write,
};

pub struct S3File<C: HttpClient> {
    bucket_endpoint: String,
    path: Path,
    credential: AwsCredential,
    region: String,
    sign_payload: bool,
    skip_signature: bool,
    checksum: bool,
    pos: u64,

    client: C,
}

impl<C: HttpClient> S3File<C> {
    fn build_request(&self, method: Method) -> Builder {
        let url = format!(
            "{}/{}",
            self.bucket_endpoint,
            utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET)
        );

        Request::builder().method(method).uri(url)
    }

    async fn checksum<B>(&self, mut request: Builder, body: &B) -> Result<Builder, Error>
    where
        B: Body<Data = Bytes> + Clone + Unpin,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        if !self.skip_signature || self.checksum {
            let mut sha256 = Context::new(&digest::SHA256);
            sha256.update(
                &body
                    .clone()
                    .collect()
                    .await
                    .map_err(|e| Error::Other(e.into()))?
                    .to_bytes(),
            );
            let payload_sha256 = sha256.finish();
            request = request.header(
                "x-amz-checksum-sha256",
                BASE64_STANDARD.encode(payload_sha256),
            );
        }
        Ok(request)
    }

    async fn authorize<B>(&self, request: Builder, body: B) -> Result<Request<B>, Error>
    where
        B: Body<Data = Bytes> + Clone + Unpin,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let request = self.checksum(request, &body).await?;

        if self.skip_signature {
            return Ok(request.body(body)?);
        }

        let authorizer = AwsAuthorizer::new(&self.credential, "s3", &self.region)
            .with_sign_payload(if self.checksum {
                false
            } else {
                self.sign_payload
            });
        let request = authorizer
            .authorize(request, body)
            .await
            .map_err(|e| Error::Other(e.into()))?;
        Ok(request)
    }
}

impl<C: HttpClient> Read for S3File<C> {
    async fn read_exact<B: IoBufMut>(&mut self, mut buf: B) -> Result<B, Error> {
        let mut request = self.build_request(Method::GET);
        request = request.header(
            RANGE,
            format!(
                "bytes={}-{}",
                self.pos,
                self.pos + buf.as_slice().len() as u64 - 1
            ),
        );

        let request = self.authorize(request, Empty::new()).await?;

        let response = self
            .client
            .send_request(request)
            .await
            .map_err(Error::Other)?;

        if !response.status().is_success() {
            Err(Error::Other(
                format!("failed to read from S3, HTTP status: {}", response.status()).into(),
            ))
        } else {
            std::io::copy(
                &mut response
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| Error::Other(e.into()))?
                    .aggregate()
                    .reader(),
                &mut buf.as_slice_mut(),
            )?;

            self.pos += buf.as_slice().len() as u64;
            Ok(buf)
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        let request = self
            .authorize(self.build_request(Method::HEAD), Empty::new())
            .await?;

        let response = self
            .client
            .send_request(request)
            .await
            .map_err(Error::Other)?;

        if !response.status().is_success() {
            Err(Error::Other(
                format!(
                    "failed to get size from S3, HTTP status: {}",
                    response.status()
                )
                .into(),
            ))
        } else {
            let size = response
                .headers()
                .get(CONTENT_LENGTH)
                .ok_or_else(|| Error::Other("missing content-length header".into()))?
                .to_str()
                .map_err(|e| Error::Other(e.into()))?
                .parse::<u64>()
                .map_err(|e| Error::Other(e.into()))?;
            Ok(size)
        }
    }
}

impl<C: HttpClient> Seek for S3File<C> {
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        self.pos = pos;
        Ok(())
    }
}

impl<C: HttpClient> Write for S3File<C> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let mut request = self.build_request(Method::PUT);
        request = request.header(CONTENT_LENGTH, buf.as_slice().len());
        let result = self.authorize(request, Full::new(buf.as_bytes())).await;
        match result {
            Ok(request) => {
                let response = self.client.send_request(request).await;
                match response {
                    Ok(response) => {
                        if response.status().is_success() {
                            self.pos += buf.as_slice().len() as u64;
                            (Ok(()), buf)
                        } else {
                            (
                                Err(Error::Other(
                                    format!(
                                        "failed to write to S3, HTTP status: {} content: {}",
                                        response.status(),
                                        String::from_utf8_lossy(
                                            &response
                                                .into_body()
                                                .collect()
                                                .await
                                                .unwrap()
                                                .to_bytes()
                                        )
                                    )
                                    .into(),
                                )),
                                buf,
                            )
                        }
                    }
                    Err(e) => (Err(Error::Other(e)), buf),
                }
            }
            Err(e) => (Err(e.into()), buf),
        }
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
    #[cfg(feature = "tokio-http")]
    #[tokio::test]
    async fn test_s3_file() {
        use std::env;

        use crate::{
            remotes::{
                aws::{credential::AwsCredential, s3::S3File},
                http::tokio::TokioClient,
            },
            Read, Seek, Write,
        };

        if env::var("AWS_ACCESS_KEY_ID").is_err() {
            eprintln!("skipping AWS s3 test");
            return;
        }
        let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        let client = TokioClient::new();
        let region = "ap-southeast-1";
        let mut s3 = S3File {
            bucket_endpoint: "https://fusio-test.s3-ap-southeast-1.amazonaws.com".into(),
            path: "test.txt".into(),
            credential: AwsCredential {
                key_id,
                secret_key,
                token: None,
            },
            region: region.into(),
            sign_payload: true,
            pos: 0,
            client,
            skip_signature: false,
            checksum: true,
        };

        let (result, _) = s3
            .write_all(&b"The answer of life, universe and everthing"[..])
            .await;
        result.unwrap();

        s3.seek(0).await.unwrap();

        let size = s3.size().await.unwrap();
        assert_eq!(size, 42);
        let buf = Vec::new();
        let buf = s3.read_to_end(buf).await.unwrap();
        assert_eq!(buf, b"The answer of life, universe and everthing");
    }
}

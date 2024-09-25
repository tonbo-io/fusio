use bytes::Buf;
use http::{header::RANGE, HeaderValue, Method, Request};
use http_body_util::BodyExt;
use percent_encoding::utf8_percent_encode;

use super::{
    credential::{AwsAuthorizer, AwsCredential},
    STRICT_PATH_ENCODE_SET,
};
use crate::{
    buf::IoBufMut,
    path::Path,
    remotes::http::{Empty, HttpClient},
    Error, Read,
};

pub struct S3File<C: HttpClient> {
    bucket_endpoint: String,
    path: Path,
    credential: AwsCredential,
    region: String,
    sign_payload: bool,
    pos: u64,

    client: C,
}

impl<C: HttpClient> S3File<C> {
    fn build_request(&self, method: Method) -> Result<Request<Empty>, Error> {
        let url = format!(
            "{}/{}",
            self.bucket_endpoint,
            utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET)
        );

        Ok(Request::builder().method(method).uri(url).body(Empty {})?)
    }

    async fn authorize(&self, request: &mut Request<Empty>) -> Result<(), Error> {
        let authorizer = AwsAuthorizer::new(&self.credential, "s3", &self.region)
            .with_sign_payload(self.sign_payload);
        authorizer
            .authorize(request, None)
            .await
            .map_err(|e| Error::Other(e.into()))?;
        Ok(())
    }
}

impl<C: HttpClient> Read for S3File<C> {
    async fn read_exact<B: IoBufMut>(&mut self, mut buf: B) -> Result<B, Error> {
        let mut request = self.build_request(Method::GET)?;
        request.headers_mut().insert(
            RANGE,
            HeaderValue::try_from(format!(
                "bytes={}-{}",
                self.pos,
                self.pos + buf.as_slice().len() as u64 - 1
            ))
            .map_err(|e| Error::Other(e.into()))?,
        );

        self.authorize(&mut request).await?;

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
            Ok(buf)
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        let mut request = self.build_request(Method::HEAD)?;

        self.authorize(&mut request).await?;

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
                .get("content-length")
                .ok_or_else(|| Error::Other("missing content-length header".into()))?
                .to_str()
                .map_err(|e| Error::Other(e.into()))?
                .parse::<u64>()
                .map_err(|e| Error::Other(e.into()))?;
            Ok(size)
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
    #[tokio::test]
    async fn test_s3_file() {
        use std::env;

        use crate::{
            remotes::{
                aws::{credential::AwsCredential, s3::S3File},
                http::tokio::TokioClient,
            },
            Read,
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
        };

        let size = s3.size().await.unwrap();
        assert_eq!(size, 12);
        let mut buf = vec![0; 12];
        let buf = s3.read_exact(&mut buf[..]).await.unwrap();
        assert_eq!(buf, b"hello, world");
    }
}

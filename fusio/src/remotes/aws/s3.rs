use http::{header::RANGE, Method, Request};
use http_body_util::BodyExt;
use percent_encoding::utf8_percent_encode;

use super::{
    credential::{AwsAuthorizer, AwsCredential},
    STRICT_PATH_ENCODE_SET,
};
use crate::{
    path::Path,
    remotes::http::{Empty, HttpClient},
    Error, IoBuf, Read,
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

impl<C: HttpClient> Read for S3File<C> {
    async fn read(&mut self, len: Option<u64>) -> Result<impl IoBuf, Error> {
        let url = format!(
            "{}/{}",
            self.bucket_endpoint,
            utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET)
        );
        let authorizer = AwsAuthorizer::new(&self.credential, "s3", &self.region)
            .with_sign_payload(self.sign_payload);

        let mut request = Request::builder()
            .method(Method::GET)
            .uri(url)
            .header(
                RANGE,
                match len {
                    Some(len) => format!("bytes={}-{}", self.pos, self.pos + len),
                    None => format!("bytes={}-", self.pos),
                },
            )
            .body(Empty {})?;

        authorizer
            .authorize(&mut request, None)
            .await
            .map_err(|e| Error::Other(e.into()))?;

        let response = self
            .client
            .send_request(request)
            .await
            .map_err(Error::Other)?;

        if response.status().is_success() {
            Ok(response
                .into_body()
                .collect()
                .await
                .map_err(|e| Error::Other(e.into()))?
                .to_bytes())
        } else {
            Err(Error::Other(
                format!("failed to read from S3, HTTP status: {}", response.status()).into(),
            ))
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        todo!()
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
            IoBuf, Read,
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

        let buf = s3.read(None).await.unwrap();
        assert_eq!(buf.as_slice(), b"hello, world");
    }
}

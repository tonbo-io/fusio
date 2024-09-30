use base64::{prelude::BASE64_STANDARD, Engine};
use bytes::Bytes;
use http::Request;
use http_body::Body;
use http_body_util::BodyExt;
use ring::digest::{self, Context};

use super::{credential::AuthorizeError, options::S3Options, CHECKSUM_HEADER};
use crate::remotes::aws::credential::AwsAuthorizer;

pub(crate) trait Sign {
    async fn checksum(&mut self, options: &S3Options) -> Result<(), AuthorizeError>;

    async fn sign(&mut self, options: &S3Options) -> Result<(), AuthorizeError>;
}

impl<B> Sign for Request<B>
where
    B: Body<Data = Bytes> + Clone + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    async fn checksum(&mut self, options: &S3Options) -> Result<(), AuthorizeError> {
        if options.credential.is_some() && options.checksum {
            let mut sha256 = Context::new(&digest::SHA256);
            sha256.update(
                &self
                    .body()
                    .clone()
                    .collect()
                    .await
                    .map_err(|e| AuthorizeError::SignHashFailed(e.into()))?
                    .to_bytes(),
            );
            let payload_sha256 = sha256.finish();
            self.headers_mut().insert(
                CHECKSUM_HEADER,
                BASE64_STANDARD.encode(payload_sha256).parse().unwrap(),
            );
        }
        Ok(())
    }

    async fn sign(&mut self, options: &S3Options) -> Result<(), AuthorizeError> {
        self.checksum(options).await?;

        let credential = if let Some(credential) = options.credential.as_ref() {
            credential
        } else {
            return Ok(());
        };

        let authorizer = AwsAuthorizer::new(credential, "s3", &options.region).with_sign_payload(
            if options.checksum {
                false
            } else {
                options.sign_payload
            },
        );
        authorizer.authorize(self).await?;

        Ok(())
    }
}

use bytes::Bytes;
use http::{HeaderName, HeaderValue, Request};
use http_body::Body;
use http_body_util::BodyExt;
use reqsign_core::{hash as reqsign_hash, ProvideCredential, SignRequest};

use super::{context::default_context, options::S3Options, CHECKSUM_HEADER};

#[derive(Debug, thiserror::Error)]
pub enum AuthorizeError {
    #[error("signing failed: {0}")]
    Signing(#[from] reqsign_core::Error),
    #[error("sign input error: {0}")]
    SignHashFailed(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

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
        if options.signer.is_none() {
            return Ok(());
        }

        if options.sign_payload || options.checksum {
            let body_bytes = self
                .body()
                .clone()
                .collect()
                .await
                .map_err(|e| AuthorizeError::SignHashFailed(e.into()))?
                .to_bytes();

            let digest_hex = reqsign_hash::hex_sha256(&body_bytes);
            if options.checksum {
                let digest_bytes = hex::decode(&digest_hex)
                    .map_err(|e| AuthorizeError::SignHashFailed(e.into()))?;
                let checksum_b64 = reqsign_hash::base64_encode(&digest_bytes);
                self.headers_mut().insert(
                    CHECKSUM_HEADER,
                    HeaderValue::from_str(&checksum_b64)
                        .map_err(|e| AuthorizeError::SignHashFailed(e.into()))?,
                );
            }

            if options.sign_payload {
                let hash_header = HeaderName::from_static("x-amz-content-sha256");
                self.headers_mut().insert(
                    hash_header,
                    HeaderValue::from_str(&digest_hex)
                        .map_err(|e| AuthorizeError::SignHashFailed(e.into()))?,
                );
            }
        }
        Ok(())
    }

    async fn sign(&mut self, options: &S3Options) -> Result<(), AuthorizeError> {
        self.checksum(options).await?;

        let Some(signer) = &options.signer else {
            if let (Some(provider), Some(region)) =
                (&options.s3_express_provider, &options.s3_express_region)
            {
                let ctx = default_context();
                let credential = provider.provide_credential(&ctx).await?;
                let (mut parts, _) = http::Request::new(()).into_parts();
                parts.method = self.method().clone();
                parts.uri = self.uri().clone();
                parts.headers = self.headers().clone();

                reqsign_aws_v4::RequestSigner::new("s3express", region)
                    .sign_request(&ctx, &mut parts, credential.as_ref(), None)
                    .await?;

                *self.method_mut() = parts.method;
                *self.uri_mut() = parts.uri;
                *self.headers_mut() = parts.headers;
            }

            return Ok(());
        };

        let (mut parts, _) = http::Request::new(()).into_parts();
        parts.method = self.method().clone();
        parts.uri = self.uri().clone();
        parts.headers = self.headers().clone();

        signer.sign(&mut parts, None).await?;

        *self.method_mut() = parts.method;
        *self.uri_mut() = parts.uri;
        *self.headers_mut() = parts.headers;

        Ok(())
    }
}

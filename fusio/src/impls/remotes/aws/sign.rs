use bytes::Bytes;
use http::{HeaderName, HeaderValue, Request};
use http_body::Body;
use http_body_util::BodyExt;
use reqsign_aws_v4::{Credential as ReqAwsCredential, RequestSigner};
use reqsign_core::{hash as reqsign_hash, Context as ReqContext, SignRequest as ReqSignRequest};

use super::{options::S3Options, CHECKSUM_HEADER};

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
        if options.credential.is_none() {
            return Ok(());
        }

        // Compute payload digest when payload signing or checksum is enabled.
        if options.sign_payload || options.checksum {
            // Collect a clone of the body without consuming the original.
            let body_bytes = self
                .body()
                .clone()
                .collect()
                .await
                .map_err(|e| AuthorizeError::SignHashFailed(e.into()))?
                .to_bytes();

            let digest_hex = reqsign_hash::hex_sha256(&body_bytes);
            // If checksum is requested, compute base64-encoded digest for S3 checksum header.
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
        // Prepare headers like checksum and content hash if needed.
        self.checksum(options).await?;

        // If no credentials are provided, skip signing.
        let Some(cred) = options.credential.as_ref() else {
            return Ok(());
        };

        // Build reqsign credential and signer.
        let req_cred = ReqAwsCredential {
            access_key_id: cred.key_id.clone(),
            secret_access_key: cred.secret_key.clone(),
            session_token: cred.token.clone(),
            expires_in: None,
        };
        let signer = RequestSigner::new("s3", &options.region);
        let ctx = ReqContext::new();

        // Clone request head into Parts for signing, then apply back.
        let (mut parts, _) = http::Request::new(()).into_parts();
        parts.method = self.method().clone();
        parts.uri = self.uri().clone();
        parts.headers = self.headers().clone();

        // Sign and apply headers/URI.
        signer
            .sign_request(&ctx, &mut parts, Some(&req_cred), None)
            .await
            .map_err(|e| AuthorizeError::SignHashFailed(e.into()))?;

        *self.method_mut() = parts.method;
        *self.uri_mut() = parts.uri;
        *self.headers_mut() = parts.headers;

        Ok(())
    }
}

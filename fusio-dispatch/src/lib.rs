use std::sync::Arc;

use fusio::{error::Error, DynFs};

#[derive(Clone)]
#[non_exhaustive]
pub enum FsOptions {
    #[cfg(any(feature = "tokio", feature = "monoio", feature = "opfs"))]
    Local,
    #[cfg(feature = "aws")]
    S3 {
        bucket: String,
        credential: Option<fusio::remotes::aws::AwsCredential>,
        endpoint: Option<String>,
        region: Option<String>,
        sign_payload: Option<bool>,
        checksum: Option<bool>,
    },
}

impl FsOptions {
    pub fn parse(self) -> Result<Arc<dyn DynFs>, Error> {
        match self {
            #[cfg(any(feature = "tokio", feature = "monoio", feature = "opfs"))]
            FsOptions::Local => Ok(Arc::new(fusio::disk::LocalFs {})),
            #[cfg(feature = "object_store")]
            FsOptions::S3 {
                bucket,
                credential,
                endpoint,
                region,
                sign_payload,
                checksum,
            } => {
                use fusio_object_store::fs::S3Store;
                use object_store::aws::AmazonS3Builder;

                let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

                if let Some(credential) = credential {
                    builder = builder
                        .with_access_key_id(credential.key_id)
                        .with_secret_access_key(credential.secret_key);

                    if let Some(token) = credential.token {
                        builder = builder.with_token(token);
                    }
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                if let Some(sign_payload) = sign_payload {
                    builder = builder.with_unsigned_payload(!sign_payload);
                }
                if matches!(checksum, Some(true)) {
                    builder = builder.with_checksum_algorithm(object_store::aws::Checksum::SHA256);
                }
                Ok(Arc::new(S3Store::from(
                    builder.build().map_err(|e| fusio::Error::Other(e.into()))?,
                )) as Arc<dyn DynFs>)
            }
            #[cfg(all(feature = "aws", not(feature = "object_store")))]
            FsOptions::S3 {
                bucket,
                credential,
                endpoint,
                region,
                sign_payload,
                checksum,
            } => {
                use fusio::remotes::aws::fs::AmazonS3Builder;

                let mut builder = AmazonS3Builder::new(bucket);

                if let Some(credential) = credential {
                    builder = builder.credential(credential);
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(region) = region {
                    builder = builder.region(region);
                }
                if let Some(sign_payload) = sign_payload {
                    builder = builder.sign_payload(sign_payload);
                }
                if let Some(checksum) = checksum {
                    builder = builder.checksum(checksum);
                }
                Ok(Arc::new(builder.build()))
            }
        }
    }
}

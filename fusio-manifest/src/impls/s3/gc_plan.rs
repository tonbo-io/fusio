use core::pin::Pin;

use bytes::Bytes;
use fusio::impls::remotes::aws::{
    fs::AmazonS3,
    head::{get_with_etag, put_if_match, put_if_none_match, ETag},
};
use fusio_core::MaybeSendFuture;

use crate::{
    gc::{GcPlan, GcPlanStore, GcTag},
    head::PutCondition,
    types::Error,
};

#[derive(Clone)]
#[allow(dead_code)] // staging
pub struct S3GcPlanStore {
    s3: AmazonS3,
    key: String,
}

#[allow(dead_code)] // staging
impl S3GcPlanStore {
    pub fn new(s3: AmazonS3, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into().trim_end_matches('/').to_string();
        let key = if prefix.is_empty() {
            "gc/GARBAGE".to_string()
        } else {
            format!("{}/gc/GARBAGE", prefix)
        };
        Self { s3, key }
    }
}

impl GcPlanStore for S3GcPlanStore {
    fn load(
        &self,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(GcPlan, GcTag)>, Error>>>> {
        let s3 = self.s3.clone();
        let key = self.key.clone();
        Box::pin(async move {
            // TODO: add bounded retries/backoff on transient errors.
            match get_with_etag(&s3, &key)
                .await
                .map_err(|e| Error::Other(Box::new(e)))?
            {
                None => Ok(None),
                Some((bytes, etag)) => {
                    let plan: GcPlan = serde_json::from_slice(&bytes)
                        .map_err(|e| Error::Corrupt(format!("invalid gc plan json: {e}")))?;
                    Ok(Some((plan, GcTag(etag.0))))
                }
            }
        })
    }

    fn put(
        &self,
        plan: &GcPlan,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<GcTag, Error>>>> {
        let s3 = self.s3.clone();
        let key = self.key.clone();
        let body = match serde_json::to_vec(plan) {
            Ok(v) => v,
            Err(e) => {
                return Box::pin(
                    async move { Err(Error::Corrupt(format!("serialize gc plan: {e}"))) },
                )
            }
        };
        Box::pin(async move {
            match cond {
                PutCondition::IfNotExists => {
                    // TODO: back off on 412/PreconditionFailed storms.
                    let etag =
                        put_if_none_match(&s3, &key, Bytes::from(body), Some("application/json"))
                            .await
                            .map_err(|e| Error::Other(Box::new(e)))?;
                    Ok(GcTag(etag.0))
                }
                PutCondition::IfMatch(tag) => {
                    // TODO: back off on 412/PreconditionFailed storms.
                    let etag = put_if_match(
                        &s3,
                        &key,
                        Bytes::from(body),
                        &ETag(tag.0),
                        Some("application/json"),
                    )
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                    Ok(GcTag(etag.0))
                }
            }
        })
    }
}

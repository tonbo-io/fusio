use core::pin::Pin;

use bytes::Bytes;
use fusio::impls::remotes::aws::{
    fs::AmazonS3,
    head::{get_with_etag, put_if_match, put_if_none_match, ETag},
};
use fusio_core::MaybeSendFuture;

use crate::head::{HeadJson, HeadStore, HeadTag, PutCondition};

#[derive(Clone)]
#[allow(dead_code)] // staging: constructed by upper layers in follow-ups
pub struct S3HeadStore {
    s3: AmazonS3,
    key: String,
}

#[allow(dead_code)] // staging
impl S3HeadStore {
    pub fn new(s3: AmazonS3, key: impl Into<String>) -> Self {
        Self {
            s3,
            key: key.into(),
        }
    }
}

impl HeadStore for S3HeadStore {
    fn load(
        &self,
    ) -> Pin<
        Box<dyn MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, crate::types::Error>>>,
    > {
        let s3 = self.s3.clone();
        let key = self.key.clone();
        Box::pin(async move {
            match get_with_etag(&s3, &key)
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?
            {
                None => Ok(None),
                Some((bytes, etag)) => {
                    let head: HeadJson = serde_json::from_slice(&bytes).map_err(|e| {
                        crate::types::Error::Corrupt(format!("invalid head json: {e}"))
                    })?;
                    Ok(Some((head, HeadTag(etag.0))))
                }
            }
        })
    }

    fn put(
        &self,
        head: &HeadJson,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<HeadTag, crate::types::Error>>>> {
        let s3 = self.s3.clone();
        let key = self.key.clone();
        let body = match serde_json::to_vec(head) {
            Ok(v) => v,
            Err(e) => {
                return Box::pin(async move {
                    Err(crate::types::Error::Corrupt(format!("serialize head: {e}")))
                })
            }
        };
        Box::pin(async move {
            match cond {
                PutCondition::IfNotExists => {
                    let etag =
                        put_if_none_match(&s3, &key, Bytes::from(body), Some("application/json"))
                            .await
                            .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                    Ok(HeadTag(etag.0))
                }
                PutCondition::IfMatch(tag) => {
                    let etag = put_if_match(
                        &s3,
                        &key,
                        Bytes::from(body),
                        &ETag(tag.0),
                        Some("application/json"),
                    )
                    .await
                    .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                    Ok(HeadTag(etag.0))
                }
            }
        })
    }
}

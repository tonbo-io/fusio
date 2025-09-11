use core::pin::Pin;
#[cfg(feature = "mem")]
use std::sync::{Arc, Mutex};

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::types::Error;

/// Opaque tag for conditional updates (e.g., S3 ETag or digest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadTag(pub String);

/// Condition for publishing HEAD.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutCondition {
    /// Only create if the object does not already exist.
    IfNotExists,
    /// Only replace if the current object's tag matches.
    IfMatch(HeadTag),
}

/// JSON-serializable HEAD structure per RFC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HeadJson {
    pub version: u32,
    pub snapshot: Option<String>,
    pub last_segment_seq: Option<u64>,
    pub last_lsn: u64,
}

/// Backend abstraction for publishing and fetching HEAD with conditional semantics.
pub trait HeadStore: MaybeSend + MaybeSync {
    fn load(
        &self,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, Error>>>>;
    fn put(
        &self,
        head: &HeadJson,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<HeadTag, Error>>>>;
}

/// Minimal in-memory head store for testing.
#[cfg(feature = "mem")]
#[derive(Debug, Default, Clone)]
pub struct MemHeadStore {
    head: Arc<Mutex<Option<(HeadJson, HeadTag)>>>,
}

#[cfg(feature = "mem")]
impl MemHeadStore {
    pub fn new() -> Self {
        Self {
            head: Arc::new(Mutex::new(None)),
        }
    }
}

#[cfg(feature = "mem")]
impl HeadStore for MemHeadStore {
    fn load(
        &self,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, Error>>>> {
        let v = self.head.lock().unwrap().clone();
        Box::pin(async move { Ok(v) })
    }

    fn put(
        &self,
        head: &HeadJson,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<HeadTag, Error>>>> {
        let mut guard = self.head.lock().unwrap();
        let res = match (&*guard, cond) {
            (None, PutCondition::IfNotExists) => {
                let tag = HeadTag(format!(
                    "v{}:{}:{}",
                    head.version,
                    head.last_lsn,
                    guard.is_some()
                ));
                *guard = Some((head.clone(), tag.clone()));
                Ok(tag)
            }
            (Some((_h, cur)), PutCondition::IfMatch(expected)) => {
                if *cur == expected {
                    let tag = HeadTag(format!("v{}:{}:{}", head.version, head.last_lsn, true));
                    *guard = Some((head.clone(), tag.clone()));
                    Ok(tag)
                } else {
                    Err(Error::PreconditionFailed)
                }
            }
            (Some(_), PutCondition::IfNotExists) => Err(Error::PreconditionFailed),
            (None, PutCondition::IfMatch(_)) => Err(Error::PreconditionFailed),
        };
        Box::pin(async move { res })
    }
}

// AWS-backed implementation placeholder under `aws-*` feature.
// Implements true CAS via If-None-Match/If-Match on PUT.
#[cfg(any(feature = "aws-tokio", feature = "aws-wasm"))]
pub mod s3 {
    use bytes::Bytes;
    use fusio::impls::remotes::aws::{
        fs::AmazonS3,
        head::{get_with_etag, put_if_match, put_if_none_match, ETag},
    };

    use super::*;

    #[derive(Clone)]
    pub struct S3HeadStore {
        s3: AmazonS3,
        key: String,
    }

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
            Box<
                dyn MaybeSendFuture<
                    Output = Result<Option<(HeadJson, HeadTag)>, crate::types::Error>,
                >,
            >,
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
                        let etag = put_if_none_match(
                            &s3,
                            &key,
                            Bytes::from(body),
                            Some("application/json"),
                        )
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
}

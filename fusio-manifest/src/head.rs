use core::pin::Pin;

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

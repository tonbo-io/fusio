use fusio::{
    fs::{CasCondition, FsCas},
    path::Path,
    Error as FsError,
};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::{checkpoint::CheckpointId, types::Error};

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
    pub checkpoint_id: Option<CheckpointId>,
    pub last_segment_seq: Option<u64>,
    pub last_txn_id: u64,
}

/// Backend abstraction for publishing and fetching HEAD with conditional semantics.
pub trait HeadStore: MaybeSend + MaybeSync {
    fn load(
        &self,
    ) -> impl MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, Error>> + '_;

    fn put(
        &self,
        head: &HeadJson,
        cond: PutCondition,
    ) -> impl MaybeSendFuture<Output = Result<HeadTag, Error>> + '_;
}

fn map_fs_error(err: FsError) -> Error {
    match err {
        FsError::PreconditionFailed => Error::PreconditionFailed,
        other => Error::Io(other),
    }
}

#[derive(Debug, Clone)]
pub struct HeadStoreImpl<C> {
    cas: C,
    key: String,
}

impl<C> HeadStoreImpl<C> {
    pub fn new(cas: C, key: impl Into<String>) -> Self {
        Self {
            cas,
            key: key.into(),
        }
    }
}

impl<C> HeadStore for HeadStoreImpl<C>
where
    C: FsCas + Clone + Send + Sync + 'static,
{
    fn load(
        &self,
    ) -> impl MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, Error>> + '_ {
        async {
            let path = Path::parse(&self.key).map_err(Error::other)?;
            match self.cas.load_with_tag(&path).await.map_err(map_fs_error)? {
                None => Ok(None),
                Some((bytes, tag)) => {
                    let head: HeadJson = serde_json::from_slice(&bytes)
                        .map_err(|e| Error::Corrupt(format!("invalid head json: {e}")))?;
                    Ok(Some((head, HeadTag(tag))))
                }
            }
        }
    }

    fn put(
        &self,
        head: &HeadJson,
        cond: PutCondition,
    ) -> impl MaybeSendFuture<Output = Result<HeadTag, Error>> + '_ {
        let txn_id = head.last_txn_id;
        let body =
            serde_json::to_vec(head).map_err(|e| Error::Corrupt(format!("serialize head: {e}")));
        async move {
            tracing::debug!(
                txn_id = %txn_id,
                condition = ?cond,
                "putting HEAD with condition"
            );

            let body = body?;
            let path = Path::parse(&self.key).map_err(Error::other)?;
            let condition = match cond {
                PutCondition::IfNotExists => CasCondition::IfNotExists,
                PutCondition::IfMatch(tag) => CasCondition::IfMatch(tag.0.clone()),
            };
            let result = self
                .cas
                .put_conditional(&path, &body, Some("application/json"), None, condition)
                .await
                .map_err(map_fs_error);

            match &result {
                Ok(tag) => tracing::info!(
                    txn_id = %txn_id,
                    tag = %tag,
                    "HEAD updated successfully"
                ),
                Err(_) => tracing::warn!(
                    txn_id = %txn_id,
                    "HEAD CAS conflict"
                ),
            }

            result.map(HeadTag)
        }
    }
}

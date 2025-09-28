//! GC plan object and CAS operations
use core::time::Duration;
use std::sync::Arc;

use fusio::{
    fs::{CasCondition, FsCas},
    path::Path,
    Error as FsError,
};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::{
    backoff::{classify_error, ExponentialBackoff, RetryClass},
    head::PutCondition,
    types::Error,
    BackoffPolicy,
};

/// Minimal GC plan representation stored at `gc/GARBAGE` under a collection prefix.
///
/// This is a stub to guide implementation. Fields and names mirror the current design and are
/// intentionally backend-agnostic. Storage/CAS functions will live beside this type
/// behind feature gates for specific backends.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcPlan {
    /// ETag/tag of the HEAD manifest the plan was computed against.
    pub(crate) against_head_tag: Option<String>,
    /// Not-before timestamp for deletes.
    #[serde(with = "duration_millis")]
    pub(crate) not_before: Duration,
    /// Segment ranges to delete (inclusive).
    pub(crate) delete_segments: Vec<SegmentRange>,
    /// Checkpoint object keys to delete.
    pub(crate) delete_checkpoints: Vec<String>,
    /// Checkpoints to materialize before dropping segments (future use).
    pub(crate) make_checkpoints: Vec<CheckpointSpec>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointSpec {
    pub(crate) name: String,
    pub(crate) upto_lsn: u64,
}

impl GcPlan {
    #[allow(dead_code)] // staging: will be used by GC phases
    pub fn is_empty(&self) -> bool {
        self.delete_segments.is_empty()
            && self.delete_checkpoints.is_empty()
            && self.make_checkpoints.is_empty()
    }
}

/// Inclusive segment range slated for deletion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl SegmentRange {
    pub fn new(start: u64, end: u64) -> Self {
        if start <= end {
            Self { start, end }
        } else {
            Self {
                start: end,
                end: start,
            }
        }
    }
}

/// Opaque tag for conditional updates to the GC plan (e.g., S3 ETag or digest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcTag(pub String);

/// Backend abstraction for publishing and fetching the GC plan with conditional semantics.
#[allow(dead_code)] // staging: wired by GC coordinator in follow-ups
pub trait GcPlanStore: MaybeSend + MaybeSync {
    fn load(&self) -> impl MaybeSendFuture<Output = Result<Option<(GcPlan, GcTag)>, Error>> + '_;

    fn put(
        &self,
        plan: &GcPlan,
        cond: PutCondition,
    ) -> impl MaybeSendFuture<Output = Result<GcTag, Error>> + '_;
}

#[derive(Clone)]
pub struct FsGcPlanStore<C> {
    cas: C,
    key: String,
    backoff: BackoffPolicy,
    timer: Arc<dyn fusio::executor::Timer + Send + Sync>,
}

impl<C> FsGcPlanStore<C> {
    pub fn new(
        cas: C,
        prefix: impl Into<String>,
        backoff: BackoffPolicy,
        timer: Arc<dyn fusio::executor::Timer + Send + Sync>,
    ) -> Self {
        let prefix = prefix.into().trim_end_matches('/').to_string();
        let key = if prefix.is_empty() {
            "gc/GARBAGE".to_string()
        } else {
            format!("{}/gc/GARBAGE", prefix)
        };
        Self {
            cas,
            key,
            backoff,
            timer,
        }
    }
}

impl<C> GcPlanStore for FsGcPlanStore<C>
where
    C: FsCas + Clone + Send + Sync + 'static,
{
    fn load(&self) -> impl MaybeSendFuture<Output = Result<Option<(GcPlan, GcTag)>, Error>> + '_ {
        async move {
            let path = Path::parse(&self.key).map_err(|e| Error::Other(Box::new(e)))?;
            let mut bo = ExponentialBackoff::new(self.backoff, self.timer.clone());
            loop {
                match self.cas.load_with_tag(&path).await.map_err(map_fs_error) {
                    Ok(None) => return Ok(None),
                    Ok(Some((bytes, etag))) => {
                        let plan: GcPlan = serde_json::from_slice(&bytes)
                            .map_err(|e| Error::Corrupt(format!("invalid gc plan json: {e}")))?;
                        return Ok(Some((plan, GcTag(etag))));
                    }
                    Err(err) => match classify_error(&err) {
                        RetryClass::RetryTransient if !bo.exhausted() => {
                            let delay = bo.next_delay();
                            self.timer.sleep(delay).await;
                            continue;
                        }
                        _ => return Err(err),
                    },
                }
            }
        }
    }

    fn put(
        &self,
        plan: &GcPlan,
        cond: PutCondition,
    ) -> impl MaybeSendFuture<Output = Result<GcTag, Error>> + '_ {
        let body =
            serde_json::to_vec(plan).map_err(|e| Error::Corrupt(format!("serialize gc plan: {e}")));
        async move {
            let body = body?;
            let path = Path::parse(&self.key).map_err(|e| Error::Other(Box::new(e)))?;
            let mut bo = ExponentialBackoff::new(self.backoff, self.timer.clone());
            loop {
                let condition = match cond {
                    PutCondition::IfNotExists => CasCondition::IfNotExists,
                    PutCondition::IfMatch(ref tag) => CasCondition::IfMatch(tag.0.clone()),
                };
                match self
                    .cas
                    .put_conditional(&path, &body, Some("application/json"), None, condition)
                    .await
                    .map_err(map_fs_error)
                {
                    Ok(etag) => return Ok(GcTag(etag)),
                    Err(err) => match classify_error(&err) {
                        RetryClass::RetryTransient if !bo.exhausted() => {
                            let delay = bo.next_delay();
                            self.timer.sleep(delay).await;
                            continue;
                        }
                        _ => return Err(err),
                    },
                }
            }
        }
    }
}

fn map_fs_error(err: FsError) -> Error {
    match err {
        FsError::PreconditionFailed => Error::PreconditionFailed,
        other => Error::Other(Box::new(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::new_inmemory_gc_plan_store;

    #[test]
    fn empty_by_default() {
        let p = GcPlan::default();
        assert!(p.is_empty());
        let j = serde_json::to_string(&p).unwrap();
        let _ = serde_json::from_str::<GcPlan>(&j).unwrap();
    }

    #[test]
    fn mem_gc_plan_store_semantics() {
        use futures_executor::block_on;
        let store = new_inmemory_gc_plan_store();
        // load none
        assert!(block_on(store.load()).unwrap().is_none());
        // if-not-exists succeeds
        let tag1 = block_on(store.put(&GcPlan::default(), PutCondition::IfNotExists)).unwrap();
        let got = block_on(store.load()).unwrap().unwrap();
        assert_eq!(got.1, tag1);
        // if-not-exists fails when exists
        assert!(matches!(
            block_on(store.put(&GcPlan::default(), PutCondition::IfNotExists)),
            Err(Error::PreconditionFailed)
        ));
        // if-match with wrong tag fails
        assert!(matches!(
            block_on(store.put(
                &GcPlan {
                    not_before: Duration::from_millis(1),
                    ..Default::default()
                },
                PutCondition::IfMatch(crate::head::HeadTag("bad".into()))
            )),
            Err(Error::PreconditionFailed)
        ));
        // if-match with correct tag succeeds and rotates tag
        let tag2 = block_on(store.put(
            &GcPlan {
                not_before: Duration::from_millis(2),
                ..Default::default()
            },
            PutCondition::IfMatch(crate::head::HeadTag(tag1.0.clone())),
        ))
        .unwrap();
        let got2 = block_on(store.load()).unwrap().unwrap();
        assert_eq!(got2.1, tag2);
        assert_ne!(tag1, tag2);
    }
}

mod duration_millis {
    use core::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(dur: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ms = dur.as_millis().min(u128::from(u64::MAX)) as u64;
        serializer.serialize_u64(ms)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ms = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(ms))
    }
}

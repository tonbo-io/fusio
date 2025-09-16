//! GC plan object and CAS operations
use core::pin::Pin;

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::{head::PutCondition, types::Error};

/// Minimal GC plan representation stored at `gc/GARBAGE` under a collection prefix.
///
/// This is a stub to guide implementation. Fields and names mirror the current design and are
/// intentionally backend-agnostic. Storage/CAS functions will live beside this type
/// behind feature gates for specific backends.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcPlan {
    /// ETag/tag of the HEAD manifest the plan was computed against.
    pub against_head_tag: Option<String>,
    /// Not-before timestamp (ms since UNIX epoch) for phase-3 deletes.
    pub not_before_ms: u64,
    /// Segment sequence numbers to delete (may be contiguous; expanded by executor).
    pub delete_segments: Vec<u64>,
    /// Checkpoint object keys to delete.
    pub delete_checkpoints: Vec<String>,
    /// Checkpoints to materialize before dropping segments (future use).
    pub make_checkpoints: Vec<CheckpointSpec>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointSpec {
    pub name: String,
    pub upto_lsn: u64,
}

impl GcPlan {
    #[allow(dead_code)] // staging: will be used by GC phases
    pub fn is_empty(&self) -> bool {
        self.delete_segments.is_empty()
            && self.delete_checkpoints.is_empty()
            && self.make_checkpoints.is_empty()
    }
}

/// Opaque tag for conditional updates to the GC plan (e.g., S3 ETag or digest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcTag(pub String);

/// Backend abstraction for publishing and fetching the GC plan with conditional semantics.
#[allow(dead_code)] // staging: wired by GC coordinator in follow-ups
pub trait GcPlanStore: MaybeSend + MaybeSync {
    fn load(
        &self,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(GcPlan, GcTag)>, Error>>>>;

    fn put(
        &self,
        plan: &GcPlan,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<GcTag, Error>>>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impls::mem::gc_plan::MemGcPlanStore;

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
        let store = MemGcPlanStore::new();
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
                    not_before_ms: 1,
                    ..Default::default()
                },
                PutCondition::IfMatch(crate::head::HeadTag("bad".into()))
            )),
            Err(Error::PreconditionFailed)
        ));
        // if-match with correct tag succeeds and rotates tag
        let tag2 = block_on(store.put(
            &GcPlan {
                not_before_ms: 2,
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

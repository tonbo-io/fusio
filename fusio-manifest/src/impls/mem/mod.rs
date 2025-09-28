#![cfg(test)]

pub mod checkpoint;
pub mod gc_plan;
pub mod head;
pub mod lease;
pub mod segment;

use std::{hash::Hash, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};

use crate::{context::ManifestContext, manifest::Manifest, BlockingExecutor};

/// Test-only type alias for a Manifest backed by in-memory stores via the shared Fs implementation.
pub type MemManifest<K, V> = Manifest<
    K,
    V,
    head::MemHeadStore,
    segment::MemSegmentStore,
    checkpoint::MemCheckpointStore,
    lease::MemLeaseStore,
>;

/// Construct an in-memory manifest with a custom context.
pub fn new_manifest_with_context<K, V, C>(opts: C) -> MemManifest<K, V>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    C: Into<Arc<ManifestContext<BlockingExecutor>>>,
{
    Manifest::new_with_context(
        head::MemHeadStore::new(),
        segment::MemSegmentStore::new(),
        checkpoint::MemCheckpointStore::new(),
        lease::MemLeaseStore::new(),
        opts.into(),
    )
}

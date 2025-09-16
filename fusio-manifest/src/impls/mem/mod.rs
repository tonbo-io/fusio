#![cfg(test)]

pub mod checkpoint;
pub mod gc_plan;
pub mod head;
pub mod lease;
pub mod segment;

use crate::{db::Manifest, options::Options};

/// Test-only type alias for a Manifest backed by in-memory stores.
pub type MemManifest<K, V> = Manifest<
    K,
    V,
    head::MemHeadStore,
    segment::MemSegmentStore,
    checkpoint::MemCheckpointStore,
    lease::MemLeaseStore,
>;

/// Construct an in-memory Manifest with custom Options.
pub fn new_manifest_with_opts<K, V>(opts: Options) -> MemManifest<K, V> {
    Manifest::new_with_opts(
        head::MemHeadStore::new(),
        segment::MemSegmentStore::new(),
        checkpoint::MemCheckpointStore::new(),
        lease::MemLeaseStore::new(),
        opts,
    )
}

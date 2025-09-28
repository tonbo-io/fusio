#![cfg(test)]

use std::{hash::Hash, sync::Arc};

use fusio::{
    executor::{BlockingExecutor, Timer},
    impls::mem::fs::InMemoryFs,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    backoff::BackoffPolicy, checkpoint::FsCheckpointStore, context::ManifestContext,
    gc::FsGcPlanStore, head::FsHeadStore, lease::FsLeaseStore, manifest::Manifest,
    segment::FsSegmentStore, BlockingExecutor as ManifestBlockingExecutor,
};

fn timer() -> Arc<dyn Timer + Send + Sync> {
    Arc::new(BlockingExecutor::default())
}

pub(crate) fn new_inmemory_stores() -> (
    FsHeadStore<InMemoryFs>,
    FsSegmentStore<InMemoryFs>,
    FsCheckpointStore<InMemoryFs>,
    FsLeaseStore<InMemoryFs>,
) {
    let fs = InMemoryFs::new();
    let head = FsHeadStore::new(fs.clone(), "HEAD.json");
    let segment = FsSegmentStore::new(fs.clone(), "segments");
    let checkpoint = FsCheckpointStore::new(fs.clone(), "");
    let lease = FsLeaseStore::new(fs, "", BackoffPolicy::default(), timer());
    (head, segment, checkpoint, lease)
}

pub(crate) fn new_inmemory_gc_plan_store() -> FsGcPlanStore<InMemoryFs> {
    FsGcPlanStore::new(InMemoryFs::new(), "", BackoffPolicy::default(), timer())
}

pub(crate) fn new_inmemory_manifest_with_context<K, V, C>(
    opts: C,
) -> Manifest<
    K,
    V,
    FsHeadStore<InMemoryFs>,
    FsSegmentStore<InMemoryFs>,
    FsCheckpointStore<InMemoryFs>,
    FsLeaseStore<InMemoryFs>,
    ManifestBlockingExecutor,
>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    C: Into<Arc<ManifestContext<ManifestBlockingExecutor>>>,
{
    let (head, segment, checkpoint, lease) = new_inmemory_stores();
    Manifest::new_with_context(head, segment, checkpoint, lease, opts.into())
}

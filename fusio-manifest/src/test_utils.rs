#![cfg(test)]

use std::sync::Arc;

use fusio::{
    executor::{BlockingExecutor, NoopExecutor},
    impls::mem::fs::InMemoryFs,
};
use rstest::fixture;

use crate::{
    backoff::BackoffPolicy, checkpoint::CheckpointStoreImpl, head::HeadStoreImpl,
    lease::LeaseStoreImpl, manifest::Manifest, retention::DefaultRetention,
    segment::SegmentStoreImpl, ManifestContext,
};

#[derive(Clone)]
pub(crate) struct InMemoryStores {
    pub head: HeadStoreImpl<InMemoryFs>,
    pub segment: SegmentStoreImpl<InMemoryFs>,
    pub checkpoint: CheckpointStoreImpl<InMemoryFs>,
    pub lease: LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
}

#[fixture]
pub(crate) fn in_memory_stores() -> InMemoryStores {
    let fs = InMemoryFs::new();
    let head = HeadStoreImpl::new(fs.clone(), "HEAD.json");
    let segment = SegmentStoreImpl::new(fs.clone(), "segments");
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
    let timer = BlockingExecutor::default();
    let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), timer);
    InMemoryStores {
        head,
        segment,
        checkpoint,
        lease,
    }
}

#[fixture]
pub(crate) fn string_in_memory_manifest(
    in_memory_stores: InMemoryStores,
) -> Manifest<
    String,
    String,
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
    NoopExecutor,
> {
    let opts: ManifestContext<DefaultRetention, NoopExecutor> =
        ManifestContext::new(NoopExecutor::default());
    let shared = Arc::new(opts);
    crate::manifest::Manifest::new_with_context(
        in_memory_stores.head.clone(),
        in_memory_stores.segment.clone(),
        in_memory_stores.checkpoint.clone(),
        in_memory_stores.lease.clone(),
        Arc::clone(&shared),
    )
}

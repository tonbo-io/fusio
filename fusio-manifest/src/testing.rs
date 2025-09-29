#![cfg(test)]

use std::sync::Arc;

use fusio::{
    executor::{BlockingExecutor, Timer},
    impls::mem::fs::InMemoryFs,
};

use crate::{
    backoff::BackoffPolicy, checkpoint::CheckpointStoreImpl, head::HeadStoreImpl,
    lease::LeaseStoreImpl, segment::SegmentStoreImpl,
};

pub(crate) fn new_inmemory_stores() -> (
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs>,
) {
    let fs = InMemoryFs::new();
    let head = HeadStoreImpl::new(fs.clone(), "HEAD.json");
    let segment = SegmentStoreImpl::new(fs.clone(), "segments");
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
    let timer: Arc<dyn Timer + Send + Sync> = Arc::new(BlockingExecutor::default());
    let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), timer);
    (head, segment, checkpoint, lease)
}

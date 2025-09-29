#![cfg(test)]

use fusio::{executor::BlockingExecutor, impls::mem::fs::InMemoryFs};

use crate::{
    backoff::BackoffPolicy, checkpoint::CheckpointStoreImpl, head::HeadStoreImpl,
    lease::LeaseStoreImpl, segment::SegmentStoreImpl,
};

pub(crate) fn new_inmemory_stores() -> (
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
) {
    let fs = InMemoryFs::new();
    let head = HeadStoreImpl::new(fs.clone(), "HEAD.json");
    let segment = SegmentStoreImpl::new(fs.clone(), "segments");
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
    let timer = BlockingExecutor::default();
    let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), timer);
    (head, segment, checkpoint, lease)
}

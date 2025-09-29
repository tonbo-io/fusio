#![cfg(test)]

use std::{hash::Hash, sync::Arc};

use fusio::{
    executor::{BlockingExecutor, Timer},
    impls::mem::fs::InMemoryFs,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    backoff::BackoffPolicy, checkpoint::FsCheckpointStore, gc::FsGcPlanStore, head::FsHeadStore,
    lease::FsLeaseStore, segment::FsSegmentStore,
};

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
    let timer: Arc<dyn Timer + Send + Sync> = Arc::new(BlockingExecutor::default());
    let lease = FsLeaseStore::new(fs, "", BackoffPolicy::default(), timer);
    (head, segment, checkpoint, lease)
}

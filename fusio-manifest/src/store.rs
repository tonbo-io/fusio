use std::sync::Arc;

use fusio::executor::{Executor, Timer};

use crate::{
    cache::{CachedCheckpointStore, CachedSegmentStore},
    context::ManifestContext,
    retention::{DefaultRetention, RetentionPolicy},
    BlockingExecutor,
};

/// Shared for head/segment/checkpoint/lease stores plus runtime context.
pub(crate) struct Store<HS, SS, CS, LS, E = BlockingExecutor, R = DefaultRetention>
where
    E: Executor + Timer + Clone + 'static,
    R: RetentionPolicy + Clone,
{
    pub(crate) head: HS,
    pub(crate) segment: CachedSegmentStore<SS>,
    pub(crate) checkpoint: CachedCheckpointStore<CS>,
    pub(crate) leases: LS,
    pub(crate) opts: Arc<ManifestContext<R, E>>,
}

impl<HS, SS, CS, LS, E, R> Store<HS, SS, CS, LS, E, R>
where
    E: Executor + Timer + Clone + 'static,
    R: RetentionPolicy + Clone,
{
    pub(crate) fn new(
        head: HS,
        segment: SS,
        checkpoint: CS,
        leases: LS,
        opts: Arc<ManifestContext<R, E>>,
    ) -> Self {
        let cache = opts.cache.clone();
        let namespace = opts.cache_namespace.clone();
        let segment = CachedSegmentStore::new(segment, cache.clone(), namespace.clone());
        let checkpoint = CachedCheckpointStore::new(checkpoint, cache, namespace);
        Self {
            head,
            segment,
            checkpoint,
            leases,
            opts,
        }
    }
}

use std::sync::Arc;

use fusio::executor::{Executor, Timer};

use crate::{
    context::ManifestContext,
    retention::{DefaultRetention, RetentionPolicy},
    BlockingExecutor,
};

/// Shared for head/segment/checkpoint/lease stores plus runtime context.
pub(crate) struct Store<HS, SS, CS, LS, E = BlockingExecutor, R = DefaultRetention>
where
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    pub(crate) head: HS,
    pub(crate) segment: SS,
    pub(crate) checkpoint: CS,
    pub(crate) leases: LS,
    pub(crate) opts: Arc<ManifestContext<R, E>>,
}

impl<HS, SS, CS, LS, E, R> Store<HS, SS, CS, LS, E, R>
where
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    pub(crate) fn new(
        head: HS,
        segment: SS,
        checkpoint: CS,
        leases: LS,
        opts: Arc<ManifestContext<R, E>>,
    ) -> Self {
        Self {
            head,
            segment,
            checkpoint,
            leases,
            opts,
        }
    }
}

use std::sync::Arc;

use fusio::executor::{Executor, Timer};

use crate::{context::ManifestContext, BlockingExecutor};

/// Shared for head/segment/checkpoint/lease stores plus runtime context.
pub(crate) struct Store<HS, SS, CS, LS, E = BlockingExecutor>
where
    E: Executor + Timer + Send + Sync + 'static,
{
    pub(crate) head: HS,
    pub(crate) segment: SS,
    pub(crate) checkpoint: CS,
    pub(crate) leases: LS,
    pub(crate) opts: Arc<ManifestContext<E>>,
}

impl<HS, SS, CS, LS, E> Store<HS, SS, CS, LS, E>
where
    E: Executor + Timer + Send + Sync + 'static,
{
    pub(crate) fn new(
        head: HS,
        segment: SS,
        checkpoint: CS,
        leases: LS,
        opts: Arc<ManifestContext<E>>,
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

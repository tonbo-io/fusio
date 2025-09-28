use std::{future::Future, sync::Arc};

use fusio::executor::{BlockingExecutor, Executor, Timer};
use fusio_core::MaybeSend;

use crate::{
    backoff::BackoffPolicy,
    retention::{DefaultRetention, RetentionHandle},
    types::Error,
};

/// ManifestContext shared across manifest components, parameterised by an executor that also
/// implements the timer abstraction.
pub struct ManifestContext<E = BlockingExecutor>
where
    E: Executor + Timer + Send + Sync + 'static,
{
    /// Retention/GC policy.
    pub retention: RetentionHandle,
    /// Backoff policy for CAS/storage contention.
    pub backoff: BackoffPolicy,
    executor: Arc<E>,
}

impl<E> ManifestContext<E>
where
    E: Executor + Timer + Send + Sync + 'static,
{
    /// Construct a context using the provided executor.
    pub fn new(executor: Arc<E>) -> Self {
        Self {
            retention: Arc::new(DefaultRetention::default()),
            backoff: BackoffPolicy::default(),
            executor,
        }
    }

    /// Borrow the executor handle.
    pub fn executor(&self) -> &Arc<E> {
        &self.executor
    }

    /// Obtain the executor as a timer trait object.
    pub fn timer(&self) -> Arc<dyn Timer + Send + Sync> {
        self.executor.clone()
    }

    /// Spawn a detached background task.
    pub fn spawn_task<F, T>(&self, task: F) -> Result<(), Error>
    where
        F: Future<Output = T> + MaybeSend + 'static,
        T: MaybeSend + 'static,
    {
        let _ = self.executor.spawn(task);
        Ok(())
    }

    /// Replace the executor handle in-place.
    pub fn set_executor(&mut self, executor: Arc<E>) {
        self.executor = executor;
    }

    /// Replace the executor, returning the modified context.
    pub fn with_executor(mut self, executor: Arc<E>) -> Self {
        self.executor = executor;
        self
    }

    /// Override the retry/backoff policy used by publish/GC loops.
    pub fn with_backoff(mut self, backoff: BackoffPolicy) -> Self {
        self.backoff = backoff;
        self
    }

    /// Mutably set the retry/backoff policy.
    pub fn set_backoff(&mut self, backoff: BackoffPolicy) {
        self.backoff = backoff;
    }

    /// Override the retention policy used by GC/lease management.
    pub fn with_retention(mut self, retention: RetentionHandle) -> Self {
        self.retention = retention;
        self
    }

    pub fn set_retention(&mut self, retention: RetentionHandle) {
        self.retention = retention;
    }
}

impl<E> Default for ManifestContext<E>
where
    E: Executor + Timer + Send + Sync + Default + 'static,
{
    fn default() -> Self {
        Self::new(Arc::new(E::default()))
    }
}

impl<E> Clone for ManifestContext<E>
where
    E: Executor + Timer + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            retention: self.retention.clone(),
            backoff: self.backoff,
            executor: Arc::clone(&self.executor),
        }
    }
}

impl<E> core::fmt::Debug for ManifestContext<E>
where
    E: Executor + Timer + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ManifestContext")
            .field("retention", &"...")
            .field("backoff", &"...")
            .finish_non_exhaustive()
    }
}

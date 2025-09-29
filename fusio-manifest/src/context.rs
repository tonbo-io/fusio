use std::future::Future;

use fusio::executor::{BlockingExecutor, Executor, Timer};
use fusio_core::MaybeSend;

use crate::{
    backoff::BackoffPolicy,
    retention::{DefaultRetention, RetentionPolicy},
    types::Error,
};

/// ManifestContext shared across manifest components, parameterised by an executor that also
/// implements the timer abstraction.
#[derive(Clone)]
pub struct ManifestContext<R = DefaultRetention, E = BlockingExecutor>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone,
{
    /// Retention/GC policy.
    pub retention: R,
    /// Backoff policy for CAS/storage contention.
    pub backoff: BackoffPolicy,
    executor: E,
}

impl<E> ManifestContext<DefaultRetention, E>
where
    E: Executor + Timer + Clone,
{
    /// Construct a context using the provided executor.
    pub fn new(executor: E) -> Self {
        Self {
            retention: DefaultRetention::default(),
            backoff: BackoffPolicy::default(),
            executor,
        }
    }
}

impl<R, E> ManifestContext<R, E>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone,
{
    /// Borrow the executor handle.
    pub fn executor(&self) -> &E {
        &self.executor
    }

    /// Obtain the executor as a timer trait object.
    pub fn timer(&self) -> &E {
        &self.executor
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

    /// Replace the executor, returning the modified context.
    pub fn with_executor(mut self, executor: E) -> Self {
        self.executor = executor;
        self
    }

    /// Mutably replace the executor in-place.
    pub fn set_executor(&mut self, executor: E) {
        self.executor = executor;
    }

    /// Override the retry/backoff policy used by publish/GC loops.
    pub fn with_backoff(mut self, backoff: BackoffPolicy) -> Self {
        self.backoff = backoff;
        self
    }

    /// Mutably replace the retry/backoff policy.
    pub fn set_backoff(&mut self, backoff: BackoffPolicy) {
        self.backoff = backoff;
    }

    /// Replace the retention policy, consuming `self` and allowing a new policy type.
    pub fn with_retention<R2>(self, retention: R2) -> ManifestContext<R2, E>
    where
        R2: RetentionPolicy + Clone,
    {
        ManifestContext {
            retention,
            backoff: self.backoff,
            executor: self.executor,
        }
    }

    /// Mutably replace the retention policy in-place when keeping the same type.
    pub fn set_retention(&mut self, retention: R) {
        self.retention = retention;
    }
}

impl<E> Default for ManifestContext<DefaultRetention, E>
where
    E: Executor + Timer + Clone + Default,
{
    fn default() -> Self {
        Self::new(E::default())
    }
}

impl<R, E> core::fmt::Debug for ManifestContext<R, E>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ManifestContext")
            .field("retention", &"...")
            .field("backoff", &"...")
            .finish_non_exhaustive()
    }
}

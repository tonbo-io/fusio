use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use fusio::executor::BlockingSleeper;

use crate::{
    backoff::TimerHandle,
    retention::{DefaultRetention, RetentionHandle},
};

fn default_timer() -> TimerHandle {
    #[cfg(not(target_arch = "wasm32"))]
    {
        Arc::new(BlockingSleeper)
    }
    #[cfg(target_arch = "wasm32")]
    {
        panic!(
            "Options::default() on wasm requires an executor timer; use \
             Options::default().with_timer(...)"
        )
    }
}

/// Manifest options.
#[derive(Clone)]
pub struct Options {
    /// Retention/GC policy.
    pub retention: RetentionHandle,
    /// Backoff policy for CAS/storage contention.
    pub backoff: crate::backoff::BackoffPolicy,
    /// Runtime-provided timer for non-blocking backoff and clock access.
    pub timer: TimerHandle,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            retention: Arc::new(DefaultRetention::default()),
            backoff: crate::backoff::BackoffPolicy::default(),
            timer: default_timer(),
        }
    }
}

impl core::fmt::Debug for Options {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut ds = f.debug_struct("Options");
        ds.field("retention", &"...")
            .field("backoff", &"...")
            .field("timer", &"...");
        ds.finish()
    }
}

impl Options {
    /// Override the retry/backoff policy used by publish/GC loops.
    pub fn with_backoff(mut self, backoff: crate::backoff::BackoffPolicy) -> Self {
        self.backoff = backoff;
        self
    }

    /// Mutably set the retry/backoff policy.
    pub fn set_backoff(&mut self, backoff: crate::backoff::BackoffPolicy) {
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

    /// Provide a runtime timer used for async-friendly backoff and timekeeping.
    pub fn with_timer(mut self, timer: TimerHandle) -> Self {
        self.timer = timer;
        self
    }

    pub fn set_timer(&mut self, timer: TimerHandle) {
        self.timer = timer;
    }
}

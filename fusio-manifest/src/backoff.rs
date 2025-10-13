use core::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder};

use crate::types::Error;

/// Classification used to guide retry logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    DurableConflict,
    RetryTransient,
    Fatal,
}

/// Map crate-local errors into retry classes.
pub fn classify_error(err: &Error) -> RetryClass {
    match err {
        Error::PreconditionFailed => RetryClass::DurableConflict,
        Error::Unimplemented(_) | Error::Corrupt(_) => RetryClass::Fatal,
        Error::Io(_) | Error::Other(_) => RetryClass::RetryTransient,
    }
}

/// Backoff policy used to construct an exponential backoff iterator.
#[derive(Debug, Clone, Copy)]
pub struct BackoffPolicy {
    pub base_ms: u64,
    pub max_ms: u64,
    pub multiplier_times_100: u64, // store as integer: e.g., 200 for 2.0
    pub jitter_frac_times_100: u64, // e.g., 25 for 0.25
    pub max_retries: u32,

    /// Maximum wall-clock time for an entire operation (in milliseconds).
    /// Includes both operation execution time and backoff sleep time.
    /// Used for operation-level timeouts on user-facing calls (e.g., session.commit).
    /// Set to 0 to disable this limit.
    pub max_elapsed_ms: u64,

    /// Maximum cumulative time to spend sleeping during backoff (in milliseconds).
    /// Only counts the sleep/wait time between retries, NOT the time spent executing operations.
    /// Used by the backon backoff iterator to limit retry delay accumulation.
    /// Set to 0 to disable this limit (only max_retries will apply).
    pub max_backoff_sleep_ms: u64,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base_ms: 20,
            max_ms: 1000,
            multiplier_times_100: 200, // 2.0x
            jitter_frac_times_100: 25, // 0.25
            max_retries: 6,
            max_elapsed_ms: 3000,
            max_backoff_sleep_ms: 3000,
        }
    }
}

impl BackoffPolicy {
    pub fn build_backoff(&self) -> impl Iterator<Item = Duration> {
        let mut strategy = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(self.base_ms))
            .with_max_delay(Duration::from_millis(self.max_ms))
            .with_factor(self.multiplier_times_100 as f32 / 100.0)
            .with_max_times(self.max_retries as usize);

        if self.jitter_frac_times_100 > 0 {
            strategy = strategy.with_jitter();
        }

        if self.max_backoff_sleep_ms > 0 {
            strategy =
                strategy.with_total_delay(Some(Duration::from_millis(self.max_backoff_sleep_ms)));
        }

        strategy.build()
    }
}

use core::time::Duration;
use std::sync::Arc;

use fusio::executor::Timer;

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
        Error::Other(_) => RetryClass::RetryTransient,
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
    pub max_elapsed_ms: u64,
}

impl BackoffPolicy {
    pub const fn default() -> Self {
        Self {
            base_ms: 20,
            max_ms: 1000,
            multiplier_times_100: 200, // 2.0x
            jitter_frac_times_100: 25, // 0.25
            max_retries: 6,
            max_elapsed_ms: 3000,
        }
    }

    /// Disable retries entirely. Loops that consult `exhausted()` will give up
    /// immediately and never sleep.
    pub const fn disabled() -> Self {
        Self {
            base_ms: 0,
            max_ms: 0,
            multiplier_times_100: 100, // 1.0x — unused but keeps math well-defined
            jitter_frac_times_100: 0,
            max_retries: 0,
            max_elapsed_ms: 0,
        }
    }
}

/// Runtime-agnostic exponential backoff with jitter.
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    pol: BackoffPolicy,
    attempt: u32,
    start: std::time::Instant,
    rng: u64,
}

impl ExponentialBackoff {
    pub fn new(pol: BackoffPolicy) -> Self {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            pol,
            attempt: 0,
            start: std::time::Instant::now(),
            rng: seed ^ 0x9e3779b97f4a7c15,
        }
    }

    #[inline]
    pub fn exhausted(&self) -> bool {
        self.attempt >= self.pol.max_retries
            || self.start.elapsed() >= Duration::from_millis(self.pol.max_elapsed_ms)
    }

    /// Compute the next delay and advance the attempt counter.
    pub fn next_delay(&mut self) -> Duration {
        let a = self.attempt as u64;
        self.attempt = self.attempt.saturating_add(1);
        // multiplier^a with integer math in x100 fixed point
        let mut mult_scaled = 100u64; // 1.00
        for _ in 0..a {
            mult_scaled = mult_scaled
                .saturating_mul(self.pol.multiplier_times_100)
                .saturating_div(100);
        }
        let base = self
            .pol
            .base_ms
            .saturating_mul(mult_scaled)
            .saturating_div(100);
        let unclamped = core::cmp::min(base, self.pol.max_ms);
        // jitter in [1 - j .. 1 + j]
        let jitter = self.next_unit(); // in [0,1)
        let jf = self.pol.jitter_frac_times_100 as f64 / 100.0;
        let low = (1.0 - jf).max(0.0);
        let high = 1.0 + jf;
        let factor = low + (high - low) * jitter;
        let out = (unclamped as f64 * factor) as u64;
        Duration::from_millis(core::cmp::max(out, 1))
    }

    fn next_unit(&mut self) -> f64 {
        // xorshift64*
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        let y = x.wrapping_mul(0x2545F4914F6CDD1D);
        // scale to [0,1)
        (y >> 11) as f64 / ((u64::MAX >> 11) as f64)
    }
}

pub type TimerHandle = Arc<dyn Timer + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_progresses_and_exhausts() {
        let pol = BackoffPolicy::default();
        let mut bo = ExponentialBackoff::new(pol);
        let mut delays = Vec::new();
        for _ in 0..pol.max_retries {
            delays.push(bo.next_delay());
        }
        assert!(delays.iter().all(|d| *d >= Duration::from_millis(1)));
        assert!(bo.exhausted());
    }

    #[test]
    fn disabled_backoff_is_exhausted_immediately() {
        let pol = BackoffPolicy::disabled();
        let bo = ExponentialBackoff::new(pol);
        assert!(bo.exhausted());
    }
}

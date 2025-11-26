use std::time::Duration;

use fusio_core::{MaybeSend, MaybeSync};

/// Policy interface governing TTLs for checkpoints, segments, and leases.
pub trait RetentionPolicy: MaybeSend + MaybeSync + 'static {
    /// Keep at least this many recent checkpoints regardless of age.
    fn checkpoints_keep_last(&self) -> usize;
    /// Minimum time to keep old checkpoints before they become GC candidates.
    fn checkpoints_min_ttl(&self) -> Duration;
    /// Minimum time to keep legacy segments before they may be deleted.
    fn segments_min_ttl(&self) -> Duration;
    /// Lease TTL used for read/write sessions.
    fn lease_ttl(&self) -> Duration;
}

/// Default retention policy tuned for correctness-first behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultRetention {
    checkpoints_keep_last: usize,
    checkpoints_min_ttl: Duration,
    segments_min_ttl: Duration,
    lease_ttl: Duration,
}

impl Default for DefaultRetention {
    fn default() -> Self {
        Self {
            checkpoints_keep_last: 2,
            checkpoints_min_ttl: Duration::from_secs(60),
            segments_min_ttl: Duration::from_secs(30),
            lease_ttl: Duration::from_secs(60),
        }
    }
}

impl RetentionPolicy for DefaultRetention {
    fn checkpoints_keep_last(&self) -> usize {
        self.checkpoints_keep_last
    }

    fn checkpoints_min_ttl(&self) -> Duration {
        self.checkpoints_min_ttl
    }

    fn segments_min_ttl(&self) -> Duration {
        self.segments_min_ttl
    }

    fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }
}

impl DefaultRetention {
    pub fn with_checkpoints_keep_last(mut self, n: usize) -> Self {
        self.checkpoints_keep_last = n;
        self
    }

    pub fn with_checkpoints_min_ttl(mut self, ttl: Duration) -> Self {
        self.checkpoints_min_ttl = ttl;
        self
    }

    pub fn with_segments_min_ttl(mut self, ttl: Duration) -> Self {
        self.segments_min_ttl = ttl;
        self
    }

    pub fn with_lease_ttl(mut self, ttl: Duration) -> Self {
        self.lease_ttl = ttl;
        self
    }
}

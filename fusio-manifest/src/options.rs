use std::time::Duration;

use fusio_core::durability::DurabilityLevel;

/// Durability barrier strategy per RFC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Local content durability (fdatasync-like).
    LocalData,
    /// Local content + metadata durability (fsync-like).
    LocalAll,
    /// Object-store style finalize/publish (e.g., S3 MPU commit).
    Commit,
}

impl From<Durability> for DurabilityLevel {
    fn from(d: Durability) -> Self {
        match d {
            Durability::LocalData => DurabilityLevel::Data,
            Durability::LocalAll => DurabilityLevel::All,
            Durability::Commit => DurabilityLevel::Commit,
        }
    }
}

/// Group commit configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GroupCommit {
    /// Optional: commit when at least this many records are buffered.
    pub records: Option<usize>,
    /// Optional: commit when at least this many bytes are buffered (estimated).
    pub bytes: Option<usize>,
    /// Optional: commit if this much time elapsed since last commit.
    pub interval: Option<Duration>,
}

/// Sync policy controlling when barriers occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Size/time guarded commit.
    GroupCommit(GroupCommit),
    /// Commit on each record/batch append.
    PerRecord,
}

/// Publish acknowledgment mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Acknowledge after HEAD is published (CAS) — default.
    ManifestPublished,
    /// Acknowledge after data becomes durable but before publish.
    /// On object stores this is rarely used; kept for flexibility.
    DataDurable,
}

/// Writer control policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterControl {
    pub ack_mode: AckMode,
    pub sync: SyncPolicy,
    /// Optional cap to bound in-memory buffering regardless of sync policy.
    pub max_buffer_records: Option<usize>,
}

impl Default for WriterControl {
    fn default() -> Self {
        Self {
            ack_mode: AckMode::ManifestPublished,
            sync: SyncPolicy::GroupCommit(GroupCommit {
                records: Some(64),
                bytes: None,
                interval: Some(Duration::from_millis(300)),
            }),
            max_buffer_records: Some(4096),
        }
    }
}

/// Segmenting configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segmenting {
    pub segment_bytes: usize,
    pub roll_interval_ms: u64,
}

/// Snapshot policy configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPolicy {
    pub target_bytes: usize,
    pub interval_ms: u64,
}

/// Manifest options.
#[derive(Debug, Clone)]
pub struct Options {
    pub durability: Durability,
    pub sync: SyncPolicy,
    pub segmenting: Segmenting,
    pub snapshots: SnapshotPolicy,
    /// Single writer by default; CAS head (multi-writer) later.
    pub single_writer: bool,
    /// Control policies applied to Writer instances.
    pub writer: WriterControl,
    /// Retention/GC policy.
    pub retention: Retention,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            durability: Durability::LocalData,
            sync: SyncPolicy::GroupCommit(GroupCommit {
                records: Some(64),
                bytes: None,
                interval: Some(Duration::from_millis(300)),
            }),
            segmenting: Segmenting {
                segment_bytes: 8 * 1024 * 1024,
                roll_interval_ms: 60_000,
            },
            snapshots: SnapshotPolicy {
                target_bytes: 256 * 1024 * 1024,
                interval_ms: 60_000,
            },
            single_writer: true,
            writer: WriterControl::default(),
            retention: Retention::default(),
        }
    }
}

/// Retention/GC policy settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// Keep at least this many recent checkpoints.
    pub checkpoints_keep_last: usize,
    /// Minimum time to keep old checkpoints before GC (ms).
    pub checkpoints_min_ttl_ms: u64,
    /// Minimum time to keep old segments before GC (ms).
    pub segments_min_ttl_ms: u64,
    /// Lease TTL used for active tx tracking (ms).
    pub lease_ttl_ms: u64,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            checkpoints_keep_last: 2,
            checkpoints_min_ttl_ms: 60_000,
            segments_min_ttl_ms: 30_000,
            lease_ttl_ms: 60_000,
        }
    }
}

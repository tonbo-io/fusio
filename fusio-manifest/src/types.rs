/// Monotonic transaction identifier assigned per committed session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct TxnId(pub u64);

/// Identifier of an immutable segment (sequence number).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentId {
    pub seq: u64,
}

/// Crate-local error type. Keep variants stable and small.
#[derive(Debug)]
pub enum Error {
    /// Operation not yet implemented in this phase.
    Unimplemented(&'static str),
    /// CAS precondition failed (e.g., If-Not-Exists or If-Match).
    PreconditionFailed,
    /// Corrupt or invalid data encountered.
    Corrupt(String),
    /// Other errors propagated as boxed error.
    Other(Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T, E = Error> = core::result::Result<T, E>;

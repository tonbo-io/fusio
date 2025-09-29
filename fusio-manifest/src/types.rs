use fusio::Error as FusioError;
use thiserror::Error;

/// Monotonic transaction identifier assigned per committed session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct TxnId(pub u64);

/// Identifier of an immutable segment (sequence number).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentId {
    pub seq: u64,
}

/// Crate-local error type. Keep variants stable and small.
#[derive(Debug, Error)]
pub enum Error {
    /// Operation not yet implemented in this phase.
    #[error("unimplemented: {0}")]
    Unimplemented(&'static str),
    /// CAS precondition failed (e.g., If-Not-Exists or If-Match).
    #[error("precondition failed")]
    PreconditionFailed,
    /// Corrupt or invalid data encountered.
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// Underlying storage/runtime IO failure (e.g., fusio FS error).
    #[error(transparent)]
    Io(#[from] FusioError),
    /// Other errors propagated as boxed error.
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T, E = Error> = core::result::Result<T, E>;

impl Error {
    #[inline]
    pub fn other<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error::Other(Box::new(err))
    }

    pub fn from_box<E>(err: Box<E>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error::Other(err)
    }
}

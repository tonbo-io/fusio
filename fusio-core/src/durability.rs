/// A coarse policy indicating the desired level of durability.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DurabilityLevel {
    /// Do not request durability beyond normal buffered IO.
    None,
    /// Ensure content reaches the OS/runtime buffers; not crash-safe.
    Flush,
    /// Ensure file content is persisted (fdatasync-like).
    Data,
    /// Ensure content and metadata are persisted (fsync-like).
    All,
    /// Finalize/publish object if the backend supports it (object stores).
    Commit,
}

/// A fine-grained operation describing a specific durability action.
///
/// Semantics are capability-based and may be unsupported on some backends. When
/// unsupported, callers should expect an `Unsupported` error and decide whether to
/// degrade or fail.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DurabilityOp {
    /// Drain in-process buffers to the OS/runtime buffers.
    /// - Does not make data persistent across crashes.
    /// - Typical mapping: buffered writer flush.
    Flush,
    /// Persist file content to durable media, excluding most metadata.
    /// - POSIX mapping: `fdatasync`.
    /// - Windows mapping: `FlushFileBuffers` (no separate distinction).
    /// - Object stores/OPFS: often unsupported; treated as best-effort.
    DataSync,
    /// Persist file content and metadata to durable media.
    /// - POSIX mapping: `fsync`.
    /// - On macOS, callers may opt into `F_FULLFSYNC` via backend options.
    Fsync,
    /// Persist parent directory mutations (create/rename/delete).
    /// - POSIX mapping: open parent directory and `fsync` it.
    /// - Windows: no distinct primitive; backends document behavior.
    DirSync,
    /// Finalize object visibility in systems requiring a publish step.
    /// - Example: S3 Complete Multipart Upload to make an object visible after parts are durably
    ///   stored.
    Commit,
}

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

/// A static capability advertised by a backend/handle.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Capability {
    /// Supports flushing buffered content to the OS/runtime.
    Flush,
    /// Supports data-only persistence (fdatasync-like).
    DataSync,
    /// Supports full fsync (content + metadata) persistence.
    Fsync,
    /// Supports syncing the parent directory (create/rename/delete persistence).
    DirSync,
    /// Supports explicit publish/finalize of objects.
    Commit,
    /// Supports range-based sync as an optimization; may degrade to full sync.
    RangeSync,
}

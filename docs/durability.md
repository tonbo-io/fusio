# Durability Semantics (Capability-Based)

This document defines durability semantics for Fusio across local filesystems, object stores, and OPFS/WASM. It is intentionally capability-based and not log-specific so it applies to logs, atomic config writes, compaction outputs, and other file-like workflows.

## Semantics
- Flush: move buffered bytes into OS/runtime buffers; may still be lost on crash.
- DataSync: persist file content to durable media; excludes metadata like rename/mtime (POSIX `fdatasync`).
- Fsync: persist file content and metadata (POSIX `fsync`).
- DirSync: persist parent directory mutations (create, delete, rename) by fsync on the directory handle.
- Commit: finalize visibility in object stores (e.g., S3 MPU complete) after uploaded parts are durable.

Notes
- When an operation is unsupported on a backend, it must be surfaced as such and either no-op or map to the closest stronger operation when safe, with explicit `Unsupported` errors for requested guarantees that cannot be met.
- Range sync is a best-effort optimization on supported OSes and can degrade to full file sync.

## API Surface (as implemented)
These traits and options are additive and do not replace the core Read/Write traits. Shared enums live in `fusio-core`; durability traits live in `fusio`.

```rust
// fusio-core (shared types)
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DurabilityOp { Flush, DataSync, Fsync, DirSync, Commit }

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DurabilityLevel { None, Flush, Data, All, Commit }

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Capability { Flush, DataSync, Fsync, DirSync, Commit, RangeSync }
```

```rust
// fusio (extension traits)
use crate::{Error, MaybeSend};
use core::future::Future;

// File-handle durability operations
pub trait FileSync: MaybeSend {
    fn sync_data(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
    fn sync_all(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
    /// Optional optimization; may fall back to data sync.
    fn sync_range(&mut self, _offset: u64, _len: u64)
        -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// Backends that require an explicit finalize step for visibility (e.g., S3 MPU complete).
pub trait FileCommit: MaybeSend {
    fn commit(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// Directory sync exposed via filesystem handle when applicable.
pub trait DirSync: MaybeSend {
    fn sync_parent(&self, path: &crate::path::Path)
        -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// Query for durability capabilities on a backend or handle.
pub trait SupportsDurability {
    fn supports(&self, op: crate::durability::DurabilityOp) -> bool;
}
```

Policy helpers on close:

```rust
// Flush drains in-process buffers; Data/All perform durable syncs.
// Commit degrades to sync_all by default.
pub async fn apply_on_close<W: FileSync + Write>(
    writer: &mut W,
    level: Option<DurabilityLevel>,
) -> Result<(), Error> { /* ... */ }

// Prefer real commit for backends that support it.
pub async fn apply_on_close_with_commit<W: FileSync + Write + FileCommit>(
    writer: &mut W,
    level: Option<DurabilityLevel>,
) -> Result<(), Error> { /* ... */ }
```

```rust
// fusio::fs::OpenOptions (additive fields)
#[derive(Debug)]
pub struct OpenOptions {
    // existing
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,

    // new (optional)
    pub write_through: bool,            // best-effort write-through/open flags
    pub bytes_per_sync: Option<u64>,     // trigger sync after N bytes written
    pub sync_on_close: Option<DurabilityLevel>,
    pub dirsync_on_rename: bool,         // sync parent after create/rename
}
```

Helpers
- apply_on_close(level): Flush → flush, Data/All → sync, Commit → sync_all fallback.
- apply_on_close_with_commit(level): Commit → commit() when available; otherwise same as apply_on_close.
- atomic_write_file(path, bytes, level): write temp, `sync_data`, close, rename, `dirsync` (planned).
- atomic_replace(paths[], level): write all temps, sync each, rename all, final `dirsync` (planned).
- DurabilityGuard: RAII helper to `sync_data` every N ms/bytes (planned).

## Backend Mapping
- POSIX/local
  - Flush: `Write::flush` on buffers.
  - DataSync: `fdatasync` (or `F_FULLFSYNC` on macOS when requested).
  - Fsync: `fsync`.
  - DirSync: `fsync(dirfd)` on the parent directory.
  - Commit: Unsupported (no publish step).
- macOS
  - Support `F_FULLFSYNC` behind an option for stronger barriers; default to `fdatasync`/`fsync`.
- Windows
  - DataSync/Fsync: `FlushFileBuffers` (file handle). Directory sync has no distinct primitive; document behavior.
  - DirSync: Unsupported (returns Unsupported in fusio).
- S3 and object stores
  - Flush: buffer upload or part upload initiation; not durable.
  - DataSync: uploaded parts persisted but not yet visible.
  - Commit: MPU complete (visibility). DirSync N/A. Fsync concept N/A.
- OPFS/WASM
  - Flush/DataSync: `SyncAccessHandle.flush()`/`close()` where available; durability caveats documented by browser.
  - DirSync/Commit: N/A.

Capability table (indicative)

| Backend         | Flush | DataSync | Fsync | DirSync | RangeSync | Commit |
|-----------------|:-----:|:--------:|:-----:|:-------:|:---------:|:------:|
| POSIX (Linux)   |  yes  |   yes    |  yes  |   yes   |  maybe    |  N/A   |
| macOS           |  yes  |  yes*    |  yes  |   yes   |  maybe    |  N/A   |
| Windows         |  yes  |   yes    |  yes  |   no    |   no      |  N/A   |
| S3/Obj store    |  yes  |   yes    |  N/A  |   N/A   |   N/A     |  yes   |
| OPFS/WASM       |  yes  |  yes†    |  N/A  |   N/A   |   N/A     |  N/A   |

- macOS yes*: optionally `F_FULLFSYNC` when requested.
- OPFS yes†: subject to browser implementation; treat as best-effort.

## Usage Patterns
- Append-only log
  - Buffer writes; `flush` per batch; `sync_data` every N bytes/time; `sync_parent` on file create/segment roll; `sync_on_close = Data` or `All`.
- Atomic config write
  - Write to temp, `sync_data`, close; rename over destination; `sync_parent` on destination parent.
- Multi-file update
  - Write temps for all, `sync_data` each; rename all; final `sync_parent` on the common parent directory.
- Object-store upload
  - Upload parts; periodic `sync` as MPU part completes (best-effort); `commit` at the end; treat re-listing as visibility confirmation.

Commit fallback example

```rust
use fusio::{durability::FileCommit, DurabilityOp, SupportsDurability};

if writer.supports(DurabilityOp::Commit) {
    writer.commit().await?;
} else {
    writer.sync_all().await?;
}
```

Or use the helper that prefers commit:

```rust
use fusio::durability::apply_on_close_with_commit;
apply_on_close_with_commit(&mut writer, Some(DurabilityLevel::Commit)).await?;
```

## Dynamic Dispatch

- All dynamic files (`Box<dyn DynFile>`) support `commit()` via `DynFileCommit`.
  - On backends where commit has no meaning (local files), it returns `Error::Unsupported`.
- `BufWriter<Box<dyn DynFile>>` implements `FileSync` by delegating to concrete backends under the hood.
- `DirSync` is exposed on filesystem handles (e.g., `TokioFs`, `MonoioFs`, `TokioUringFs`), not file handles.

## Error and Degradation Policy
- If a requested level or op is unsupported, return `Error::Unsupported` from the specific op and document the nearest guarantee that can be achieved.
- Helpers should allow a policy: "error on unsupported" or "degrade to nearest" with explicit logging/metrics.

## Phased Adoption
1. Add types and traits behind a `durability` feature; implement for local disk backends first (Tokio/Tokio-uring/Monoio).
2. Implement directory sync for create/rename paths in local backends; add `sync_on_close` semantics.
3. Map S3 MPU to `CommitOp` with documented DataSync/Commit split; OPFS: best-effort flush/close.
4. Wire options into fusio-log; add tests for crash safety (truncate-on-recover, early CRC, fsync-on-create/roll).
5. Expose helpers (`atomic_write_file`, `atomic_replace`) and DurabilityGuard; add metrics and tracing.

---
This is a design document; exact signatures and module placement can be adjusted to match crate boundaries and feature flags.

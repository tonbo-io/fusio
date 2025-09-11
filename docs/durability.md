# Durability Semantics (Method-First, Family-Specific)

This document defines durability semantics for Fusio across local filesystems, object stores, and OPFS/WASM. It focuses on method-based, family-specific traits (no runtime durability dispatch) so the call sites read cleanly and guarantees are explicit.

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
These traits are additive and do not replace the core `Read`/`Write`. Policy enums live in `fusio-core` (`DurabilityLevel`); durability traits live in `fusio`.

```rust
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DurabilityLevel { None, Flush, Data, All, Commit }
```

Key traits in `fusio::durability`:

- `FileSync` — local filesystem handles: `sync_data()`, `sync_all()`, and optional `sync_range()`.
- `FileCommit` — object stores: `commit()` to finalize/publish objects (e.g., complete MPU).
- `DirSync` — filesystem-level parent directory durability: `sync_parent(path)`.

Close policy: higher layers may map `DurabilityLevel` to method calls:

```rust
match level {
    None | Some(DurabilityLevel::None) => {}
    Some(DurabilityLevel::Flush) => writer.flush().await?,
    Some(DurabilityLevel::Data) => writer.sync_data().await?,
    Some(DurabilityLevel::All) => writer.sync_all().await?,
    Some(DurabilityLevel::Commit) => writer.commit().await?, // object-store only
}
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
- atomic_write_file(path, bytes): write temp, `sync_data`, close, rename, `dirsync`.
- atomic_replace(paths[]): write all temps, sync each, rename all, final `dirsync`.
- DurabilityGuard: RAII helper to call `sync_data` every N ms/bytes.

## Backend Mapping
- POSIX/local
  - Flush: `Write::flush` on buffers.
  - DataSync: `sync_data` (fdatasync-like; or `F_FULLFSYNC` on macOS when requested).
  - Fsync: `sync_all` (fsync-like).
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

- Backend notes
- Local FS (POSIX/Windows): `sync_data`/`sync_all`; on Windows both map to `FlushFileBuffers`. Parent `sync_parent` for create/rename on POSIX.
- Object Store: `commit()` finalizes visibility (e.g., S3 MPU complete). No rename/dirsync.
- OPFS/WASM: treat as best-effort; `flush` only.

## Usage Patterns
- Append-only log
  - Buffer writes; `flush` per batch; `sync_data` every N bytes/time; `sync_parent` on file create/segment roll; `sync_on_close = Data` or `All`.
- Atomic config write
  - Write to temp, `sync_data`, close; rename over destination; `sync_parent` on destination parent.
- Multi-file update
  - Write temps for all, `sync_data` each; rename all; final `sync_parent` on the common parent directory.
- Object-store upload
  - Upload parts; periodic `sync` as MPU part completes (best-effort); `commit` at the end; treat re-listing as visibility confirmation.

Commit usage
```rust
use fusio::durability::FileCommit;
// `writer` must implement `FileCommit`
writer.commit().await?;
```

## Dynamic Notes

- For `Box<dyn DynFile>`, durable barriers require knowing the family at construction time, or an
  adapter that maps to the correct methods internally. Prefer using concrete types for durability.

## Error and Degradation Policy
- If a requested level or op is unsupported, return `Error::Unsupported` from the specific op and document the nearest guarantee that can be achieved.
- Helpers should allow a policy: "error on unsupported" or "degrade to nearest" with explicit logging/metrics.

## Phased Adoption
1. Add types and traits behind a `durability` feature; implement for local disk backends first (Tokio/Tokio-uring/Monoio).
2. Implement directory sync for create/rename paths in local backends; add `sync_on_close` semantics.
3. Map S3 MPU to `CommitOp` with documented DataSync/Commit split; OPFS: best-effort flush/close.
4. Integrate into higher layers (e.g., manifest) with tests for crash safety (truncate-on-recover, early CRC, fsync-on-create/roll).
5. Expose helpers (`atomic_write_file`, `atomic_replace`) and DurabilityGuard; add metrics and tracing.

---
This is a design document; exact signatures and module placement can be adjusted to match crate boundaries and feature flags.

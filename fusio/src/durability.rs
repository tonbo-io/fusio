use core::future::Future;

use fusio_core::{Capability, DurabilityLevel, DurabilityOp, MaybeSend};

use crate::error::Error;

/// File-handle durability operations.
pub trait FileSync: MaybeSend {
    fn sync_data(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
    fn sync_all(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
    /// Optional optimization; allowed to fall back to sync_data.
    fn sync_range(
        &mut self,
        _offset: u64,
        _len: u64,
    ) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// Backends that require an explicit finalize step for visibility (e.g., S3 MPU complete).
pub trait Commit: MaybeSend {
    fn commit(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// Filesystem-level parent directory sync operations.
pub trait DirSync: MaybeSend {
    fn sync_parent(
        &self,
        path: &crate::path::Path,
    ) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// Capability query used by higher layers to feature-detect.
pub trait SupportsDurability {
    fn supports(&self, op: DurabilityOp) -> bool;
    fn capabilities(&self) -> &'static [Capability] {
        const EMPTY: &[Capability] = &[];
        let _ = EMPTY; // silence unused warning when not overridden
        EMPTY
    }
}

/// Policy helper: convert a DurabilityLevel into concrete operations.
pub async fn apply_on_close<W: FileSync>(
    writer: &mut W,
    level: Option<DurabilityLevel>,
) -> Result<(), Error> {
    match level {
        None | Some(DurabilityLevel::None) => Ok(()),
        Some(DurabilityLevel::Flush) => writer.sync_data().await, // ensure content
        Some(DurabilityLevel::Data) => writer.sync_data().await,
        Some(DurabilityLevel::All) => writer.sync_all().await,
        Some(DurabilityLevel::Commit) => writer.sync_all().await, // commit is file-store specific
    }
}

// --- SupportsDurability implementations ---

// Dynamic file handles: detect capabilities by downcasting to known backends.
#[cfg(feature = "dyn")]
impl SupportsDurability for Box<dyn crate::dynamic::fs::DynFile> {
    fn supports(&self, op: DurabilityOp) -> bool {
        use core::any::Any;
        let any = self.as_ref() as &dyn Any;

        // Local files (Tokio)
        #[cfg(feature = "tokio")]
        if any.is::<crate::impls::disk::tokio::TokioFile>() {
            return matches!(
                op,
                DurabilityOp::Flush | DurabilityOp::DataSync | DurabilityOp::Fsync
            );
        }

        // Local files (tokio-uring)
        #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
        if any.is::<crate::impls::disk::tokio_uring::TokioUringFile>() {
            return matches!(
                op,
                DurabilityOp::Flush | DurabilityOp::DataSync | DurabilityOp::Fsync
            );
        }

        // Local files (monoio)
        #[cfg(feature = "monoio")]
        if any.is::<crate::impls::disk::monoio::MonoioFile>() {
            return matches!(
                op,
                DurabilityOp::Flush | DurabilityOp::DataSync | DurabilityOp::Fsync
            );
        }

        // OPFS streaming writer: no durable sync or commit; flush only.
        #[cfg(all(feature = "opfs", target_arch = "wasm32"))]
        if any.is::<crate::impls::disk::OPFSFile>() {
            return matches!(op, DurabilityOp::Flush);
        }

        // OPFS Sync Access Handle: flush acts as data sync; no dirsync/commit
        #[cfg(all(feature = "opfs", target_arch = "wasm32", feature = "sync"))]
        if any.is::<crate::impls::disk::OPFSSyncFile>() {
            return matches!(op, DurabilityOp::Flush | DurabilityOp::DataSync);
        }

        // S3/object store: flush (best-effort) and commit supported.
        #[cfg(feature = "aws")]
        if any.is::<crate::impls::remotes::aws::s3::S3File>() {
            return matches!(
                op,
                DurabilityOp::Flush | DurabilityOp::Commit | DurabilityOp::DataSync
            );
        }

        false
    }
}

// Concrete backends

#[cfg(feature = "tokio")]
impl SupportsDurability for crate::impls::disk::tokio::TokioFile {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(
            op,
            DurabilityOp::Flush | DurabilityOp::DataSync | DurabilityOp::Fsync
        )
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::Flush, Capability::DataSync, Capability::Fsync];
        CAPS
    }
}

#[cfg(all(feature = "tokio-uring", target_os = "linux"))]
impl SupportsDurability for crate::impls::disk::tokio_uring::TokioUringFile {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(
            op,
            DurabilityOp::Flush | DurabilityOp::DataSync | DurabilityOp::Fsync
        )
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::Flush, Capability::DataSync, Capability::Fsync];
        CAPS
    }
}

#[cfg(feature = "monoio")]
impl SupportsDurability for crate::impls::disk::monoio::MonoioFile {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(
            op,
            DurabilityOp::Flush | DurabilityOp::DataSync | DurabilityOp::Fsync
        )
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::Flush, Capability::DataSync, Capability::Fsync];
        CAPS
    }
}

#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
impl SupportsDurability for crate::impls::disk::OPFSFile {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(op, DurabilityOp::Flush)
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::Flush];
        CAPS
    }
}

#[cfg(all(feature = "opfs", target_arch = "wasm32", feature = "sync"))]
impl SupportsDurability for crate::impls::disk::OPFSSyncFile {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(op, DurabilityOp::Flush | DurabilityOp::DataSync)
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::Flush, Capability::DataSync];
        CAPS
    }
}

#[cfg(feature = "aws")]
impl SupportsDurability for crate::impls::remotes::aws::s3::S3File {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(
            op,
            DurabilityOp::Flush | DurabilityOp::DataSync | DurabilityOp::Commit
        )
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::Flush, Capability::DataSync, Capability::Commit];
        CAPS
    }
}

// Filesystem handles that provide directory sync
#[cfg(all(feature = "tokio", feature = "fs"))]
impl SupportsDurability for crate::impls::disk::tokio::fs::TokioFs {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(op, DurabilityOp::DirSync)
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::DirSync];
        CAPS
    }
}

#[cfg(all(feature = "tokio-uring", target_os = "linux", feature = "fs"))]
impl SupportsDurability for crate::impls::disk::tokio_uring::fs::TokioUringFs {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(op, DurabilityOp::DirSync)
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::DirSync];
        CAPS
    }
}

#[cfg(all(feature = "monoio", feature = "fs"))]
impl SupportsDurability for crate::impls::disk::monoio::fs::MonoIoFs {
    fn supports(&self, op: DurabilityOp) -> bool {
        matches!(op, DurabilityOp::DirSync)
    }

    fn capabilities(&self) -> &'static [Capability] {
        const CAPS: &[Capability] = &[Capability::DirSync];
        CAPS
    }
}

#[cfg(all(feature = "opfs", target_arch = "wasm32", feature = "fs"))]
impl SupportsDurability for crate::impls::disk::opfs::fs::OPFS {
    fn supports(&self, _op: DurabilityOp) -> bool {
        // OPFS does not provide directory sync primitive
        false
    }
}

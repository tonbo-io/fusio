use core::future::Future;

use fusio_core::MaybeSend;

use crate::{error::Error, path::Path};

/// File-handle durability operations for local filesystems.
///
/// These map to POSIX-style `fdatasync`/`fsync` semantics on supported
/// platforms. Implementors may degrade `sync_range` to `sync_data`.
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
pub trait FileCommit: MaybeSend {
    fn commit(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// Filesystem-level parent directory sync operations.
pub trait DirSync: MaybeSend {
    fn sync_parent(&self, path: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// Readable alias for directory sync.
pub trait DirSyncExt: DirSync {
    fn dirsync_parent(&self, path: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        self.sync_parent(path)
    }
}
impl<T: DirSync + ?Sized> DirSyncExt for T {}

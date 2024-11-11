//! This module contains the `Fs` trait, which is used to abstract file system operations across
//! different file systems.

mod options;

use std::future::Future;

use futures_core::Stream;
pub use options::*;

use crate::{path::Path, Error, MaybeSend, MaybeSync, Read, Write};

#[derive(Debug)]
pub struct FileMeta {
    pub path: Path,
    pub size: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FileSystemTag {
    Local,
    OPFS,
    // TODO: Remote needs to check whether endpoint and other remote fs are consistent
    S3,
}

pub trait Fs: MaybeSend + MaybeSync {
    //! This trait is used to abstract file system operations across different file systems.

    type File: Read + Write + MaybeSend + 'static;

    fn file_system(&self) -> FileSystemTag;

    fn open(&self, path: &Path) -> impl Future<Output = Result<Self::File, Error>> {
        self.open_options(path, OpenOptions::default())
    }

    fn open_options(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> impl Future<Output = Result<Self::File, Error>> + MaybeSend;

    fn create_dir_all(path: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn list(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<FileMeta, Error>>, Error>> + MaybeSend;

    fn remove(&self, path: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn copy<F: Fs>(
        &self,
        from: &Path,
        to_fs: &F,
        to: &Path,
    ) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn link<F: Fs>(
        &self,
        from: &Path,
        to_fs: &F,
        to: &Path,
    ) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

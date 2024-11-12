//! This module contains the `Fs` trait, which is used to abstract file system operations across
//! different file systems.

mod options;

use std::{cmp, future::Future};

use futures_core::Stream;
pub use options::*;

use crate::{path::Path, Error, IoBufMut, MaybeSend, MaybeSync, Read, Write};

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

    fn copy(&self, from: &Path, to: &Path) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn link<F: Fs>(
        &self,
        from: &Path,
        to_fs: &F,
        to: &Path,
    ) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

pub async fn copy<F, T>(from_fs: &F, from: &Path, to_fs: &T, to: &Path) -> Result<(), Error>
where
    F: Fs,
    T: Fs,
{
    if from_fs.file_system() == to_fs.file_system() {
        from_fs.copy(from, to).await?;
        return Ok(());
    }
    let mut from_file = from_fs
        .open_options(from, OpenOptions::default().read(true))
        .await?;
    let from_file_size = from_file.size().await? as usize;

    let mut to_file = to_fs
        .open_options(to, OpenOptions::default().create(true).write(true))
        .await?;
    let buf_size = cmp::min(from_file_size, 4 * 1024);
    let mut buf = vec![0u8; buf_size];
    let mut read_pos = 0u64;

    while (read_pos as usize) < from_file_size - 1 {
        let (result, _) = from_file.read_exact_at(buf.as_slice_mut(), read_pos).await;
        result?;
        read_pos += buf.len() as u64;

        let (result, _) = to_file.write_all(buf.as_slice()).await;
        result?;
        buf.resize(buf_size, 0);
    }

    Ok(())
}

use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
};

use futures_core::Stream;

use crate::{Read, Write};

pub struct FileMeta {
    pub path: PathBuf,
}

#[cfg(not(feature = "no-send"))]
pub trait Fs: Send + Sync {
    type File: Read + Write;

    fn open(
        &self,
        path: impl AsRef<Path> + Send,
    ) -> impl Future<Output = io::Result<Self::File>> + Send;

    fn list(
        &self,
        path: impl AsRef<Path> + Send,
    ) -> impl Future<Output = io::Result<impl Stream<Item = io::Result<FileMeta>>>> + Send;

    fn remove(&self, path: impl AsRef<Path> + Send) -> impl Future<Output = io::Result<()>> + Send;
}

#[cfg(feature = "no-send")]
pub trait Fs {
    type File: Read + Write;

    fn open(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self::File>>;

    fn list(
        &self,
        path: impl AsRef<Path>,
    ) -> impl Future<Output = io::Result<impl Stream<Item = io::Result<FileMeta>>>>;

    fn remove(&self, path: impl AsRef<Path>) -> impl Future<Output = io::Result<()>>;
}

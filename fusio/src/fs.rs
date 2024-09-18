use std::future::Future;

use futures_core::Stream;

use crate::{path::Path, Error, Read, Write};

pub struct FileMeta {
    pub path: Path,
}

#[cfg(not(feature = "no-send"))]
pub trait Fs: Send + Sync {
    type File: Read + Write;

    fn open(&self, path: &Path) -> impl Future<Output = Result<Self::File, Error>> + Send;

    fn create_dir(path: &Path) -> impl Future<Output = Result<(), Error>> + Send;

    fn list(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<FileMeta, Error>>, Error>> + Send;

    fn remove(&self, path: &Path) -> impl Future<Output = Result<(), Error>> + Send;
}

#[cfg(feature = "no-send")]
pub trait Fs {
    type File: Read + Write;

    fn open(&self, path: &Path) -> impl Future<Output = Result<Self::File, Error>>;

    fn create_dir(path: &Path) -> impl Future<Output = Result<(), Error>>;

    fn list(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<FileMeta, Error>>, Error>>;

    fn remove(&self, path: &Path) -> impl Future<Output = Result<(), Error>>;
}

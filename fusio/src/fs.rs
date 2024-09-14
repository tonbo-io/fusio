use std::future::Future;

use futures_core::Stream;

use crate::{path::Path, Error, Read, Write};

pub struct FileMeta {
    pub path: Path,
}

#[cfg(not(feature = "no-send"))]
pub trait Fs: Send + Sync {
    type Read: Read;
    type Write: Write;

    fn open_read(&self, path: &Path) -> impl Future<Output = Result<Self::Read, Error>> + Send;

    fn open_write(&self, path: &Path) -> impl Future<Output = Result<Self::Write, Error>> + Send;

    fn list(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<FileMeta, Error>>, Error>> + Send;

    fn remove(&self, path: &Path) -> impl Future<Output = Result<(), Error>> + Send;
}

#[cfg(feature = "no-send")]
pub trait Fs {
    type Read: Read;
    type Write: Write;

    fn open_read(&self, path: &Path) -> impl Future<Output = Result<Self::Read, Error>>;

    fn open_write(&self, path: &Path) -> impl Future<Output = Result<Self::Write, Error>>;

    fn list(
        &self,
        path: &Path,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<FileMeta, Error>>, Error>>;

    fn remove(&self, path: &Path) -> impl Future<Output = Result<(), Error>>;
}

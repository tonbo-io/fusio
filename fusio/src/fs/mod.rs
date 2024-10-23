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

pub trait Fs: MaybeSend + MaybeSync {
    type File: Read + Write + MaybeSend + 'static;

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
}

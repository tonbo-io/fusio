mod options;

use std::future::Future;

use futures_core::Stream;
pub use options::*;
use url::Url;

use crate::{Error, MaybeSend, MaybeSync, Read, Seek, Write};

#[derive(Debug)]
pub struct FileMeta {
    pub url: Url,
    pub size: u64,
}

pub trait Fs: MaybeSend + MaybeSync {
    type File: Read + Seek + Write + MaybeSend + MaybeSync + 'static;

    fn open(&self, url: &Url) -> impl Future<Output = Result<Self::File, Error>> {
        self.open_options(url, OpenOptions::default())
    }

    fn open_options(
        &self,
        url: &Url,
        options: OpenOptions,
    ) -> impl Future<Output = Result<Self::File, Error>> + MaybeSend;

    fn create_dir_all(url: &Url) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn list(
        &self,
        url: &Url,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<FileMeta, Error>>, Error>> + MaybeSend;

    fn remove(&self, url: &Url) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

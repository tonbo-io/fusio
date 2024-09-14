use std::{future::Future, io};

use async_stream::stream;
use futures_core::Stream;

use super::MonoioFile;
use crate::{
    fs::Fs,
    path::{path_to_local, Path},
    Error,
};

pub struct MonoIoFs;

impl Fs for MonoIoFs {
    type Read = MonoioFile;
    type Write = MonoioFile;

    async fn open_read(&self, path: &Path) -> Result<Self::File, Error> {
        let path = path_to_local(path)?;

        Ok(monoio::fs::File::open(path).await?.into())
    }

    fn open_write(&self, path: &Path) -> impl Future<Output = Result<Self::Write, Error>> {
        self.open_read(path)
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<crate::fs::FileMeta, Error>>, Error> {
        let path = path_to_local(path)?;
        let dir = path.read_dir()?;

        Ok(stream! {
            for entry in dir {
                yield Ok(crate::fs::FileMeta { path: Path::from_filesystem_path(entry?.path())? });
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path)?;

        Ok(std::fs::remove_file(path)?)
    }
}

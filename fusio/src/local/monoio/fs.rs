use std::{io, path::Path};

use async_stream::stream;
use futures_core::Stream;

use crate::fs::Fs;

use super::MonoioFile;

pub struct MonoIoFs;

impl Fs for MonoIoFs {
    type File = MonoioFile;

    async fn open(&self, path: impl AsRef<Path>) -> io::Result<Self::File> {
        Ok(monoio::fs::File::open(path).await?.into())
    }

    async fn list(
        &self,
        path: impl AsRef<Path>,
    ) -> io::Result<impl Stream<Item = io::Result<crate::fs::FileMeta>>> {
        let dir = path.as_ref().read_dir()?;
        Ok(stream! {
            for entry in dir {
                yield Ok(crate::fs::FileMeta { path: entry?.path() });
            }
        })
    }

    async fn remove(&self, path: impl AsRef<Path>) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

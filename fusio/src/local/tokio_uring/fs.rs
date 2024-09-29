use std::{io, path::Path};

use async_stream::stream;
use futures_core::Stream;
use tokio_uring::fs::{remove_file, File};

use crate::fs::{FileMeta, Fs};

pub struct TokioUringFs;

impl Fs for TokioUringFs {
    type File = File;

    async fn open(&self, path: impl AsRef<Path>) -> io::Result<Self::File> {
        File::open(path).await
    }

    async fn list(
        &self,
        path: impl AsRef<Path>,
    ) -> io::Result<impl Stream<Item = io::Result<FileMeta>>> {
        let dir = path.as_ref().read_dir()?;
        Ok(stream! {
            for entry in dir {
                yield Ok(crate::fs::FileMeta { url: entry?.path() });
            }
        })
    }

    async fn remove(&self, path: impl AsRef<Path>) -> io::Result<()> {
        remove_file(path).await
    }
}

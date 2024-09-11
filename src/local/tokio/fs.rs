use std::{io, path::Path};

use async_stream::stream;
use futures_core::Stream;
use tokio::{fs::File, task::spawn_blocking};

use crate::fs::{FileMeta, Fs};

pub struct TokioFs;

impl Fs for TokioFs {
    type File = File;

    async fn open(&self, path: impl AsRef<Path>) -> io::Result<Self::File> {
        File::open(path).await
    }

    async fn list(
        &self,
        path: impl AsRef<Path>,
    ) -> io::Result<impl Stream<Item = io::Result<FileMeta>>> {
        let path = path.as_ref().to_owned();
        let stream = spawn_blocking(move || {
            let entries = path.read_dir()?;
            Ok::<_, io::Error>(stream! {
                for entry in entries {
                    let entry = entry?;
                    let path = entry.path();
                    let meta = FileMeta { path };
                    yield Ok(meta);
                }
            })
        })
        .await??;
        Ok(stream)
    }

    async fn remove(&self, path: impl AsRef<Path>) -> io::Result<()> {
        tokio::fs::remove_file(path).await
    }
}

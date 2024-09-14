use std::{future::Future, io};

use async_stream::stream;
use futures_core::Stream;
use tokio::{
    fs::{remove_file, File},
    task::spawn_blocking,
};

use crate::{
    fs::{FileMeta, Fs},
    path::{path_to_local, Path},
    Error,
};

pub struct TokioFs;

impl Fs for TokioFs {
    type Read = File;
    type Write = File;

    async fn open_read(&self, path: &Path) -> Result<Self::Read, Error> {
        let path = path_to_local(path)?;

        Ok(File::open(&path).await?)
    }

    fn open_write(&self, path: &Path) -> impl Future<Output = Result<Self::Write, Error>> {
        self.open_read(path)
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = path_to_local(path)?;

        spawn_blocking(move || {
            let entries = path.read_dir()?;
            Ok::<_, Error>(stream! {
                for entry in entries {
                    yield Ok(FileMeta { path: Path::from_filesystem_path(entry?.path())? });
                }
            })
        })
        .await
        .map_err(io::Error::from)?
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path)?;

        remove_file(&path).await?;
        Ok(())
    }
}

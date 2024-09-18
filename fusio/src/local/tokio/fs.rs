use std::io;

use async_stream::stream;
use futures_core::Stream;
use tokio::{
    fs::{create_dir, remove_file, File},
    task::spawn_blocking,
};

use crate::{
    fs::{FileMeta, Fs},
    path::{path_to_local, Path},
    Error,
};

pub struct TokioFs;

impl Fs for TokioFs {
    type File = File;

    async fn open(&self, path: &Path) -> Result<Self::File, Error> {
        let path = path_to_local(path)?;

        Ok(File::open(&path).await?)
    }

    async fn create_dir(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path)?;
        create_dir(path).await?;

        Ok(())
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

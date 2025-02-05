use std::io;

use async_stream::stream;
use futures_core::Stream;
use tokio::{
    fs::{create_dir_all, remove_file},
    task::spawn_blocking,
};

use crate::{
    disk::tokio::TokioFile,
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::{path_to_local, Path},
    Error,
};

pub struct TokioFs;

impl Fs for TokioFs {
    type File = TokioFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::Local
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path = path_to_local(path)?;

        let file = tokio::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .append(options.write)
            .create(options.create)
            .open(&local_path)
            .await?;

        if options.truncate {
            file.set_len(0).await?;
        }

        Ok(TokioFile::new(file))
    }

    async fn create_dir_all(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path)?;
        create_dir_all(path).await?;

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
                    let entry = entry?;
                    yield Ok(FileMeta { path: Path::from_filesystem_path(entry.path())?, size: entry.metadata()?.len() });
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

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from)?;
        let to = path_to_local(to)?;

        tokio::fs::copy(&from, &to).await?;

        Ok(())
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from)?;
        let to = path_to_local(to)?;

        tokio::fs::hard_link(&from, &to).await?;

        Ok(())
    }
}

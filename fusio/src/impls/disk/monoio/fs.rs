use std::{fs, fs::create_dir_all};

use async_stream::stream;
use futures_core::Stream;

use super::MonoioFile;
use crate::{
    error::Error,
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::{path_to_local, Path},
};

pub struct MonoIoFs;

impl Fs for MonoIoFs {
    type File = MonoioFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::Local
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path = path_to_local(path).map_err(|err| Error::Path(err.into()))?;
        if options.create && !local_path.exists() {
            Self::create_file(&path).await?;
        }

        let file = monoio::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .truncate(options.truncate)
            .append(!options.truncate)
            .open(&local_path)
            .await?;
        let metadata = file.metadata().await?;
        Ok(MonoioFile::new(file, metadata.len()))
    }

    async fn create_file(path: &Path) -> Result<(), Error> {
        let local_path = path_to_local(path).map_err(|err| Error::Path(err.into()))?;
        if let Some(parent) = local_path.parent() {
            let parent_path = Path::from_filesystem_path(parent).map_err(|err| Error::Path(err.into()))?;
            Self::create_dir_all(&parent_path).await?;
        }

        monoio::fs::File::create(&local_path).await?;
        Ok(())
    }

    async fn create_dir_all(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(err.into()))?;
        create_dir_all(path)?;

        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(err.into()))?;
        let dir = path.read_dir()?;

        Ok(stream! {
            for entry in dir {
                let entry = entry?;
                yield Ok(FileMeta {
                    path: Path::from_filesystem_path(entry.path()).map_err(|err| Error::Path(err.into()))?,
                    size: entry.metadata()?.len()
                });
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(err.into()))?;

        Ok(fs::remove_file(path)?)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(err.into()))?;
        let to = path_to_local(to).map_err(|err| Error::Path(err.into()))?;

        monoio::spawn(async move { fs::copy(&from, &to) }).await?;

        Ok(())
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(err.into()))?;
        let to = path_to_local(to).map_err(|err| Error::Path(err.into()))?;

        monoio::spawn(async move { fs::hard_link(&from, &to) }).await?;

        Ok(())
    }
}

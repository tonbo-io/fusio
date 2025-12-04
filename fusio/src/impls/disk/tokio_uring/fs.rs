use std::{fs, io::ErrorKind};

use async_stream::stream;
use futures_core::Stream;
use tokio_uring::fs::{create_dir_all, remove_file};

use crate::{
    disk::tokio_uring::TokioUringFile,
    durability::DirSync,
    error::Error,
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::{path_to_local, Path},
};

pub struct TokioUringFs;

impl Fs for TokioUringFs {
    type File = TokioUringFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::Local
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path = path_to_local(path).map_err(|err| Error::Path(err.into()))?;
        if !local_path.exists() {
            if options.create {
                if let Some(parent_path) = local_path.parent() {
                    create_dir_all(parent_path).await?;
                }

                tokio_uring::fs::File::create(&local_path).await?;
            } else {
                return Err(Error::Path(Box::new(std::io::Error::new(
                    ErrorKind::NotFound,
                    "Path not found and option.create is false",
                ))));
            }
        }

        let absolute_path = std::fs::canonicalize(&local_path).unwrap();
        let file = tokio_uring::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .truncate(options.truncate)
            .open(&absolute_path)
            .await?;
        let stat = file.statx().await?;
        Ok(TokioUringFile {
            file: Some(file),
            pos: stat.stx_size,
        })
    }

    async fn create_dir_all(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(err.into()))?;
        create_dir_all(path).await?;

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
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(err) if err.kind() == ErrorKind::NotFound => continue,
                    Err(err) => Err(err)?,
                };
                yield Ok(FileMeta {
                    path: Path::from_filesystem_path(entry.path()).map_err(|err| Error::Path(err.into()))?,
                    size: metadata.len()
                });
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(err.into()))?;

        Ok(remove_file(path).await?)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(err.into()))?;
        let to = path_to_local(to).map_err(|err| Error::Path(err.into()))?;

        fs::copy(&from, &to)?;

        Ok(())
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(err.into()))?;
        let to = path_to_local(to).map_err(|err| Error::Path(err.into()))?;

        fs::hard_link(&from, &to)?;

        Ok(())
    }
}

impl DirSync for TokioUringFs {
    async fn sync_parent(&self, path: &Path) -> Result<(), Error> {
        let p = path_to_local(path).map_err(|err| Error::Path(err.into()))?;
        let Some(parent) = p.parent() else {
            return Ok(());
        };
        // Best-effort: use blocking std sync for directory.
        std::fs::File::open(parent)
            .and_then(|f| {
                f.sync_all()?;
                Ok(())
            })
            .map_err(Error::from)
    }
}

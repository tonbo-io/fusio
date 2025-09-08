use std::io;

use async_stream::stream;
use futures_core::Stream;
use tokio::{
    fs::{create_dir_all, remove_file},
    task::spawn_blocking,
};

use crate::{
    disk::tokio::TokioFile,
    durability::DirSync,
    error::Error,
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::{path_to_local, Path},
};

pub struct TokioFs;

impl Fs for TokioFs {
    type File = TokioFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::Local
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;
        if !local_path.exists() {
            if options.create {
                if let Some(parent_path) = local_path.parent() {
                    create_dir_all(parent_path).await?;
                }

                tokio::fs::File::create(&local_path).await?;
            } else {
                return Err(Error::Path(Box::new(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Path not found and option.create is false",
                ))));
            }
        }

        let absolute_path = std::fs::canonicalize(&local_path).unwrap();
        let file = tokio::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .append(!options.truncate)
            .truncate(options.truncate)
            .open(&absolute_path)
            .await?;

        Ok(TokioFile::new(file))
    }

    async fn create_dir_all(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;
        create_dir_all(path).await?;

        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;

        spawn_blocking(move || {
            let entries = path.read_dir()?;
            Ok::<_, Error>(stream! {
                for entry in entries {
                    let entry = entry?;
                    yield Ok(FileMeta {
                        path: Path::from_filesystem_path(entry.path()).map_err(|err| Error::Path(Box::new(err)))?,
                        size: entry.metadata()?.len()
                    });
                }
            })
        })
        .await
        .map_err(io::Error::from)?
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;

        remove_file(&path).await?;
        Ok(())
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(Box::new(err)))?;
        let to = path_to_local(to).map_err(|err| Error::Path(Box::new(err)))?;

        tokio::fs::copy(&from, &to).await?;

        Ok(())
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(Box::new(err)))?;
        let to = path_to_local(to).map_err(|err| Error::Path(Box::new(err)))?;

        tokio::fs::hard_link(&from, &to).await?;

        Ok(())
    }
}

#[cfg(not(target_os = "windows"))]
impl DirSync for TokioFs {
    async fn sync_parent(&self, path: &Path) -> Result<(), Error> {
        let p = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;
        let Some(parent) = p.parent() else {
            return Ok(());
        };
        let parent_path = parent.to_path_buf();
        tokio::task::spawn_blocking(move || {
            // Open directory and fsync it.
            let file = std::fs::File::open(parent_path)?;
            file.sync_all()?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(std::io::Error::from)??;
        Ok(())
    }
}

#[cfg(target_os = "windows")]
impl DirSync for TokioFs {
    async fn sync_parent(&self, _path: &Path) -> Result<(), Error> {
        // Windows lacks a direct, stable way to fsync directories via std APIs.
        Err(Error::Unsupported {
            message: "DirSync is not supported on Windows".into(),
        })
    }
}

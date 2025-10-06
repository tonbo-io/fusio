use std::io;

use async_fs::{create_dir_all, remove_file};
use async_stream::try_stream;
use fusio_core::error::Error;
use futures_core::Stream;
use futures_util::StreamExt;

use crate::{
    disk::AsyncFile,
    fs::{FileMeta, FileSystemTag},
    path::{path_to_local, Path},
    Fs, OpenOptions,
};

pub struct AsyncFs;

impl Fs for AsyncFs {
    type File = AsyncFile;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::Local
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path =
            path_to_local(path).map_err(|err| Error::Path(std::boxed::Box::new(err)))?;

        let exists = async_fs::metadata(&local_path).await.is_ok();
        if !exists {
            if options.create {
                if let Some(parent_path) = local_path.parent() {
                    create_dir_all(parent_path).await?;
                }

                async_fs::File::create(&local_path).await?;
            } else {
                return Err(Error::Path(std::boxed::Box::new(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Path not found and option.create is false",
                ))));
            }
        }

        let absolute_path = std::fs::canonicalize(&local_path).unwrap();
        let file = async_fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .append(!options.truncate)
            .truncate(options.truncate)
            .open(&absolute_path)
            .await?;

        Ok(AsyncFile::new(file))
    }

    async fn create_dir_all(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(std::boxed::Box::new(err)))?;
        create_dir_all(path).await?;

        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;

        let mut entries = async_fs::read_dir(&path)
            .await
            .map_err(|err| Error::Path(Box::new(err)))?;

        Ok(try_stream! {
            while let Some(entry) = entries.next().await {
                let entry = entry?;
                let path = Path::from_filesystem_path(entry.path())
                    .map_err(|err| Error::Path(Box::new(err)))?;
                let size = entry.metadata().await?.len();
                yield FileMeta { path, size };
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path).map_err(|err| Error::Path(Box::new(err)))?;
        remove_file(&path).await?;
        Ok(())
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(Box::new(err)))?;
        let to = path_to_local(to).map_err(|err| Error::Path(Box::new(err)))?;

        async_fs::copy(&from, &to).await?;

        Ok(())
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = path_to_local(from).map_err(|err| Error::Path(Box::new(err)))?;
        let to = path_to_local(to).map_err(|err| Error::Path(Box::new(err)))?;

        async_fs::hard_link(&from, &to).await?;

        Ok(())
    }
}

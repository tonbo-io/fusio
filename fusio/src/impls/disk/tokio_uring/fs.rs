use async_stream::stream;
use futures_core::Stream;
use tokio_uring::fs::{create_dir_all, remove_file};

use crate::{
    disk::tokio_uring::TokioUringFile,
    fs::{FileMeta, Fs, OpenOptions, WriteMode},
    path::{path_to_local, Path},
    Error,
};

pub struct TokioUringFs;

impl Fs for TokioUringFs {
    type File = TokioUringFile;

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path = path_to_local(path)?;

        let file = tokio_uring::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write.is_some())
            .create(options.create)
            .append(options.write == Some(WriteMode::Append))
            .truncate(options.truncate)
            .open(&local_path)
            .await?;

        Ok(TokioUringFile {
            file: Some(file),
            pos: 0,
        })
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
        let dir = path.read_dir()?;

        Ok(stream! {
            for entry in dir {
                let entry = entry?;
                yield Ok(FileMeta { path: Path::from_filesystem_path(entry.path())?, size: entry.metadata()?.len() });
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path)?;

        Ok(remove_file(path).await?)
    }
}

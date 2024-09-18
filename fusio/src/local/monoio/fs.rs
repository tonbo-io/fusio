use std::fs::create_dir;

use async_stream::stream;
use futures_core::Stream;

use super::MonoioFile;
use crate::{
    fs::{Fs, OpenOptions, WriteMode},
    path::{path_to_local, Path},
    Error,
};

pub struct MonoIoFs;

impl Fs for MonoIoFs {
    type File = MonoioFile;

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let path = path_to_local(path)?;

        Ok(monoio::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write.is_some())
            .create(options.create)
            .append(options.write == Some(WriteMode::Append))
            .truncate(options.write == Some(WriteMode::Overwrite))
            .open(&path)
            .await?
            .into())
    }

    async fn create_dir(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path)?;
        create_dir(path)?;

        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<crate::fs::FileMeta, Error>>, Error> {
        let path = path_to_local(path)?;
        let dir = path.read_dir()?;

        Ok(stream! {
            for entry in dir {
                yield Ok(crate::fs::FileMeta { path: Path::from_filesystem_path(entry?.path())? });
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path_to_local(path)?;

        Ok(std::fs::remove_file(path)?)
    }
}

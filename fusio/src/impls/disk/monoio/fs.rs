use std::fs::create_dir_all;

use async_stream::stream;
use futures_core::Stream;

use super::MonoioFile;
use crate::{
    fs::{FileMeta, Fs, OpenOptions},
    path::{path_to_local, Path},
    Error,
};

pub struct MonoIoFs;

impl Fs for MonoIoFs {
    type File = MonoioFile;

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path = path_to_local(path)?;

        Ok(MonoioFile::from(
            monoio::fs::OpenOptions::new()
                .read(options.read)
                .write(options.write)
                .create(options.create)
                .truncate(options.truncate)
                .open(&local_path)
                .await?,
        ))
    }

    async fn create_dir_all(path: &Path) -> Result<(), Error> {
        let path = path_to_local(path)?;
        create_dir_all(path)?;

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

        Ok(std::fs::remove_file(path)?)
    }
}

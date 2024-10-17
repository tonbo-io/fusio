use std::fs::create_dir_all;

use async_stream::stream;
use futures_core::Stream;

use super::MonoioFile;
use crate::{
    disk::Position,
    fs::{FileMeta, Fs, OpenOptions, WriteMode},
    path::{path_to_local, Path},
    Error,
};

pub struct MonoIoFs;

impl Fs for MonoIoFs {
    type File = MonoioFile;

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let local_path = path_to_local(path)?;
        let file = monoio::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write.is_some())
            .create(options.create)
            .append(options.write == Some(WriteMode::Append))
            .truncate(options.write == Some(WriteMode::Truncate))
            .open(&local_path)
            .await?;

        Ok(MonoioFile::from((
            file,
            Position {
                read_pos: 0,
                write_pos: 0,
            },
        )))
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

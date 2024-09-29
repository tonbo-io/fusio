use std::fs::create_dir_all;

use async_stream::stream;
use futures_core::Stream;
use url::Url;

use super::MonoioFile;
use crate::{
    fs::{FileMeta, Fs, OpenOptions, WriteMode},
    Error,
};

pub struct MonoIoFs;

impl Fs for MonoIoFs {
    type File = MonoioFile;

    async fn open_options(&self, url: &Url, options: OpenOptions) -> Result<Self::File, Error> {
        let path = url
            .to_file_path()
            .map_err(|_| Error::InvalidLocalUrl { url: url.clone() })?;

        Ok(MonoioFile::from(
            monoio::fs::OpenOptions::new()
                .read(options.read)
                .write(options.write.is_some())
                .create(options.create)
                .append(options.write == Some(WriteMode::Append))
                .truncate(options.write == Some(WriteMode::Overwrite))
                .open(&path)
                .await?,
        ))
    }

    async fn create_dir_all(url: &Url) -> Result<(), Error> {
        let path = url
            .to_file_path()
            .map_err(|_| Error::InvalidLocalUrl { url: url.clone() })?;

        create_dir_all(path)?;

        Ok(())
    }

    async fn list(&self, url: &Url) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = url
            .to_file_path()
            .map_err(|_| Error::InvalidLocalUrl { url: url.clone() })?;

        Ok(stream! {
            for entry in path.read_dir()? {
                let entry = entry?;
                let path = entry.path();

                let url = Url::from_file_path(&path)
                    .map_err(|_| Error::InvalidLocalPath { path })?;


                yield Ok(FileMeta { url, size: entry.metadata()?.len() });
            }
        })
    }

    async fn remove(&self, url: &Url) -> Result<(), Error> {
        let path = url
            .to_file_path()
            .map_err(|_| Error::InvalidLocalUrl { url: url.clone() })?;

        Ok(std::fs::remove_file(path)?)
    }
}

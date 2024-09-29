use std::io;

use async_stream::stream;
use futures_core::Stream;
use tokio::{
    fs::{create_dir_all, remove_file, File},
    task::spawn_blocking,
};
use url::Url;

use crate::{
    fs::{FileMeta, Fs, OpenOptions, WriteMode},
    Error,
};

pub struct TokioFs;

impl Fs for TokioFs {
    type File = File;

    async fn open_options(&self, url: &Url, options: OpenOptions) -> Result<Self::File, Error> {
        let path = url
            .to_file_path()
            .map_err(|_| Error::InvalidLocalUrl { url: url.clone() })?;

        Ok(tokio::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write.is_some())
            .create(options.create)
            .append(options.write == Some(WriteMode::Append))
            .truncate(options.write == Some(WriteMode::Overwrite))
            .open(path)
            .await?)
    }

    async fn create_dir_all(url: &Url) -> Result<(), Error> {
        let path = url
            .to_file_path()
            .map_err(|_| Error::InvalidLocalUrl { url: url.clone() })?;

        create_dir_all(path).await?;

        Ok(())
    }

    async fn list(&self, url: &Url) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = url
            .to_file_path()
            .map_err(|_| Error::InvalidLocalUrl { url: url.clone() })?;

        spawn_blocking(move || {
            let entries = path.read_dir()?;
            Ok::<_, Error>(stream! {
                for entry in entries {
                    let entry = entry?;
                    let path = entry.path();

                    let url = Url::from_file_path(&path)
                        .map_err(|_| Error::InvalidLocalPath { path })?;

                    yield Ok(FileMeta { url, size: entry.metadata()?.len() });
                }
            })
        })
        .await
        .map_err(io::Error::from)?
    }

    async fn remove(&self, url: &Url) -> Result<(), Error> {
        let path = url
            .to_file_path()
            .map_err(|_| Error::InvalidLocalUrl { url: url.clone() })?;

        remove_file(&path).await?;

        Ok(())
    }
}

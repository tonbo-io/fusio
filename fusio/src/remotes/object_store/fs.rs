use std::sync::Arc;

use async_stream::stream;
use futures_core::Stream;
use futures_util::stream::StreamExt;
use object_store::{aws::AmazonS3, path::Path, ObjectStore};
use url::Url;

use crate::{
    fs::{FileMeta, Fs, OpenOptions, WriteMode},
    remotes::object_store::S3File,
    Error,
};

pub struct S3Store {
    inner: Arc<AmazonS3>,
}

impl Fs for S3Store {
    type File = S3File;

    async fn open_options(&self, url: &Url, options: OpenOptions) -> Result<Self::File, Error> {
        if let Some(WriteMode::Append) = options.write {
            return Err(Error::Unsupported);
        }
        Ok(S3File {
            inner: self.inner.clone(),
            path: Path::from_url_path(url)?,
            pos: 0,
        })
    }

    async fn create_dir_all(_: &Url) -> Result<(), Error> {
        Ok(())
    }

    async fn list(&self, url: &Url) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = Path::from_url_path(url)?;
        let mut stream = self.inner.list(Some(&path));

        Ok(stream! {
            while let Some(meta) = stream.next().await.transpose()? {
                yield Ok(FileMeta { url: meta.location.into(), size: meta.size as u64 });
            }
        })
    }

    async fn remove(&self, url: &Url) -> Result<(), Error> {
        let path = Path::from_url_path(url)?;
        self.inner.delete(&path).await?;

        Ok(())
    }
}

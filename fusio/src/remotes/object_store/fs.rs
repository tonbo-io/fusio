use std::sync::Arc;

use async_stream::stream;
use futures_core::Stream;
use futures_util::stream::StreamExt;
use object_store::{aws::AmazonS3, ObjectStore};

use crate::{
    fs::{FileMeta, Fs, OpenOptions, WriteMode},
    path::Path,
    remotes::object_store::S3File,
    Error,
};

pub struct S3Store {
    inner: Arc<AmazonS3>,
}

impl Fs for S3Store {
    type File = S3File;

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        if let Some(WriteMode::Append) = options.write {
            return Err(Error::Unsupported);
        }
        Ok(S3File {
            inner: self.inner.clone(),
            path: path.clone().into(),
        })
    }

    async fn create_dir(_: &Path) -> Result<(), Error> {
        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = path.clone().into();
        let mut stream = self.inner.list(Some(&path));

        Ok(stream! {
            while let Some(meta) = stream.next().await.transpose()? {
                yield Ok(FileMeta { path: meta.location.into() });
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path.clone().into();
        self.inner.delete(&path).await?;

        Ok(())
    }
}

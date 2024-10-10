use std::sync::Arc;

use async_stream::stream;
use futures_core::Stream;
use futures_util::stream::StreamExt;
use object_store::{aws::AmazonS3, ObjectStore};

use crate::{
    fs::{FileMeta, Fs, OpenOptions, WriteMode},
    path::Path,
    remotes::object_store::{BoxedError, S3File},
    Error,
};

pub struct S3Store {
    inner: Arc<AmazonS3>,
}

impl From<AmazonS3> for S3Store {
    fn from(inner: AmazonS3) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl Fs for S3Store {
    type File = S3File;

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        if let Some(WriteMode::Append) = options.write {
            return Err(Error::Unsupported {
                message: "append mode is not supported in Amazon S3".into(),
            });
        }
        Ok(S3File {
            inner: self.inner.clone(),
            path: path.clone().into(),
            pos: 0,
        })
    }

    async fn create_dir_all(_: &Path) -> Result<(), Error> {
        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        let path = path.clone().into();
        let mut stream = self.inner.list(Some(&path));

        Ok(stream! {
            while let Some(meta) = stream.next().await.transpose().map_err(BoxedError::from)? {
                yield Ok(FileMeta { path: meta.location.into(), size: meta.size as u64 });
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path.clone().into();
        self.inner.delete(&path).await.map_err(BoxedError::from)?;

        Ok(())
    }
}

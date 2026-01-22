use std::sync::Arc;

use async_stream::stream;
use fusio::{
    error::Error,
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::Path,
};
use futures_core::Stream;
use futures_util::stream::StreamExt;
use object_store::ObjectStore;

use crate::{BoxedError, S3File};

pub struct S3Store<O: ObjectStore> {
    inner: Arc<O>,
}

impl<O: ObjectStore> From<O> for S3Store<O> {
    fn from(inner: O) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<O: ObjectStore> Fs for S3Store<O> {
    type File = S3File<O>;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::S3
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        if !options.truncate {
            return Err(Error::Unsupported {
                message: "append mode is not supported in Amazon S3".into(),
            });
        }
        Ok(S3File {
            inner: self.inner.clone(),
            path: path.clone().into(),
            buf: None,
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
            while let Some(meta) = stream.next().await.transpose().map_err(|err| Error::Remote(BoxedError::from(err)))? {
                yield Ok(FileMeta { path: meta.location.into(), size: meta.size });
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let path = path.clone().into();
        self.inner
            .delete(&path)
            .await
            .map_err(|err| Error::Remote(BoxedError::from(err)))?;

        Ok(())
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let from = from.clone().into();
        let to = to.clone().into();

        self.inner
            .copy(&from, &to)
            .await
            .map_err(|err| Error::Remote(BoxedError::from(err)))?;

        Ok(())
    }

    async fn link(&self, _: &Path, _: &Path) -> Result<(), Error> {
        Err(Error::Unsupported {
            message: "s3 does not support link file".to_string(),
        })
    }

    async fn exists(&self, path: &Path) -> Result<bool, Error> {
        let path = path.clone().into();

        match self.inner.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(Error::Remote(BoxedError::from(err))),
        }
    }
}

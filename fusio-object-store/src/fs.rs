use std::{future::Future, sync::Arc};

use async_stream::stream;
use fusio::{
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::Path,
    Error, MaybeSend,
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

    fn copy<F: Fs>(
        &self,
        from: &Path,
        to_fs: &F,
        to: &Path,
    ) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        todo!()
    }

    fn link<F: Fs>(
        &self,
        from: &Path,
        to_fs: &F,
        to: &Path,
    ) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        todo!()
    }
}

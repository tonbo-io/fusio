use std::sync::Arc;

use async_stream::stream;
use futures_core::Stream;
use futures_util::stream::StreamExt;
use object_store::{aws::AmazonS3, buffered::BufWriter, ObjectStore};

use crate::{
    fs::{FileMeta, Fs},
    path::Path,
    remotes::object_store::{S3File, S3FileWriter},
    Error,
};

pub struct S3Store {
    inner: Arc<AmazonS3>,
}

impl Fs for S3Store {
    type Read = S3File;
    type Write = S3FileWriter;

    async fn open_read(&self, path: &Path) -> Result<Self::Read, Error> {
        Ok(S3File {
            inner: self.inner.clone(),
            path: path.clone().into(),
        })
    }

    async fn open_write(&self, path: &Path) -> Result<Self::Write, Error> {
        Ok(S3FileWriter {
            inner: BufWriter::new(self.inner.clone(), path.clone().into()),
        })
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

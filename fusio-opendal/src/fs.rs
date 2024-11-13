use std::future::Future;

use fusio::{
    fs::{FileMeta, FileSystemTag, Fs, OpenOptions},
    path::Path,
    Error, MaybeSend,
};
use futures_core::Stream;
use futures_util::TryStreamExt;
use opendal::{Metakey, Operator};

use crate::{utils::parse_opendal_error, OpendalFile};

/// OpendalFs is a [`fusio::Fs`] compatible file system implementation.
///
/// It can be built from an [`opendal::Operator`] and provides file operations on top of it.
pub struct OpendalFs {
    op: Operator,
}

impl From<Operator> for OpendalFs {
    fn from(op: Operator) -> Self {
        Self { op }
    }
}

impl Fs for OpendalFs {
    type File = OpendalFile;

    fn file_system(&self) -> FileSystemTag {
        todo!()
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        OpendalFile::open(self.op.clone(), path.to_string(), options).await
    }

    /// FIXME: we need operator to perform this operation.
    async fn create_dir_all(_: &Path) -> Result<(), Error> {
        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        Ok(self
            .op
            .lister_with(path.as_ref())
            .metakey(Metakey::ContentLength)
            .await
            .map_err(parse_opendal_error)?
            .map_ok(|e| FileMeta {
                path: e.path().into(),
                size: e.metadata().content_length(),
            })
            .map_err(parse_opendal_error))
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        self.op
            .delete(path.as_ref())
            .await
            .map_err(parse_opendal_error)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        self.op
            .copy(from.as_ref(), to.as_ref())
            .await
            .map_err(parse_opendal_error)
    }

    async fn link(&self, from: &Path, to: &Path) -> Result<(), Error> {
        todo!()
    }
}

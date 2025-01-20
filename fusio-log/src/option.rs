use std::sync::Arc;

use fusio::DynFs;
pub use fusio_dispatch::FsOptions;
use futures_core::TryStream;

use crate::{error::LogError, Decode, Encode, Logger};

const DEFAULT_BUF_SIZE: usize = 4 * 1024;

#[derive(Clone)]
pub struct Options {
    pub(crate) path: fusio::path::Path,
    pub(crate) buf_size: usize,
    pub(crate) fs_option: FsOptions,
}

impl Options {
    pub fn new(path: fusio::path::Path) -> Self {
        Self {
            path,
            buf_size: DEFAULT_BUF_SIZE,
            fs_option: FsOptions::Local,
        }
    }

    pub fn disable_buf(self) -> Self {
        Self {
            buf_size: 0,
            ..self
        }
    }

    pub fn buf_size(self, buf_size: usize) -> Self {
        Self { buf_size, ..self }
    }

    pub fn fs(self, fs_option: FsOptions) -> Self {
        Self { fs_option, ..self }
    }

    pub async fn build<T>(self) -> Result<Logger<T>, LogError>
    where
        T: Encode,
    {
        let logger = Logger::<T>::new(self).await?;
        Ok(logger)
    }

    pub async fn build_with_fs<T>(self, fs: Arc<dyn DynFs>) -> Result<Logger<T>, LogError>
    where
        T: Encode,
    {
        let logger = Logger::<T>::with_fs(fs, self).await?;
        Ok(logger)
    }

    pub async fn recover<T>(
        self,
    ) -> Result<impl TryStream<Ok = Vec<T>, Error = LogError> + Unpin, LogError>
    where
        T: Decode,
    {
        Logger::<T>::recover(self).await
    }
}

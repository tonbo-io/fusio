use std::sync::Arc;

use fusio::DynFs;
pub use fusio_dispatch::FsOptions;
use futures_core::TryStream;

use crate::{error::LogError, Decode, Encode, Logger};

pub(crate) const DEFAULT_BUF_SIZE: usize = 4 * 1024;

#[cfg(not(any(
    feature = "tokio",
    feature = "web",
    feature = "monoio",
    feature = "aws"
)))]
compile_error!("one of these features must be enabled: tokio, monoio, web, aws");

#[cfg(all(
    feature = "aws",
    not(any(feature = "tokio-http", feature = "web-http", feature = "monoio-http"))
))]
compile_error!("aws feature must be used with tokio-http, monoio-http or web-http feature");

#[derive(Clone)]
pub struct Options {
    pub(crate) path: fusio::path::Path,
    pub(crate) buf_size: usize,
    pub(crate) fs_option: FsOptions,
    pub(crate) truncate: bool,
}

impl Options {
    /// Create a new log options.
    #[cfg(any(feature = "tokio", feature = "web", feature = "monoio"))]
    pub fn new(path: fusio::path::Path) -> Self {
        Self {
            path,
            buf_size: DEFAULT_BUF_SIZE,
            fs_option: FsOptions::Local,
            truncate: false,
        }
    }

    /// Create a new log options with [`FsOptions`].
    ///
    /// See [`FsOptions`] for more details.
    pub fn with_fs_options(path: fusio::path::Path, fs_option: FsOptions) -> Self {
        Self {
            path,
            buf_size: DEFAULT_BUF_SIZE,
            fs_option,
            truncate: false,
        }
    }

    /// Disable buffer for the log. It is recommended to keep the buffer enabled and use
    /// [`Logger::flush`] to flush the data to the log file.
    pub fn disable_buf(self) -> Self {
        Self {
            buf_size: 0,
            ..self
        }
    }

    /// Set the buffer size for the log. Default is 4K.
    pub fn buf_size(self, buf_size: usize) -> Self {
        Self { buf_size, ..self }
    }

    /// Set the filesystem options for the log.
    ///
    /// See [`FsOptions`] for more details.
    pub fn fs(self, fs_option: FsOptions) -> Self {
        Self { fs_option, ..self }
    }

    /// Open log file with truncate option.
    pub fn truncate(self, truncate: bool) -> Self {
        Self { truncate, ..self }
    }

    /// Open the log file. Return error if open file failed.
    pub async fn build<T>(self) -> Result<Logger<T>, LogError>
    where
        T: Encode,
    {
        let logger = Logger::<T>::new(self).await?;
        Ok(logger)
    }

    /// Open the log with the given [`DynFs`]. Return error if open file failed.
    pub async fn build_with_fs<T>(self, fs: Arc<dyn DynFs>) -> Result<Logger<T>, LogError>
    where
        T: Encode,
    {
        let logger = Logger::<T>::with_fs(fs, self).await?;
        Ok(logger)
    }

    /// Recover the log from existing log file. Return a stream of log entries.
    pub async fn recover<T>(
        self,
    ) -> Result<impl TryStream<Ok = Vec<T>, Error = LogError> + Unpin, LogError>
    where
        T: Decode,
    {
        Logger::<T>::recover(self).await
    }
}

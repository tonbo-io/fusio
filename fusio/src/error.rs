use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum Error {
    Io(#[from] io::Error),
    #[cfg(feature = "aws")]
    #[error(transparent)]
    S3Error(#[from] crate::remotes::aws::S3Error),
    #[error(transparent)]
    PathError(#[from] crate::path::Error),
    #[error("unsupported operation: {message}")]
    Unsupported {
        message: String,
    },
    #[error(transparent)]
    Other(#[from] BoxedError),
}

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

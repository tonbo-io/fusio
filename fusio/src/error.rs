use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum Error {
    Io(#[from] io::Error),
    #[cfg(feature = "http")]
    Http(#[from] http::Error),
    #[cfg(feature = "object_store")]
    ObjectStore(#[from] object_store::Error),
    Path(#[from] crate::path::Error),
    #[error("unsupported operation")]
    Unsupported,
    #[error(transparent)]
    Other(#[from] BoxedError),
    #[error("invalid url: {0}")]
    InvalidUrl(BoxedError),
    #[cfg(feature = "http")]
    #[error("http request failed, status: {status_code}, body: {body}")]
    HttpNotSuccess {
        status_code: http::StatusCode,
        body: String,
    },
}

#[allow(unused)]
pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

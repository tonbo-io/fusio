use std::{io, path::PathBuf};

use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum Error {
    Io(#[from] io::Error),
    #[error("Unable to convert url \"{url}\" to local path")]
    InvalidLocalUrl {
        url: Url,
    },
    #[error("Unable to convert path \"{path}\" to local url")]
    InvalidLocalPath {
        path: PathBuf,
    },
    #[cfg(feature = "http")]
    Http(#[from] http::Error),
    #[cfg(feature = "object_store")]
    ObjectStore(#[from] object_store::Error),
    #[cfg(feature = "object_store")]
    ObjectStorePath(#[from] object_store::path::Error),
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

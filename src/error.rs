use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
#[error(transparent)]
pub enum Error {
    Io(#[from] io::Error),
    #[cfg(feature = "http")]
    Http(#[from] hyper::http::Error),
}

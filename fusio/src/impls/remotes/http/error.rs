use thiserror::Error;

use crate::BoxedError;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum HttpError {
    #[error("HTTP request failed, status: {status}, body: {body}")]
    HttpNotSuccess {
        status: http::StatusCode,
        body: String,
    },
    #[error(transparent)]
    Http(#[from] http::Error),
    #[cfg(any(feature = "tokio-http", feature = "wasm-http"))]
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Url(#[from] url::ParseError),
    #[cfg(feature = "serde_urlencoded")]
    #[error(transparent)]
    UrlEncode(#[from] serde_urlencoded::ser::Error),
    #[error(transparent)]
    Other(#[from] BoxedError),
}

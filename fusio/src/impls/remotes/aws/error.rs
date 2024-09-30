use thiserror::Error;

use crate::remotes::{aws::credential::AuthorizeError, http::HttpError};

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("http error: {0}")]
    HttpError(#[from] HttpError),
    #[error("authorize error: {0}")]
    AuthorizeError(#[from] AuthorizeError),
    #[error("xml parse error: {0}")]
    XmlParseError(#[from] quick_xml::DeError),
}

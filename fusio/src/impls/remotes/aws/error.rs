use thiserror::Error;

use crate::remotes::{aws::credential::AuthorizeError, http::HttpError};

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("http error: {0}")]
    HttpError(#[from] HttpError),
    #[error("authorize error: {0}")]
    AuthorizeError(#[from] AuthorizeError),
    #[error("xml serialze error: {0}")]
    XmlSerializeError(#[from] quick_xml::SeError),
    #[error("xml deserialize error: {0}")]
    XmlDeserializeError(#[from] quick_xml::DeError),
}

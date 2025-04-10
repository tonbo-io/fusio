use fusio::BoxedError;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("log error")]
#[non_exhaustive]
pub enum LogError {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    #[cfg(feature = "aws")]
    #[error("S3 error: {0}")]
    S3Error(fusio::Error),
    #[error("encode error: {message}")]
    Encode { message: String },
    #[error("decode error: {message}")]
    Decode { message: String },
    #[error("recover error: bad data")]
    BadData,
    #[error("recover error: checksum does not match")]
    Checksum,
    #[error(transparent)]
    Other(#[from] BoxedError),
}

impl From<fusio::Error> for LogError {
    fn from(err: fusio::Error) -> Self {
        match err {
            fusio::Error::Io(error) => LogError::IO(error),
            #[cfg(feature = "aws")]
            fusio::Error::Remote(_) => LogError::S3Error(err),
            err => LogError::Other(Box::new(err)),
        }
    }
}

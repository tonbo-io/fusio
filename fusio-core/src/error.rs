use alloc::{boxed::Box, string::String};
use core::fmt::Debug;

use thiserror::Error;

pub type BoxedError = Box<dyn core::error::Error + Send + Sync + 'static>;

#[derive(Debug, Error)]
#[error(transparent)]
#[non_exhaustive]
pub enum Error {
    #[cfg(feature = "std")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Path(BoxedError),
    #[error(transparent)]
    Remote(BoxedError),
    #[error("unsupported operation: {message}")]
    Unsupported { message: String },
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("Performs dynamic cast failed.")]
    CastError,
    #[error("Error occurs in wasm: {message}")]
    Wasm { message: String },
    #[error(transparent)]
    Other(BoxedError),
}

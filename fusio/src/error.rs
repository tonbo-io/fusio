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
    #[error("Performs dynamic cast failed.")]
    CastError,
    #[error("Error occurs in wasm: {message}")]
    Wasm {
        message: String,
    },
    #[error(transparent)]
    Other(#[from] BoxedError),
    #[error(transparent)]
    Other2(#[from] fusio_core::Error),
}

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub(crate) fn wasm_err(js_val: js_sys::wasm_bindgen::JsValue) -> Error {
    Error::Wasm {
        message: format!("{js_val:?}"),
    }
}

// impl From<fusio_core::Error> for Error {
//     fn from(err: fusio_core::Error) -> Self {
//         match err {
//             fusio_core::Error::Io(error) => Error::Io(error),
//             fusio_core::Error::Unsupported { message } => Error::Unsupported { message },
//             fusio_core::Error::CastError => Error::CastError,
//             fusio_core::Error::Wasm { message } => Error::Wasm { message },
//             fusio_core::Error::Other(error) => Error::Other(error),
//             _ => todo!(),
//         }
//     }
// }
//
//

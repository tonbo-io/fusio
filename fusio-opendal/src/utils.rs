use std::io;

use fusio::error::Error;

/// Convert an `opendal::Error` into a `fusio::Error`.
pub fn parse_opendal_error(e: opendal::Error) -> Error {
    match e.kind() {
        opendal::ErrorKind::AlreadyExists => {
            Error::Io(io::Error::new(io::ErrorKind::AlreadyExists, e))
        }
        opendal::ErrorKind::PermissionDenied => {
            Error::Io(io::Error::new(io::ErrorKind::PermissionDenied, e))
        }
        opendal::ErrorKind::NotFound => Error::Io(io::Error::new(io::ErrorKind::NotFound, e)),
        _ => Error::Other(Box::new(e)),
    }
}

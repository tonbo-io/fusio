pub(crate) mod credential;
mod error;
#[cfg(feature = "fs")]
pub mod fs;
pub(crate) mod options;
mod s3;
pub(crate) mod sign;

pub use credential::AwsCredential;
pub use error::S3Error;
pub use s3::S3File;

const STRICT_ENCODE_SET: percent_encoding::AsciiSet = percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');
const STRICT_PATH_ENCODE_SET: percent_encoding::AsciiSet = STRICT_ENCODE_SET.remove(b'/');
const CHECKSUM_HEADER: &str = "x-amz-checksum-sha256";

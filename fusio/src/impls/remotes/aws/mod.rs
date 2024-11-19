pub mod credential;
mod error;
#[cfg(feature = "fs")]
pub mod fs;
pub(crate) mod multipart_upload;
pub(crate) mod options;
pub(crate) mod s3;
pub(crate) mod sign;
pub(crate) mod writer;

pub use credential::AwsCredential;
pub use error::S3Error;
pub use s3::S3File;
use serde::Deserialize;

const STRICT_ENCODE_SET: percent_encoding::AsciiSet = percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');
const STRICT_PATH_ENCODE_SET: percent_encoding::AsciiSet = STRICT_ENCODE_SET.remove(b'/');
const CHECKSUM_HEADER: &str = "x-amz-checksum-sha256";

#[derive(Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "PascalCase")]
pub(crate) struct S3ResponseError {
    pub code: String,
    pub message: String,
    pub resource: String,
    pub request_id: String,
}

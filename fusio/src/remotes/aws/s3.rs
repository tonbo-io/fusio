use hyper::Request;
use percent_encoding::utf8_percent_encode;

use crate::{Error, IoBufMut, Read};

use super::STRICT_PATH_ENCODE_SET;

pub struct S3 {
    bucket_endpoint: String,
    path: String,
}

impl S3 {
    pub fn new(bucket_endpoint: String, path: String) -> Self {
        Self {
            bucket_endpoint,
            path,
        }
    }
}

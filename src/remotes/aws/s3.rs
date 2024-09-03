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

impl Read for S3 {
    async fn read<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<usize, Error>, B) {
        let path = utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET);
        let url = format!("{}/{}", self.bucket_endpoint, path);
        match Request::builder().uri(url).body(()) {
            Ok(req) => todo!(),
            Err(e) => (Err(e.into()), buf),
        }
    }
}

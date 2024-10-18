use std::sync::Arc;

use bytes::{Buf, Bytes};
use http::{
    header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG},
    Method, Request, Response,
};
use http_body::Body;
use http_body_util::{BodyExt, Empty, Full};
use itertools::Itertools;
use percent_encoding::utf8_percent_encode;

use crate::{
    path::Path,
    remotes::{
        aws::{options::S3Options, sign::Sign, S3Error, S3ResponseError, STRICT_PATH_ENCODE_SET},
        http::{BoxBody, DynHttpClient, HttpClient},
        serde::{
            CompleteMultipartUploadRequest, CompleteMultipartUploadRequestPart,
            InitiateMultipartUploadResult, MultipartPart,
        },
    },
    Error,
};

pub(crate) struct MultipartUpload {
    options: Arc<S3Options>,
    path: Path,
    client: Arc<dyn DynHttpClient>,
}

impl MultipartUpload {
    pub fn new(options: Arc<S3Options>, path: Path, client: Arc<dyn DynHttpClient>) -> Self {
        Self {
            options,
            path,
            client,
        }
    }

    async fn check_response(response: Response<BoxBody>) -> Result<Response<BoxBody>, Error> {
        if !response.status().is_success() {
            return Err(Error::Other(
                format!(
                    "failed to write to S3, HTTP status: {} content: {}",
                    response.status(),
                    String::from_utf8_lossy(
                        &response.into_body().collect().await.unwrap().to_bytes()
                    )
                )
                .into(),
            ));
        }
        Ok(response)
    }

    async fn send_request<B>(&self, mut request: Request<B>) -> Result<Response<BoxBody>, Error>
    where
        B: Body<Data = Bytes> + Clone + Unpin + Send + Sync + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        request
            .sign(&self.options)
            .await
            .map_err(|e| Error::S3Error(S3Error::from(e)))?;
        let response = self
            .client
            .send_request(request)
            .await
            .map_err(|e| Error::S3Error(S3Error::from(e)))?;
        Self::check_response(response).await
    }

    pub(crate) async fn upload_once<B>(&self, size: usize, body: B) -> Result<(), Error>
    where
        B: Body<Data = Bytes> + Clone + Unpin + Send + Sync + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let url = format!(
            "{}/{}",
            self.options.endpoint,
            utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET)
        );
        let request = Request::builder()
            .uri(url)
            .method(Method::PUT)
            .header(CONTENT_LENGTH, size)
            .body(body)
            .map_err(|e| Error::Other(e.into()))?;
        let _ = self.send_request(request).await?;

        Ok(())
    }

    pub(crate) async fn initiate(&self) -> Result<String, Error> {
        let url = format!(
            "{}/{}?uploads",
            self.options.endpoint,
            utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET)
        );
        let request = Request::builder()
            .uri(url)
            .method(Method::POST)
            .body(Empty::new())
            .map_err(|e| Error::Other(e.into()))?;
        let response = self.send_request(request).await?;
        let result: InitiateMultipartUploadResult = quick_xml::de::from_reader(
            response
                .collect()
                .await
                .map_err(S3Error::from)?
                .aggregate()
                .reader(),
        )
        .map_err(S3Error::from)?;

        Ok(result.upload_id)
    }

    pub(crate) async fn upload_part<B>(
        &self,
        upload_id: &str,
        part_num: usize,
        size: usize,
        body: B,
    ) -> Result<MultipartPart, Error>
    where
        B: Body<Data = Bytes> + Clone + Unpin + Send + Sync + 'static,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let url = format!(
            "{}/{}?partNumber={}&uploadId={}",
            self.options.endpoint,
            utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET),
            part_num + 1,
            utf8_percent_encode(upload_id, &STRICT_PATH_ENCODE_SET),
        );
        let request = Request::builder()
            .uri(url)
            .method(Method::PUT)
            .header(CONTENT_LENGTH, size)
            .body(body)
            .map_err(|e| Error::Other(e.into()))?;
        let response = self.send_request(request).await?;
        let etag = response
            .headers()
            .get(ETAG)
            .ok_or_else(|| Error::Other("etag header not found".into()))?
            .to_str()
            .map_err(|e| Error::Other(e.into()))?;

        Ok(MultipartPart {
            part_num,
            etag: etag.to_string(),
        })
    }

    pub(crate) async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[MultipartPart],
    ) -> Result<(), Error> {
        let url = format!(
            "{}/{}?uploadId={}",
            self.options.endpoint,
            utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET),
            utf8_percent_encode(upload_id, &STRICT_PATH_ENCODE_SET),
        );
        let content = quick_xml::se::to_string(&CompleteMultipartUploadRequest {
            part: parts
                .iter()
                .map(|p| CompleteMultipartUploadRequestPart {
                    part_number: p.part_num + 1,
                    etag: p.etag.to_owned(),
                })
                .collect_vec(),
        })
        .map_err(S3Error::from)?;

        let request = Request::builder()
            .uri(url)
            .method(Method::POST)
            .header(CONTENT_LENGTH, content.len())
            .header(CONTENT_TYPE, "application/xml")
            .body(Full::new(Bytes::from(content)))
            .map_err(|e| Error::Other(e.into()))?;
        let response = self.send_request(request).await?;
        // still check if there is any error because S3 might return error for status code 200
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html#API_CompleteMultipartUpload_Example_4
        let (parts, body) = response.into_parts();
        let maybe_error: S3ResponseError = quick_xml::de::from_reader(
            body.collect()
                .await
                .map_err(S3Error::from)?
                .aggregate()
                .reader(),
        )
        .map_err(S3Error::from)?;
        if !maybe_error.code.is_empty() {
            return Err(Error::Other(
                format!("{:#?}, {:?}", parts, maybe_error).into(),
            ));
        }

        Ok(())
    }
}

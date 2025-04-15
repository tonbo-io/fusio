use std::sync::Arc;

use bytes::Buf;
use http::{
    header::{CONTENT_LENGTH, RANGE},
    request::Builder,
    Method, Request,
};
use http_body_util::{BodyExt, Empty};
use percent_encoding::utf8_percent_encode;

use super::{fs::AmazonS3, sign::Sign, S3Error, STRICT_PATH_ENCODE_SET};
use crate::{
    error::Error,
    path::Path,
    remotes::{
        aws::{multipart_upload::MultipartUpload, writer::S3Writer},
        http::{HttpClient, HttpError},
    },
    IoBuf, IoBufMut, Read, Write,
};

pub struct S3File {
    fs: AmazonS3,
    path: Path,
    writer: Option<S3Writer>,
}

impl S3File {
    pub(crate) fn new(fs: AmazonS3, path: Path) -> Self {
        Self {
            fs,
            path,
            writer: None,
        }
    }

    fn build_request(&self, method: Method) -> Builder {
        let url = format!(
            "{}/{}",
            self.fs.as_ref().options.endpoint,
            utf8_percent_encode(self.path.as_ref(), &STRICT_PATH_ENCODE_SET)
        );

        Request::builder().method(method).uri(url)
    }
}

impl Read for S3File {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let request = self
            .build_request(Method::GET)
            .header(
                RANGE,
                format!("bytes={}-{}", pos, pos + buf.as_slice().len() as u64 - 1),
            )
            .body(Empty::new())
            .map_err(|e| S3Error::from(HttpError::from(e)));

        let mut request = match request {
            Ok(request) => request,
            Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
        };

        if let Err(e) = request
            .sign(&self.fs.as_ref().options)
            .await
            .map_err(S3Error::from)
        {
            return (Err(Error::Remote(e.into())), buf);
        }

        let response = match self
            .fs
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(S3Error::from)
        {
            Ok(response) => response,
            Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
        };

        if !response.status().is_success() {
            return (
                Err(Error::Remote(Box::new(HttpError::HttpNotSuccess {
                    status: response.status(),
                    body: String::from_utf8_lossy(
                        &response
                            .into_body()
                            .collect()
                            .await
                            .map(|b| b.to_bytes())
                            .unwrap_or_default(),
                    )
                    .to_string(),
                }))),
                buf,
            );
        } else {
            match response.into_body().collect().await.map_err(S3Error::from) {
                Ok(body) => {
                    if let Err(e) = std::io::Read::read_exact(
                        &mut body.aggregate().reader(),
                        buf.as_slice_mut(),
                    ) {
                        return (Err(e.into()), buf);
                    }
                }
                Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
            }

            (Ok(()), buf)
        }
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let mut request = match self
            .build_request(Method::GET)
            .header(RANGE, format!("bytes={}-", pos))
            .body(Empty::new())
            .map_err(|e| S3Error::from(HttpError::from(e)))
        {
            Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
            Ok(request) => request,
        };

        if let Err(e) = request
            .sign(&self.fs.as_ref().options)
            .await
            .map_err(S3Error::from)
        {
            return (Err(Error::Other(Box::new(e))), buf);
        }

        let response = match self
            .fs
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(S3Error::from)
        {
            Ok(response) => response,
            Err(e) => return (Err(Error::Remote(Box::new(e))), buf),
        };

        if !response.status().is_success() {
            return (
                Err(Error::Remote(Box::new(HttpError::HttpNotSuccess {
                    status: response.status(),
                    body: String::from_utf8_lossy(
                        &response
                            .into_body()
                            .collect()
                            .await
                            .map(|b| b.to_bytes())
                            .unwrap_or_default(),
                    )
                    .to_string(),
                }))),
                buf,
            );
        } else {
            match response.into_body().collect().await.map_err(S3Error::from) {
                Ok(body) => {
                    let mut body = body.to_bytes();
                    buf.resize(body.len(), 0);
                    body.copy_to_slice(&mut buf[..]);
                    (Ok(()), buf)
                }
                Err(e) => (Err(Error::Remote(Box::new(e))), buf),
            }
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        let mut request = self
            .build_request(Method::HEAD)
            .body(Empty::new())
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;
        request
            .sign(&self.fs.as_ref().options)
            .await
            .map_err(|err| Error::Remote(Box::new(err)))?;

        let response = self
            .fs
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(|err| Error::Remote(Box::new(err)))?;

        if !response.status().is_success() {
            Err(Error::Other(Box::new(HttpError::HttpNotSuccess {
                status: response.status(),
                body: String::from_utf8_lossy(
                    &response
                        .into_body()
                        .collect()
                        .await
                        .map_err(|err| Error::Remote(Box::new(err)))?
                        .to_bytes(),
                )
                .to_string(),
            })))
        } else {
            let size = response
                .headers()
                .get(CONTENT_LENGTH)
                .ok_or_else(|| Error::Other("missing content-length header".into()))
                .map_err(|err| Error::Other(Box::new(err)))?
                .to_str()
                .map_err(|e| Error::Remote(e.into()))?
                .parse::<u64>()
                .map_err(|e| Error::Remote(e.into()))?;
            Ok(size)
        }
    }
}

impl Write for S3File {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        self.writer
            .get_or_insert_with(|| {
                #[allow(clippy::arc_with_non_send_sync)]
                S3Writer::new(Arc::new(MultipartUpload::new(
                    self.fs.clone(),
                    self.path.clone(),
                )))
            })
            .write_all(buf)
            .await
    }

    async fn flush(&mut self) -> Result<(), Error> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(mut writer) = self.writer.take() {
            writer.close().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
    #[tokio::test]
    async fn write_and_read_s3_file() {
        use std::sync::Arc;

        use crate::{
            remotes::{
                aws::{
                    credential::AwsCredential,
                    fs::{AmazonS3, AmazonS3Inner},
                    options::S3Options,
                    s3::S3File,
                },
                http::{tokio::TokioClient, DynHttpClient},
            },
            Read, Write,
        };

        if option_env!("AWS_ACCESS_KEY_ID").is_none()
            || option_env!("AWS_SECRET_ACCESS_KEY").is_none()
        {
            eprintln!("can not get `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`");
            return;
        }
        let key_id = std::option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = std::option_env!("AWS_SECRET_ACCESS_KEY")
            .unwrap()
            .to_string();

        dbg!("test write to s3");

        let client = TokioClient::new();
        let region = "ap-southeast-1";
        let options = S3Options {
            endpoint: "https://fusio-test.s3.ap-southeast-1.amazonaws.com".into(),
            bucket: "fusio-test".to_string(),
            credential: Some(AwsCredential {
                key_id,
                secret_key,
                token: None,
            }),
            region: region.into(),
            sign_payload: true,
            checksum: false,
        };

        let s3 = AmazonS3 {
            inner: Arc::new(AmazonS3Inner {
                options,
                client: Box::new(client) as Box<dyn DynHttpClient>,
            }),
        };

        {
            let mut s3 = S3File::new(s3.clone(), "read-write.txt".into());

            let (result, _) = s3
                .write_all(&b"The answer of life, universe and everthing"[..])
                .await;
            result.unwrap();
            s3.close().await.unwrap();
        }
        let mut s3 = S3File::new(s3, "read-write.txt".into());

        let size = s3.size().await.unwrap();
        assert_eq!(size, 42);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, b"The answer of life, universe and everthing");
    }

    #[ignore]
    #[cfg(all(feature = "monoio-http", feature = "completion-based"))]
    #[monoio::test(enable_timer = true)]
    async fn monoio_write_and_read_s3_file() {
        use crate::{
            remotes::aws::{credential::AwsCredential, fs::AmazonS3Builder, s3::S3File},
            Read, Write,
        };

        if option_env!("AWS_ACCESS_KEY_ID").is_none()
            || option_env!("AWS_SECRET_ACCESS_KEY").is_none()
        {
            eprintln!("can not get `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`");
            return;
        }
        let key_id = std::option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = std::option_env!("AWS_SECRET_ACCESS_KEY")
            .unwrap()
            .to_string();

        let s3 = AmazonS3Builder::new("fusio-test".into())
            .credential(AwsCredential {
                key_id,
                secret_key,
                token: None,
            })
            .region("ap-southeast-1".into())
            .sign_payload(true)
            .build();

        {
            let mut s3 = S3File::new(s3.clone(), "read-write.txt".into());

            let (result, _) = s3
                .write_all(&b"The answer of life, universe and everthing"[..])
                .await;
            result.unwrap();
            s3.close().await.unwrap();
        }
        let mut s3 = S3File::new(s3, "read-write.txt".into());

        let size = s3.size().await.unwrap();
        assert_eq!(size, 42);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, b"The answer of life, universe and everthing");
    }
}

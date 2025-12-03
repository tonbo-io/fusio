use bytes::Bytes;
use http::{
    header::{self, CONTENT_LENGTH},
    Method, Request,
};
use http_body_util::{BodyExt, Empty, Full};
use url::Url;

use crate::{
    error::Error,
    impls::remotes::{
        aws::{fs::AmazonS3, sign::Sign},
        http::{HttpClient, HttpError},
    },
};

/// Opaque ETag wrapper. Keep formatting verbatim (including quotes) as returned by S3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ETag(pub String);

impl AmazonS3 {
    /// Fetch object bytes and its ETag. Returns Ok(None) if object does not exist.
    pub async fn get_with_etag(&self, key: &str) -> Result<Option<(Bytes, ETag)>, Error> {
        let mut url = Url::parse(&self.as_ref().options.endpoint)
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;
        url = url
            .join(key)
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

        let mut req = Request::builder()
            .method(Method::GET)
            .uri(url.as_str())
            .header(CONTENT_LENGTH, 0)
            .body(Empty::<Bytes>::new())
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

        req.sign(&self.as_ref().options)
            .await
            .map_err(|e| Error::Remote(Box::new(e)))?;

        let resp = self
            .as_ref()
            .client
            .send_request(req)
            .await
            .map_err(|e| Error::Remote(Box::new(e)))?;

        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(Error::Remote(Box::new(HttpError::HttpNotSuccess {
                status: resp.status(),
                body: String::from_utf8_lossy(
                    &resp
                        .into_body()
                        .collect()
                        .await
                        .map_err(|e| Error::Remote(Box::new(e)))?
                        .to_bytes(),
                )
                .to_string(),
            })));
        }

        let headers = resp.headers().clone();
        let body = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| Error::Remote(Box::new(e)))?
            .to_bytes();

        #[cfg(target_arch = "wasm32")]
        if body.starts_with(b"<?xml") && String::from_utf8_lossy(&body).contains("NoSuchKey") {
            // Some S3-compatible gateways surface error documents with 200 in browsers when
            // fetch runs under restrictive modes. Treat these as missing keys so callers can
            // create fresh manifests.
            return Ok(None);
        }

        let etag = headers
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| ETag(s.to_string()))
            .ok_or_else(|| {
                Error::Other("missing ETag header in S3 response (ensure CORS exposes ETag)".into())
            })?;

        Ok(Some((body, etag)))
    }

    /// Create the object only if it does not exist yet via `If-None-Match: *`.
    pub async fn put_if_none_match(
        &self,
        key: &str,
        body: Bytes,
        content_type: Option<&str>,
        metadata: Option<&[(String, String)]>,
    ) -> Result<ETag, Error> {
        let mut url = Url::parse(&self.as_ref().options.endpoint)
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;
        url = url
            .join(key)
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

        let body_len = body.len();
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri(url.as_str())
            .header(header::IF_NONE_MATCH, "*")
            .header(CONTENT_LENGTH, body_len)
            .body(Full::new(body))
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

        if let Some(ct) = content_type {
            let hv = ct.parse().map_err(|e| Error::Remote(Box::new(e)))?;
            req.headers_mut().insert(header::CONTENT_TYPE, hv);
        }

        if let Some(entries) = metadata {
            for (name, value) in entries {
                let header_name: header::HeaderName =
                    name.parse().map_err(|e| Error::Remote(Box::new(e)))?;
                let header_value: header::HeaderValue =
                    value.parse().map_err(|e| Error::Remote(Box::new(e)))?;
                req.headers_mut().insert(header_name, header_value);
            }
        }

        req.sign(&self.as_ref().options)
            .await
            .map_err(|e| Error::Remote(Box::new(e)))?;

        let resp = self
            .as_ref()
            .client
            .send_request(req)
            .await
            .map_err(|e| Error::Remote(Box::new(e)))?;

        if resp.status().as_u16() == 412 {
            // Drain body to avoid dangling connection
            let _ = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| Error::Remote(Box::new(e)))?;
            return Err(Error::PreconditionFailed);
        }
        if !resp.status().is_success() {
            return Err(Error::Remote(Box::new(HttpError::HttpNotSuccess {
                status: resp.status(),
                body: String::from_utf8_lossy(
                    &resp
                        .into_body()
                        .collect()
                        .await
                        .map_err(|e| Error::Remote(Box::new(e)))?
                        .to_bytes(),
                )
                .to_string(),
            })));
        }

        let headers = resp.headers().clone();
        let etag = headers
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| ETag(s.to_string()))
            .ok_or_else(|| {
                Error::Other("missing ETag header in S3 response (ensure CORS exposes ETag)".into())
            })?;
        Ok(etag)
    }

    /// Replace the object only if the current ETag matches.
    pub async fn put_if_match(
        &self,
        key: &str,
        body: Bytes,
        etag: &ETag,
        content_type: Option<&str>,
        metadata: Option<&[(String, String)]>,
    ) -> Result<ETag, Error> {
        let mut url = Url::parse(&self.as_ref().options.endpoint)
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;
        url = url
            .join(key)
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

        let body_len = body.len();
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri(url.as_str())
            .header(header::IF_MATCH, etag.0.as_str())
            .header(CONTENT_LENGTH, body_len)
            .body(Full::new(body))
            .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

        if let Some(ct) = content_type {
            let hv = ct.parse().map_err(|e| Error::Remote(Box::new(e)))?;
            req.headers_mut().insert(header::CONTENT_TYPE, hv);
        }

        if let Some(entries) = metadata {
            for (name, value) in entries {
                let header_name: header::HeaderName =
                    name.parse().map_err(|e| Error::Remote(Box::new(e)))?;
                let header_value: header::HeaderValue =
                    value.parse().map_err(|e| Error::Remote(Box::new(e)))?;
                req.headers_mut().insert(header_name, header_value);
            }
        }

        req.sign(&self.as_ref().options)
            .await
            .map_err(|e| Error::Remote(Box::new(e)))?;

        let resp = self
            .as_ref()
            .client
            .send_request(req)
            .await
            .map_err(|e| Error::Remote(Box::new(e)))?;

        if resp.status().as_u16() == 412 {
            let _ = resp
                .into_body()
                .collect()
                .await
                .map_err(|e| Error::Remote(Box::new(e)))?;
            return Err(Error::PreconditionFailed);
        }
        if !resp.status().is_success() {
            return Err(Error::Remote(Box::new(HttpError::HttpNotSuccess {
                status: resp.status(),
                body: String::from_utf8_lossy(
                    &resp
                        .into_body()
                        .collect()
                        .await
                        .map_err(|e| Error::Remote(Box::new(e)))?
                        .to_bytes(),
                )
                .to_string(),
            })));
        }

        let headers = resp.headers().clone();
        let new_etag = headers
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| ETag(s.to_string()))
            .ok_or_else(|| {
                Error::Other("missing ETag header in S3 response (ensure CORS exposes ETag)".into())
            })?;
        Ok(new_etag)
    }
}

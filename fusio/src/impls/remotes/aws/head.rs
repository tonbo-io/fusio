use bytes::Bytes;
use http::{header, Method, Request};
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

/// Fetch object bytes and its ETag. Returns Ok(None) if object does not exist.
pub async fn get_with_etag(s3: &AmazonS3, key: &str) -> Result<Option<(Bytes, ETag)>, Error> {
    let mut url = Url::parse(&s3.as_ref().options.endpoint)
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;
    url = url
        .join(key)
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

    let mut req = Request::builder()
        .method(Method::GET)
        .uri(url.as_str())
        .body(Empty::<Bytes>::new())
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

    req.sign(&s3.as_ref().options)
        .await
        .map_err(|e| Error::Remote(Box::new(e)))?;

    let resp = s3
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

    let etag = resp
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| ETag(s.to_string()))
        .ok_or_else(|| Error::Other("missing ETag header in S3 response".into()))?;

    let body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| Error::Remote(Box::new(e)))?
        .to_bytes();

    Ok(Some((body, etag)))
}

/// Create the object only if it does not exist yet via `If-None-Match: *`.
pub async fn put_if_none_match(
    s3: &AmazonS3,
    key: &str,
    body: Bytes,
    content_type: Option<&str>,
) -> Result<ETag, Error> {
    let mut url = Url::parse(&s3.as_ref().options.endpoint)
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;
    url = url
        .join(key)
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

    let mut req = Request::builder()
        .method(Method::PUT)
        .uri(url.as_str())
        .header(header::IF_NONE_MATCH, "*")
        .body(Full::new(body))
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

    if let Some(ct) = content_type {
        let hv = ct.parse().map_err(|e| Error::Remote(Box::new(e)))?;
        req.headers_mut().insert(header::CONTENT_TYPE, hv);
    }

    req.sign(&s3.as_ref().options)
        .await
        .map_err(|e| Error::Remote(Box::new(e)))?;

    let resp = s3
        .as_ref()
        .client
        .send_request(req)
        .await
        .map_err(|e| Error::Remote(Box::new(e)))?;

    if resp.status().as_u16() == 412 {
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

    let etag = resp
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| ETag(s.to_string()))
        .ok_or_else(|| Error::Other("missing ETag header in S3 response".into()))?;
    Ok(etag)
}

/// Replace the object only if the current ETag matches.
pub async fn put_if_match(
    s3: &AmazonS3,
    key: &str,
    body: Bytes,
    etag: &ETag,
    content_type: Option<&str>,
) -> Result<ETag, Error> {
    let mut url = Url::parse(&s3.as_ref().options.endpoint)
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;
    url = url
        .join(key)
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

    let mut req = Request::builder()
        .method(Method::PUT)
        .uri(url.as_str())
        .header(header::IF_MATCH, etag.0.as_str())
        .body(Full::new(body))
        .map_err(|e| Error::Remote(Box::new(HttpError::from(e))))?;

    if let Some(ct) = content_type {
        let hv = ct.parse().map_err(|e| Error::Remote(Box::new(e)))?;
        req.headers_mut().insert(header::CONTENT_TYPE, hv);
    }

    req.sign(&s3.as_ref().options)
        .await
        .map_err(|e| Error::Remote(Box::new(e)))?;

    let resp = s3
        .as_ref()
        .client
        .send_request(req)
        .await
        .map_err(|e| Error::Remote(Box::new(e)))?;

    if resp.status().as_u16() == 412 {
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

    let new_etag = resp
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| ETag(s.to_string()))
        .ok_or_else(|| Error::Other("missing ETag header in S3 response".into()))?;
    Ok(new_etag)
}

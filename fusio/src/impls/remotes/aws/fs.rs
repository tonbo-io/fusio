use core::pin::Pin;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
};

use async_trait::async_trait;
use async_stream::stream;
use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use fusio_core::MaybeSendFuture;
use futures_core::Stream;
use futures_util::lock::Mutex as AsyncMutex;
use http::{
    header::{self, HOST},
    HeaderValue, Method, Request, StatusCode,
};
use http_body_util::{BodyExt, Empty};
use serde::{Deserialize, Serialize};
use url::Url;
use reqsign_core::{ProvideCredential, SignRequest};

use super::{
    context::default_context, credential::AwsCredential, options::S3Options, S3Error, S3File,
};
use crate::{
    error::Error,
    fs::{CasCondition, FileMeta, FileSystemTag, Fs, FsCas, OpenOptions},
    impls::remotes::aws::head::ETag,
    path::Path,
    remotes::{
        aws::{
            multipart_upload::{MultipartUpload, UploadType},
            sign::Sign,
        },
        http::{DynHttpClient, HttpClient, HttpError},
    },
};

#[derive(Debug, Clone)]
enum S3ExpressBaseCredentialProvider {
    Static(reqsign_aws_v4::Credential),
    Default,
}

#[derive(Debug, Clone)]
pub(crate) struct S3ExpressCredentialProvider {
    bucket: String,
    zone: String,
    region: String,
    base: S3ExpressBaseCredentialProvider,
}

type SharedS3ExpressSession = Arc<AsyncMutex<Option<reqsign_aws_v4::Credential>>>;

fn s3_express_session_cache() -> &'static Mutex<HashMap<String, SharedS3ExpressSession>> {
    static CACHE: OnceLock<Mutex<HashMap<String, SharedS3ExpressSession>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn is_s3_express_session_valid(credential: &reqsign_aws_v4::Credential) -> bool {
    if credential.access_key_id.is_empty() || credential.secret_access_key.is_empty() {
        return false;
    }

    credential
        .expires_in
        .is_none_or(|expires_at| expires_at > Utc::now() + chrono::TimeDelta::seconds(5))
}

impl S3ExpressCredentialProvider {
    fn debug_enabled() -> bool {
        std::env::var("FUSIO_S3_EXPRESS_DEBUG").as_deref() == Ok("1")
    }

    fn cache_key(&self, access_key_id: &str) -> String {
        format!(
            "{}:{}:{}:{}",
            self.bucket, self.zone, self.region, access_key_id
        )
    }

    fn session_slot(&self, access_key_id: &str) -> SharedS3ExpressSession {
        let mut cache = s3_express_session_cache()
            .lock()
            .expect("s3 express session cache lock poisoned");
        cache
            .entry(self.cache_key(access_key_id))
            .or_insert_with(|| Arc::new(AsyncMutex::new(None)))
            .clone()
    }

    async fn base_credential(
        &self,
        ctx: &reqsign_core::Context,
    ) -> reqsign_core::Result<reqsign_aws_v4::Credential> {
        match &self.base {
            S3ExpressBaseCredentialProvider::Static(cred) => Ok(cred.clone()),
            S3ExpressBaseCredentialProvider::Default => {
                reqsign_aws_v4::DefaultCredentialProvider::new()
                    .provide_credential(ctx)
                    .await?
                    .ok_or_else(|| {
                        reqsign_core::Error::unexpected(
                            "no AWS credential available for S3 Express session".to_string(),
                        )
                    })
            }
        }
    }

    async fn create_session(
        &self,
        ctx: &reqsign_core::Context,
        base_cred: &reqsign_aws_v4::Credential,
    ) -> reqsign_core::Result<reqsign_aws_v4::Credential> {
        let authority = format!(
            "{}.s3express-{}.{}.amazonaws.com",
            self.bucket, self.zone, self.region
        );
        let request = Request::builder()
            .method(Method::GET)
            .uri(format!("https://{authority}/?session"))
            .header(HOST, &authority)
            .header("x-amz-content-sha256", reqsign_aws_v4::EMPTY_STRING_SHA256)
            .header("x-amz-create-session-mode", "ReadWrite")
            .body(Bytes::new())
            .map_err(|err| {
                reqsign_core::Error::unexpected(format!(
                    "failed to build s3 express create-session request: {err}"
                ))
            })?;

        let (mut parts, body) = request.into_parts();
        let sign_cred = if let Some(token) = &base_cred.session_token {
            parts.headers.insert(
                "x-amz-security-token",
                HeaderValue::from_str(token).map_err(|err| {
                    reqsign_core::Error::unexpected(format!(
                        "failed to encode aws session token header: {err}"
                    ))
                })?,
            );
            let mut cred = base_cred.clone();
            cred.session_token = None;
            cred
        } else {
            base_cred.clone()
        };
        reqsign_aws_v4::RequestSigner::new("s3express", &self.region)
            .sign_request(ctx, &mut parts, Some(&sign_cred), None)
            .await?;

        let response = ctx.http_send(Request::from_parts(parts, body)).await?;
        let status = response.status();
        let body = response.into_body();
        if !status.is_success() {
            return Err(reqsign_core::Error::unexpected(format!(
                "s3 express CreateSession failed with status {status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }

        let parsed: CreateSessionResponse =
            quick_xml::de::from_reader(body.reader()).map_err(|err| {
                reqsign_core::Error::unexpected(format!(
                    "failed to parse s3 express CreateSession response: {err}"
                ))
            })?;

        if Self::debug_enabled() {
            eprintln!(
                "fusio s3-express: CreateSession bucket={} zone={} region={} expires_at={}",
                self.bucket, self.zone, self.region, parsed.credentials.expiration
            );
        }

        let expires_in = chrono::DateTime::parse_from_rfc3339(&parsed.credentials.expiration)
            .map_err(|err| {
                reqsign_core::Error::unexpected(format!(
                    "failed to parse s3 express session expiration: {err}"
                ))
            })?;

        Ok(reqsign_aws_v4::Credential {
            access_key_id: parsed.credentials.access_key_id,
            secret_access_key: parsed.credentials.secret_access_key,
            session_token: Some(parsed.credentials.session_token),
            expires_in: Some(expires_in.into()),
        })
    }
}

#[async_trait]
impl reqsign_core::ProvideCredential for S3ExpressCredentialProvider {
    type Credential = reqsign_aws_v4::Credential;

    async fn provide_credential(
        &self,
        ctx: &reqsign_core::Context,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        let base_cred = self.base_credential(ctx).await?;
        let session_slot = self.session_slot(&base_cred.access_key_id);
        let mut cached = session_slot.lock().await;

        if cached.as_ref().is_some_and(is_s3_express_session_valid) {
            return Ok(cached.clone());
        }

        let session = self.create_session(ctx, &base_cred).await?;
        *cached = Some(session.clone());
        Ok(Some(session))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename = "CreateSessionResult", rename_all = "PascalCase")]
struct CreateSessionResponse {
    credentials: CreateSessionCredentials,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateSessionCredentials {
    session_token: String,
    secret_access_key: String,
    access_key_id: String,
    expiration: String,
}

pub struct AmazonS3Builder {
    endpoint: Option<String>,
    region: String,
    bucket: String,
    credential: Option<AwsCredential>,
    use_default_credential_provider: bool,
    s3_express: bool,
    sign_payload: bool,
    checksum: bool,
    client: Box<dyn DynHttpClient>,
}

impl AmazonS3Builder {
    #[allow(unreachable_code, unused_variables)]
    pub fn new(bucket: String) -> Self {
        #[allow(clippy::needless_late_init, unused_variables)]
        let client: Box<dyn DynHttpClient>;
        cfg_if::cfg_if! {
            if #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))] {
                client = Box::new(crate::remotes::http::tokio::TokioClient::new());
            } else if #[cfg(all(feature = "web-http", not(feature = "completion-based")))]{
                client = Box::new(crate::remotes::http::wasm::WasmClient::new());
            } else if #[cfg(all(feature = "monoio-http", feature = "completion-based"))]{
                client = Box::new(crate::remotes::http::monoio::MonoioClient::new());
            } else {
                unreachable!()
            }
        }

        Self {
            endpoint: None,
            region: "us-east-1".into(),
            bucket,
            credential: None,
            use_default_credential_provider: false,
            s3_express: false,
            sign_payload: false,
            checksum: false,
            client,
        }
    }
}

impl AmazonS3Builder {
    pub fn region(mut self, region: String) -> Self {
        self.region = region;
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn credential(mut self, credential: AwsCredential) -> Self {
        self.credential = Some(credential);
        self
    }

    /// Enable S3 Express One Zone mode for directory buckets.
    ///
    /// When enabled, Fusio constructs zonal virtual-hosted endpoints and signs
    /// requests with short-lived `CreateSession` credentials instead of using
    /// path-style custom endpoints.
    pub fn s3_express(mut self, s3_express: bool) -> Self {
        self.s3_express = s3_express;
        self
    }

    /// Use the default credential provider chain (env, profile, SSO, IRSA, ECS, IMDS).
    ///
    /// Requires `tokio-http` or `web-http` feature for full functionality (IMDS, ECS,
    /// STS assume-role). Without an HTTP transport the provider chain falls back to
    /// environment variables only.
    #[cfg(any(feature = "tokio-http", feature = "web-http"))]
    pub fn default_credential_provider(mut self) -> Self {
        self.use_default_credential_provider = true;
        self
    }

    pub fn sign_payload(mut self, sign_payload: bool) -> Self {
        self.sign_payload = sign_payload;
        self
    }

    pub fn checksum(mut self, checksum: bool) -> Self {
        self.checksum = checksum;
        self
    }

    pub fn build(self) -> AmazonS3 {
        let trimmed_bucket = self.bucket.trim_start_matches('/');
        let endpoint = if let Some(endpoint) = self.endpoint {
            let trimmed_endpoint = endpoint.trim_end_matches('/');
            if self.s3_express {
                trimmed_endpoint.to_string()
            } else {
                format!("{}/{}/", trimmed_endpoint, trimmed_bucket)
            }
        } else if self.s3_express {
            format!(
                "https://{}.s3express-{}.{}.amazonaws.com",
                trimmed_bucket,
                s3_express_zone(trimmed_bucket).unwrap_or_default(),
                self.region
            )
        } else {
            format!(
                "https://{}.s3.{}.amazonaws.com",
                trimmed_bucket, self.region
            )
        };

        let s3_express_provider = match (
            &self.credential,
            self.use_default_credential_provider,
            self.s3_express,
        ) {
            (Some(cred), _, true) => Some(Arc::new(S3ExpressCredentialProvider {
                bucket: self.bucket.clone(),
                zone: s3_express_zone(trimmed_bucket).unwrap_or_default().to_string(),
                region: self.region.clone(),
                base: S3ExpressBaseCredentialProvider::Static(reqsign_aws_v4::Credential {
                    access_key_id: cred.key_id.clone(),
                    secret_access_key: cred.secret_key.clone(),
                    session_token: cred.token.clone(),
                    expires_in: None,
                }),
            })),
            (None, true, true) => Some(Arc::new(S3ExpressCredentialProvider {
                bucket: self.bucket.clone(),
                zone: s3_express_zone(trimmed_bucket).unwrap_or_default().to_string(),
                region: self.region.clone(),
                base: S3ExpressBaseCredentialProvider::Default,
            })),
            _ => None,
        };

        let signer = match (
            &self.credential,
            self.use_default_credential_provider,
            self.s3_express,
        ) {
            (Some(_), _, true) | (None, true, true) => None,
            (Some(cred), _, false) => {
                let mut provider =
                    reqsign_aws_v4::StaticCredentialProvider::new(&cred.key_id, &cred.secret_key);
                if let Some(token) = &cred.token {
                    provider = provider.with_session_token(token);
                }
                Some(reqsign_core::Signer::new(
                    default_context(),
                    provider,
                    reqsign_aws_v4::RequestSigner::new("s3", &self.region),
                ))
            }
            (None, true, false) => Some(reqsign_core::Signer::new(
                default_context(),
                reqsign_aws_v4::DefaultCredentialProvider::new(),
                reqsign_aws_v4::RequestSigner::new("s3", &self.region),
            )),
            (None, false, _) => None,
        };

        AmazonS3 {
            #[allow(clippy::arc_with_non_send_sync)]
            inner: Arc::new(AmazonS3Inner {
                options: S3Options {
                    endpoint,
                    bucket: self.bucket,
                    signer,
                    s3_express_provider,
                    s3_express_region: self.s3_express.then(|| self.region.clone()),
                    sign_payload: self.sign_payload,
                    checksum: self.checksum,
                },
                client: self.client,
            }),
        }
    }
}

fn s3_express_zone(bucket: &str) -> Option<&str> {
    let mut parts = bucket.rsplitn(3, "--");
    let suffix = parts.next()?;
    let zone = parts.next()?;
    if suffix != "x-s3" || zone.is_empty() {
        return None;
    }
    Some(zone)
}

#[derive(Clone)]
pub struct AmazonS3 {
    pub(super) inner: Arc<AmazonS3Inner>,
}

#[derive(Debug, Clone)]
pub struct HeadObject {
    pub size: u64,
    pub etag: Option<String>,
    pub metadata: HashMap<String, String>,
    pub headers: Vec<(String, String)>,
}

impl AsRef<AmazonS3Inner> for AmazonS3 {
    fn as_ref(&self) -> &AmazonS3Inner {
        self.inner.as_ref()
    }
}

pub(super) struct AmazonS3Inner {
    pub(super) options: S3Options,
    pub(super) client: Box<dyn DynHttpClient>,
}

impl AmazonS3 {
    #[allow(dead_code)]
    pub(crate) fn new(client: Box<dyn DynHttpClient>, options: S3Options) -> Self {
        AmazonS3 {
            #[allow(clippy::arc_with_non_send_sync)]
            inner: Arc::new(AmazonS3Inner { options, client }),
        }
    }

    pub async fn head_object(&self, path: &Path) -> Result<Option<HeadObject>, Error> {
        let mut url = Url::from_str(self.as_ref().options.endpoint.as_str())
            .map_err(|e| S3Error::from(HttpError::from(e)))
            .map_err(|e| Error::Remote(Box::new(e)))?;
        url = url
            .join(path.as_ref())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;

        let mut request = Request::builder()
            .method(Method::HEAD)
            .uri(url.as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;
        request
            .sign(&self.as_ref().options)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        let response = self
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        if response.status() == StatusCode::NOT_FOUND {
            let _ = response
                .into_body()
                .collect()
                .await
                .map_err(|e| Error::Remote(e.into()))?;
            return Ok(None);
        }

        if !response.status().is_success() {
            return Err(Error::Remote(
                HttpError::HttpNotSuccess {
                    status: response.status(),
                    body: String::from_utf8_lossy(
                        &response
                            .into_body()
                            .collect()
                            .await
                            .map_err(|e| Error::Remote(e.into()))?
                            .to_bytes(),
                    )
                    .to_string(),
                }
                .into(),
            ));
        }

        let headers = response.headers().clone();
        let size = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|hv| hv.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let etag = headers
            .get(header::ETAG)
            .and_then(|hv| hv.to_str().ok())
            .map(|s| s.to_string());

        let mut metadata = HashMap::new();
        for (name, value) in headers.iter() {
            let lower = name.as_str().to_ascii_lowercase();
            if lower.starts_with("x-amz-meta-") {
                if let Ok(val) = value.to_str() {
                    metadata.insert(lower.clone(), val.to_string());
                }
            }
        }

        let mut header_pairs = Vec::new();
        for (name, value) in headers.iter() {
            if let Ok(val) = value.to_str() {
                header_pairs.push((name.as_str().to_string(), val.to_string()));
            }
        }

        let _ = response
            .into_body()
            .collect()
            .await
            .map_err(|e| Error::Remote(e.into()))?;

        Ok(Some(HeadObject {
            size,
            etag,
            metadata,
            headers: header_pairs,
        }))
    }
}

impl Fs for AmazonS3 {
    type File = S3File;

    fn file_system(&self) -> FileSystemTag {
        FileSystemTag::S3
    }

    async fn open_options(&self, path: &Path, options: OpenOptions) -> Result<Self::File, Error> {
        let mut file = S3File::new(self.clone(), path.clone(), options.create || options.write);

        if options.write && !options.truncate {
            file.prefill_existing().await?;
        }

        Ok(file)
    }

    async fn create_dir_all(_path: &Path) -> Result<(), Error> {
        Ok(())
    }

    async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = Result<FileMeta, Error>>, Error> {
        Ok(stream! {
            let mut next_token = None::<String>;
            loop {
                let mut path = path.to_string();
                if self.as_ref().options.endpoint.contains(".s3express-")
                    && !path.is_empty()
                    && !path.ends_with('/')
                {
                    path.push('/');
                }
                let mut query = vec![("list-type", "2"), ("prefix", path.as_str())];
                if let Some(token) = next_token.as_ref() {
                    query.push(("continuation-token", token.as_str()));
                }

                let mut url = Url::from_str(self.as_ref().options.endpoint.as_str())
                    .map_err(|e| S3Error::from(HttpError::from(e)))
                    .map_err(|err| Error::Remote(Box::new(err)))?;
                let result = {
                    let mut pairs = url.query_pairs_mut();
                    let serializer = serde_urlencoded::Serializer::new(&mut pairs);
                    query
                        .serialize(serializer)
                        .map(|_| ())
                };
                result.map_err(|e| S3Error::from(HttpError::from(e))).map_err(|err| Error::Remote(Box::new(err)))?;

                let mut request = Request::builder()
                    .method(Method::GET)
                    .uri(url.as_str())
                    .body(Empty::<Bytes>::new())
                    .map_err(|e| S3Error::from(HttpError::from(e)))
                    .map_err(|err| Error::Remote(Box::new(err)))?;
                request.sign(&self.as_ref().options).await
                    .map_err(S3Error::from)
                    .map_err(|err| Error::Path(Box::new(err)))?;
                let response = self.as_ref().client.send_request(request).await
                    .map_err(S3Error::from)
                    .map_err(|err| Error::Path(Box::new(err)))?;

                if !response.status().is_success() {
                    if S3ExpressCredentialProvider::debug_enabled() {
                        eprintln!(
                            "fusio s3-express: list failed url={} status={}",
                            url,
                            response.status()
                        );
                    }
                    yield Err(Error::Other(Box::new(HttpError::HttpNotSuccess {
                        status: response.status(),
                        body: String::from_utf8_lossy(
                            &response
                                .collect()
                                .await
                                .map_err(|e| Error::Remote(e.into()))?
                                .to_bytes()
                        ).to_string()
                    })));
                    return;
                }

                let mut response: ListResponse = quick_xml::de::from_reader(
                    response
                    .collect()
                    .await
                    .map_err(|e| Error::Remote(e.into()))?
                    .aggregate().reader()
                ).map_err(|err| Error::Remote(err.into()))?;

                next_token = response.next_continuation_token.take();

                for content in &response.contents {
                    yield Ok(FileMeta {
                        path: Path::parse(&content.key).map_err(|err| Error::Path(Box::new(err)))?,
                        size: content.size as u64
                    });
                }

                if next_token.is_none() {
                    break;
                }
            }
        })
    }

    async fn remove(&self, path: &Path) -> Result<(), Error> {
        let mut url = Url::from_str(self.as_ref().options.endpoint.as_str())
            .map_err(|e| S3Error::from(HttpError::from(e)))
            .map_err(|e| Error::Remote(e.into()))?;
        url = url
            .join(path.as_ref())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;

        let mut request = Request::builder()
            .method(Method::DELETE)
            .uri(url.as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;
        request
            .sign(&self.as_ref().options)
            .await
            .map_err(|err| Error::Remote(err.into()))?;
        let response = self
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        if !response.status().is_success() {
            return Err(Error::Remote(
                HttpError::HttpNotSuccess {
                    status: response.status(),
                    body: String::from_utf8_lossy(
                        &response
                            .collect()
                            .await
                            .map_err(|e| Error::Remote(e.into()))?
                            .to_bytes(),
                    )
                    .to_string(),
                }
                .into(),
            ));
        }

        Ok(())
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        let upload = MultipartUpload::new(self.clone(), to.clone());
        upload
            .upload_once(
                UploadType::Copy {
                    bucket: self.inner.options.bucket.clone(),
                    from: from.clone(),
                    body: Empty::<Bytes>::new(),
                },
                None,
            )
            .await?;

        Ok(())
    }

    async fn link(&self, _: &Path, _: &Path) -> Result<(), Error> {
        Err(Error::Unsupported {
            message: "s3 does not support link file".to_string(),
        })
    }

    async fn exists(&self, path: &Path) -> Result<bool, Error> {
        let mut url = Url::from_str(self.as_ref().options.endpoint.as_str())
            .map_err(|e| S3Error::from(HttpError::from(e)))
            .map_err(|e| Error::Remote(Box::new(e)))?;
        url = url
            .join(path.as_ref())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;

        let mut request = Request::builder()
            .method(Method::HEAD)
            .uri(url.as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|e| Error::Remote(HttpError::from(e).into()))?;

        request
            .sign(&self.as_ref().options)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        let response = self
            .as_ref()
            .client
            .send_request(request)
            .await
            .map_err(|err| Error::Remote(err.into()))?;

        let is_not_found = response.status() == StatusCode::NOT_FOUND;

        let _ = response
            .into_body()
            .collect()
            .await
            .map_err(|e| Error::Remote(e.into()))?;

        Ok(is_not_found)
    }
}

impl FsCas for AmazonS3 {
    fn load_with_tag(
        &self,
        path: &Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(Vec<u8>, String)>, Error>> + '_>> {
        let key = path.to_string();
        Box::pin(async move {
            match self.get_with_etag(&key).await? {
                Some((bytes, etag)) => Ok(Some((bytes.to_vec(), etag.0))),
                None => Ok(None),
            }
        })
    }

    fn put_conditional(
        &self,
        path: &Path,
        payload: &[u8],
        content_type: Option<&str>,
        metadata: Option<Vec<(String, String)>>,
        condition: CasCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<String, Error>> + '_>> {
        let key = path.to_string();
        let ct = content_type.map(|s| s.to_string());
        let payload = Bytes::copy_from_slice(payload);
        let metadata = metadata.unwrap_or_default();
        Box::pin(async move {
            let ct_ref = ct.as_deref();
            let metadata_ref = if metadata.is_empty() {
                None
            } else {
                Some(metadata.as_slice())
            };
            let result = match condition {
                CasCondition::IfNotExists => {
                    self.put_if_none_match(&key, payload, ct_ref, metadata_ref)
                        .await
                }
                CasCondition::IfMatch(tag) => {
                    let etag = ETag(tag);
                    self.put_if_match(&key, payload, &etag, ct_ref, metadata_ref)
                        .await
                }
            }?;
            Ok(result.0)
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListPrefix {
    pub prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListContents {
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
    #[serde(rename = "ETag")]
    pub e_tag: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListResponse {
    #[serde(default)]
    pub contents: Vec<ListContents>,
    #[serde(default)]
    pub common_prefixes: Vec<ListPrefix>,
    #[serde(default)]
    pub next_continuation_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::s3_express_zone;
    #[cfg(feature = "tokio-http")]
    use crate::{fs::Fs, path::Path};

    #[test]
    fn s3_express_zone_suffix_is_parsed() {
        assert_eq!(
            s3_express_zone("tonbo-bench-use1-az6-20260331--use1-az6--x-s3"),
            Some("use1-az6")
        );
        assert_eq!(s3_express_zone("plain-bucket"), None);
    }

    #[test]
    fn standard_custom_endpoint_remains_path_style() {
        let s3 = super::AmazonS3Builder::new("bucket".to_string())
            .endpoint("https://example.com".to_string())
            .build();
        assert_eq!(s3.as_ref().options.endpoint, "https://example.com/bucket/");
    }

    #[test]
    fn s3_express_endpoint_uses_bucket_host() {
        let s3 = super::AmazonS3Builder::new("bucket--use1-az6--x-s3".to_string())
            .region("us-east-1".to_string())
            .s3_express(true)
            .build();
        assert_eq!(
            s3.as_ref().options.endpoint,
            "https://bucket--use1-az6--x-s3.s3express-use1-az6.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn s3_express_custom_endpoint_is_not_rewritten() {
        let s3 = super::AmazonS3Builder::new("bucket--use1-az6--x-s3".to_string())
            .endpoint(
                "https://bucket--use1-az6--x-s3.s3express-use1-az6.us-east-1.amazonaws.com"
                    .to_string(),
            )
            .s3_express(true)
            .build();
        assert_eq!(
            s3.as_ref().options.endpoint,
            "https://bucket--use1-az6--x-s3.s3express-use1-az6.us-east-1.amazonaws.com"
        );
    }

    #[cfg(feature = "tokio-http")]
    #[tokio::test]
    async fn list_and_remove() {
        use std::{env, pin::pin};

        use fusio_core::Write;
        use futures_util::StreamExt;

        use super::*;

        if env::var("AWS_ACCESS_KEY_ID").is_err() {
            eprintln!("skipping AWS s3 test");
            return;
        }
        let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        let bucket = std::option_env!("BUCKET_NAME")
            .expect("expected bucket not to be empty")
            .to_string();
        let region = std::option_env!("AWS_REGION")
            .expect("expected region not to be empty")
            .to_string();
        let token = std::option_env!("AWS_SESSION_TOKEN").map(|v| v.to_string());

        let s3 = AmazonS3Builder::new(bucket)
            .credential(AwsCredential {
                key_id,
                secret_key,
                token,
            })
            .region(region)
            .sign_payload(true)
            .build();

        let dir = Path::parse("list").unwrap();
        {
            let file_path = dir.child("file");
            let mut file = s3
                .open_options(
                    &file_path,
                    OpenOptions::default().create(true).truncate(true),
                )
                .await
                .unwrap();
            file.close().await.unwrap();
        }
        let mut stream = pin!(s3.list(&dir).await.unwrap());
        while let Some(meta) = stream.next().await {
            let meta = meta.unwrap();
            s3.remove(&meta.path).await.unwrap();
        }
    }

    #[ignore]
    #[cfg(feature = "tokio-http")]
    #[tokio::test]
    async fn s3_express_paged_list_probe() {
        use std::{env, pin::pin, time::{SystemTime, UNIX_EPOCH}};

        use fusio_core::Write;
        use futures_util::StreamExt;

        use super::*;

        let bucket = match env::var("TONBO_S3_BUCKET") {
            Ok(value) if !value.is_empty() => value,
            _ => {
                eprintln!("skipping S3 Express paged list probe");
                return;
            }
        };
        let key_id = env::var("TONBO_S3_ACCESS_KEY").expect("TONBO_S3_ACCESS_KEY");
        let secret_key = env::var("TONBO_S3_SECRET_KEY").expect("TONBO_S3_SECRET_KEY");
        let region = env::var("TONBO_S3_REGION").expect("TONBO_S3_REGION");
        let endpoint = env::var("TONBO_S3_ENDPOINT").ok();
        let token = env::var("TONBO_S3_SESSION_TOKEN").ok();
        let probe_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_secs();

        let mut builder = AmazonS3Builder::new(bucket)
            .credential(AwsCredential {
                key_id,
                secret_key,
                token,
            })
            .region(region)
            .s3_express(true)
            .sign_payload(true);
        if let Some(endpoint) = endpoint {
            builder = builder.endpoint(endpoint);
        }
        let s3 = builder.build();

        let dir = Path::parse(format!("probe/list-paged-{probe_id}")).expect("probe dir");
        for idx in 0..1_200usize {
            let file_path = dir.child(format!("file-{idx:04}.bin"));
            let mut file = s3
                .open_options(
                    &file_path,
                    OpenOptions::default().create(true).truncate(true),
                )
                .await
                .expect("open probe file");
            let (result, _) = file.write_all(vec![0u8; 1]).await;
            result.expect("write probe file");
            file.close().await.expect("close probe file");
        }

        let mut seen = 0usize;
        let mut stream = pin!(s3.list(&dir).await.expect("list probe dir"));
        while let Some(meta) = stream.next().await {
            let meta = meta.expect("list entry");
            seen = seen.saturating_add(1);
            s3.remove(&meta.path).await.expect("remove probe file");
        }

        assert_eq!(seen, 1_200);
    }

    #[ignore]
    #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
    #[tokio::test]
    async fn copy() {
        use crate::{
            remotes::aws::{credential::AwsCredential, fs::AmazonS3Builder, s3::S3File},
            Read, Write,
        };

        let s3 = AmazonS3Builder::new("data".to_string())
            .endpoint("http://localhost:9000".to_string())
            .region("ap-southeast-1".to_string())
            .credential(AwsCredential {
                key_id: "user".to_string(),
                secret_key: "password".to_string(),
                token: None,
            })
            .sign_payload(true)
            .checksum(true)
            .build();

        let from_path: Path = "read-write.txt".into();
        let to_path: Path = "read-write-copy.txt".into();
        {
            let mut s3 = S3File::new(s3.clone(), from_path.clone(), false);

            let (result, _) = s3
                .write_all(&b"The answer of life, universe and everthing"[..])
                .await;
            result.unwrap();
            s3.close().await.unwrap();
        }
        s3.copy(&from_path, &to_path).await.unwrap();
        let mut s3 = S3File::new(s3, to_path.clone(), false);

        let size = s3.size().await.unwrap();
        assert_eq!(size, 42);
        let buf = Vec::new();
        let (result, buf) = s3.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, b"The answer of life, universe and everthing");
    }
}

#[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
pub(crate) mod tokio;

use std::{future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use futures_core::Stream;
use http::{Request, Response};
use http_body::Body;
use http_body_util::BodyExt;

use crate::{dynamic::MaybeSendFuture, error::BoxedError, MaybeSend, MaybeSync};

pub trait HttpClient: MaybeSend + MaybeSync + 'static {
    type RespBody: Body<Data: Into<Bytes>, Error: std::error::Error + Send + Sync + 'static>
        + Send
        + MaybeSync
        + 'static;

    fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = Result<Response<Self::RespBody>, BoxedError>> + MaybeSend
    where
        B: Body + Send + MaybeSync + 'static,
        B::Data: Into<Bytes>,
        B::Error: Into<BoxedError>;
}

pub trait MaybeSendStream: Stream + Unpin + MaybeSend {}

// Avoid: https://github.com/rust-lang/rust/issues/130596
pub struct BoxError(BoxedError);

impl std::fmt::Debug for BoxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for BoxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<BoxedError> for BoxError {
    fn from(value: BoxedError) -> Self {
        Self(value)
    }
}

impl std::error::Error for BoxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

#[cfg(not(feature = "no-send"))]
pub type BoxBody = http_body_util::combinators::BoxBody<Bytes, BoxError>;
#[cfg(feature = "no-send")]
pub type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, BoxError>;

pub trait DynHttpClient: MaybeSend + MaybeSync {
    fn dyn_send_request(
        &self,
        request: Request<BoxBody>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Response<BoxBody>, BoxError>> + '_>>;
}

impl<C> DynHttpClient for C
where
    C: HttpClient,
{
    fn dyn_send_request(
        &self,
        request: Request<BoxBody>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Response<BoxBody>, BoxError>> + '_>> {
        Box::pin(async move {
            let response = self.send_request(request).await;
            match response {
                Ok(response) => {
                    let (parts, body) = response.into_parts();
                    Ok(Response::from_parts(
                        parts,
                        BoxBody::new(
                            body.map_frame(|f| f.map_data(|data| data.into()))
                                .map_err(|e| BoxError::from(Box::new(e) as BoxedError)),
                        ),
                    ))
                }
                Err(e) => return Err(e.into()),
            }
        })
    }
}

impl HttpClient for Arc<dyn DynHttpClient> {
    type RespBody = BoxBody;

    async fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<Self::RespBody>, BoxedError>
    where
        B: Body + Send + MaybeSync + 'static,
        B::Data: Into<Bytes>,
        B::Error: Into<BoxedError>,
    {
        let (parts, body) = request.into_parts();
        let request = Request::from_parts(
            parts,
            BoxBody::new(
                body.map_frame(|f| f.map_data(|data| data.into()))
                    .map_err(|e| BoxError::from(e.into() as BoxedError)),
            ),
        );
        let response = self.as_ref().dyn_send_request(request).await?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
    #[tokio::test]
    async fn test_tokio_client() {
        use bytes::Bytes;
        use http::{Request, StatusCode};
        use http_body_util::Empty;

        use super::{tokio::TokioClient, HttpClient};

        let request = Request::get("https://hyper.rs/")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let client = TokioClient::new();
        let response = client.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

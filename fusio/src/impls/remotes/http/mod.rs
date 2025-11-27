mod error;
#[cfg(all(feature = "monoio-http", feature = "completion-based"))]
pub mod monoio;
#[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
pub mod tokio;
#[cfg(all(feature = "wasm-http", not(feature = "completion-based")))]
pub mod wasm;

#[cfg(target_arch = "wasm32")]
use std::task::{Context, Poll};
use std::{future::Future, pin::Pin};

use bytes::Bytes;
pub use error::HttpError;
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use futures_core::Stream;
use http::{Request, Response};
use http_body::Body;
#[cfg(target_arch = "wasm32")]
use http_body::Frame;
use http_body_util::BodyExt;

use crate::error::BoxedError;

#[cfg(target_arch = "wasm32")]
pub trait HttpBody:
    Body<Data: Into<Bytes>, Error: Into<BoxedError>> + MaybeSend + MaybeSync + 'static
{
}
#[cfg(target_arch = "wasm32")]
impl<T> HttpBody for T where
    T: Body<Data: Into<Bytes>, Error: Into<BoxedError>> + MaybeSend + MaybeSync + 'static
{
}

#[cfg(not(target_arch = "wasm32"))]
pub trait HttpBody:
    Body<Data: Into<Bytes>, Error: Into<BoxedError>> + Send + MaybeSync + 'static
{
}
#[cfg(not(target_arch = "wasm32"))]
impl<T> HttpBody for T where
    T: Body<Data: Into<Bytes>, Error: Into<BoxedError>> + Send + MaybeSync + 'static
{
}

pub trait HttpClient: MaybeSend + MaybeSync {
    type RespBody: HttpBody;

    fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = Result<Response<Self::RespBody>, HttpError>> + MaybeSend
    where
        B: HttpBody;
}

pub trait MaybeSendStream: Stream + Unpin + MaybeSend {}

#[cfg(target_arch = "wasm32")]
pub trait WasmHttpBody:
    Body<Data = Bytes, Error = HttpError> + MaybeSend + MaybeSync + 'static
{
}
#[cfg(target_arch = "wasm32")]
impl<T> WasmHttpBody for T where
    T: Body<Data = Bytes, Error = HttpError> + MaybeSend + MaybeSync + 'static
{
}

#[cfg(target_arch = "wasm32")]
pub struct BoxBody {
    inner: Pin<Box<dyn WasmHttpBody>>,
}

#[cfg(target_arch = "wasm32")]
impl BoxBody {
    pub fn new<B>(body: B) -> Self
    where
        B: WasmHttpBody,
    {
        Self {
            inner: Box::pin(body),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl Body for BoxBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.get_mut().inner.as_mut().poll_frame(cx)
    }
}

#[cfg(all(not(target_arch = "wasm32"), not(feature = "no-send")))]
pub type BoxBody = http_body_util::combinators::BoxBody<Bytes, HttpError>;
#[cfg(all(not(target_arch = "wasm32"), feature = "no-send"))]
pub type BoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, HttpError>;

pub trait DynHttpClient: MaybeSend + MaybeSync {
    fn dyn_send_request(
        &self,
        request: Request<BoxBody>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Response<BoxBody>, HttpError>> + '_>>;
}

impl<C> DynHttpClient for C
where
    C: HttpClient,
{
    fn dyn_send_request(
        &self,
        request: Request<BoxBody>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Response<BoxBody>, HttpError>> + '_>> {
        Box::pin(async move {
            let response = self.send_request(request).await;
            match response {
                Ok(response) => {
                    let (parts, body) = response.into_parts();
                    Ok(Response::from_parts(
                        parts,
                        BoxBody::new(
                            body.map_frame(|f| f.map_data(|data| data.into()))
                                .map_err(|e| HttpError::from(e.into() as BoxedError)),
                        ),
                    ))
                }
                Err(e) => Err(e),
            }
        })
    }
}

impl HttpClient for Box<dyn DynHttpClient> {
    type RespBody = BoxBody;

    async fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<Self::RespBody>, HttpError>
    where
        B: HttpBody,
    {
        let (parts, body) = request.into_parts();
        let request = Request::from_parts(
            parts,
            BoxBody::new(
                body.map_frame(|f| f.map_data(|data| data.into()))
                    .map_err(|e| HttpError::from(e.into() as BoxedError)),
            ),
        );
        let response = self.as_ref().dyn_send_request(request).await?;
        Ok(response)
    }
}

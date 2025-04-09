mod error;
#[cfg(all(feature = "monoio-http", feature = "completion-based"))]
pub mod monoio;
#[cfg(all(feature = "tokio-http", not(feature = "completion-based")))]
pub mod tokio;
#[cfg(all(feature = "wasm-http", not(feature = "completion-based")))]
pub mod wasm;

use std::{future::Future, pin::Pin};

use bytes::Bytes;
pub use error::HttpError;
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use futures_core::Stream;
use http::{Request, Response};
use http_body::Body;
use http_body_util::BodyExt;

use crate::BoxedError;

pub trait HttpClient: MaybeSend + MaybeSync {
    type RespBody: Body<Data: Into<Bytes>, Error: Into<BoxedError>> + Send + MaybeSync + 'static;

    fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = Result<Response<Self::RespBody>, HttpError>> + MaybeSend
    where
        B: Body + Send + MaybeSync + 'static,
        B::Data: Into<Bytes>,
        B::Error: Into<BoxedError>;
}

pub trait MaybeSendStream: Stream + Unpin + MaybeSend {}

#[cfg(not(feature = "no-send"))]
pub type BoxBody = http_body_util::combinators::BoxBody<Bytes, HttpError>;
#[cfg(feature = "no-send")]
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
        B: Body + Send + MaybeSync + 'static,
        B::Data: Into<Bytes>,
        B::Error: Into<BoxedError>,
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

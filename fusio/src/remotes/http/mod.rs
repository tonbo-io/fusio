#[cfg(feature = "tokio-http")]
pub(crate) mod tokio;

use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::{Stream, TryStream};
use http::{Method, Request, Response};
use http_body::{Body, SizeHint};

use crate::error::BoxError;
use crate::{MaybeSend, MaybeSync};

pub trait HttpClient: MaybeSend + MaybeSync {
    type RespBody: Body<Data = Bytes, Error: std::error::Error + Send + Sync + 'static>
        + MaybeSend
        + 'static;

    fn send_request<E, B>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = Result<Response<Self::RespBody>, BoxError>> + MaybeSend
    where
        E: std::error::Error + Send + Sync + 'static,
        B: TryStream<Ok = Bytes, Error = E> + MaybeSend + 'static;

    fn get(
        &self,
        url: &str,
    ) -> impl Future<Output = Result<Response<Self::RespBody>, BoxError>> + MaybeSend {
        async move {
            let request = Request::get(url).method(Method::GET).body(Empty {})?;
            self.send_request(request).await
        }
    }
}

pub(crate) struct Empty;

impl Stream for Empty {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl Body for Empty {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(None)
    }

    fn is_end_stream(&self) -> bool {
        true
    }

    fn size_hint(&self) -> http_body::SizeHint {
        SizeHint::with_exact(0)
    }
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "tokio-http")]
    #[tokio::test]
    async fn test_tokio_client() {
        use http::{Request, StatusCode};

        use super::{tokio::TokioClient, Empty, HttpClient};

        let request = Request::get("https://hyper.rs/").body(Empty {}).unwrap();
        let client = TokioClient::new();
        let response = client.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

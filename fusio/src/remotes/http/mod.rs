#[cfg(all(feature = "tokio-http"))]
pub(crate) mod tokio;

use std::future::Future;

use bytes::Bytes;
use http::{Request, Response};
use http_body::Body;

use crate::{error::BoxError, MaybeSend, MaybeSync};

pub trait HttpClient: MaybeSend + MaybeSync {
    type RespBody: Body<Data = Bytes, Error: std::error::Error + Send + Sync + 'static>
        + MaybeSend
        + 'static;

    fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = Result<Response<Self::RespBody>, BoxError>> + MaybeSend
    where
        B: Body + MaybeSend + MaybeSync + 'static + std::fmt::Debug,
        B::Data: Into<Bytes>,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>;
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

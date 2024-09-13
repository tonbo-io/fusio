#[cfg(feature = "monoio-http")]
mod monoio;
#[cfg(feature = "tokio-http")]
mod tokio;

use bytes::Bytes;
use futures_core::TryStream;
use http::{Request, Response};
use http_body::Body;
use std::future::Future;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub trait HttpClient {
    type RespBody: Body<Data = Bytes> + Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    fn send_request<B: TryStream<Ok = Bytes, Error = BoxError> + Send + 'static>(
        &self,
        request: Request<B>,
    ) -> impl Future<Output = Result<Response<Self::RespBody>, Self::Error>>;
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "tokio-http")]
    #[tokio::test]
    async fn test_tokio_client() {
        use futures_util::stream;
        use http::Request;
        use http_body_util::BodyExt;

        use super::tokio::TokioClient;
        use super::HttpClient;

        let request = Request::get("https://hyper.rs/")
            .body(stream::iter(vec![]))
            .unwrap();
        let client = TokioClient::new();
        let response = client.send_request(request).await.unwrap();
        dbg!(response.into_body().collect().await.unwrap().to_bytes());
    }
}

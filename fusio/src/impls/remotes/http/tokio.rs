use bytes::Bytes;
use http::{Request, Response};
use http_body::Body;

use super::{HttpClient, HttpError};
use crate::{error::BoxedError, MaybeSync};

pub struct TokioClient {
    client: reqwest::Client,
}

impl Default for TokioClient {
    fn default() -> Self {
        Self::new()
    }
}

impl TokioClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl HttpClient for TokioClient {
    type RespBody = reqwest::Body;

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
        let request = Request::from_parts(parts, reqwest::Body::wrap(body));
        let request = reqwest::Request::try_from(request)?;
        let response = self.client.execute(request).await?;
        Ok(response.into())
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_tokio_client() {
        use bytes::Bytes;
        use http::{Request, StatusCode};
        use http_body_util::Empty;

        use super::{HttpClient, TokioClient};

        let request = Request::get("https://hyper.rs/")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let client = TokioClient::new();
        let response = client.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

use bytes::Bytes;
use futures_core::TryStream;
use http::{Request, Response};

use super::{BoxError, HttpClient};

pub struct TokioClient {
    client: reqwest::Client,
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

    type Error = reqwest::Error;

    async fn send_request<B: TryStream<Ok = Bytes, Error = BoxError> + Send + 'static>(
        &self,
        request: Request<B>,
    ) -> Result<Response<Self::RespBody>, Self::Error> {
        let (parts, body) = request.into_parts();
        let request = Request::from_parts(parts, reqwest::Body::wrap_stream(body));
        let request = reqwest::Request::try_from(request)?;
        let response = self.client.execute(request).await?;
        Ok(response.into())
    }
}

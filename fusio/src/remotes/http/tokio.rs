use bytes::Bytes;
use http::{Request, Response};
use http_body::Body;

use super::{BoxedError, HttpClient};
use crate::{MaybeSend, MaybeSync};

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

    async fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<Self::RespBody>, BoxedError>
    where
        B: Body + MaybeSend + MaybeSync + 'static,
        B::Data: Into<Bytes>,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let (parts, body) = request.into_parts();
        let request = Request::from_parts(parts, reqwest::Body::wrap(body));
        let request = reqwest::Request::try_from(request)?;
        let response = self.client.execute(request).await?;
        Ok(response.into())
    }
}

use bytes::Bytes;
use futures_core::TryStream;
use http::{Request, Response};

use super::{BoxError, HttpClient};
use crate::MaybeSend;

pub(crate) struct TokioClient {
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

    async fn send_request<E, B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<Self::RespBody>, BoxError>
    where
        E: std::error::Error + Send + Sync + 'static,
        B: TryStream<Ok = Bytes, Error = E> + MaybeSend + 'static,
    {
        let (parts, body) = request.into_parts();
        let request = Request::from_parts(parts, reqwest::Body::wrap_stream(body));
        let request = reqwest::Request::try_from(request)?;
        let response = self.client.execute(request).await?;
        Ok(response.into())
    }
}

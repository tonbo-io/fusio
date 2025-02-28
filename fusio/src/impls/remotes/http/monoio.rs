use bytes::Bytes;
use http::{request::Builder, HeaderValue, Request, Response, Version};
use http_body::Body;
use http_body_util::BodyExt;
use monoio_http::common::body::{Body as _, FixedBody, HttpBody};

use super::{HttpClient, HttpError};

#[derive(Default)]
pub struct MonoioClient {}

impl MonoioClient {
    pub fn new() -> Self {
        Self {}
    }
}

impl HttpClient for MonoioClient {
    type RespBody = http_body_util::Full<Bytes>;

    async fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<Self::RespBody>, HttpError>
    where
        B: Body + Send + crate::MaybeSync + 'static,
        B::Data: Into<bytes::Bytes>,
        B::Error: Into<crate::error::BoxedError>,
    {
        let uri = request.uri().clone();
        let (parts, body) = request.into_parts();

        let body: HttpBody = HttpBody::fixed_body(Some(
            body.collect()
                .await
                .map_err(|err| HttpError::Other(err.into()))?
                .to_bytes(),
        ));

        let mut req = Builder::new()
            .method(parts.method)
            .uri(parts.uri)
            .version(parts.version)
            .body(body)
            .unwrap();
        let headers = req.headers_mut();
        headers.insert(
            http::header::HOST,
            HeaderValue::from_str(uri.host().unwrap()).unwrap(),
        );
        for (header_name, header_value) in parts.headers.into_iter() {
            headers.insert(header_name.unwrap(), header_value);
        }

        let client = match parts.version {
            Version::HTTP_11 => {
                let headers = req.headers_mut();
                headers.insert(
                    http::header::HOST,
                    HeaderValue::from_str(uri.host().unwrap()).unwrap(),
                );
                monoio_http_client::Builder::new().http1_client().build()
            }
            Version::HTTP_2 => monoio_http_client::Builder::new().http2_client().build(),
            _ => todo!("unsupported request"),
        };

        let response = client.send_request(req).await.unwrap();
        let (parts, mut body) = response.into_parts();
        let mut buf = vec![];
        while let Some(Ok(data)) = body.next_data().await {
            buf.extend(data);
        }

        let mut resp_builder = Response::builder();
        let mut headers = resp_builder.headers_mut();
        for (name, value) in parts.headers.iter() {
            headers.as_mut().unwrap().append(name, value.clone());
        }

        resp_builder
            .body(http_body_util::Full::new(Bytes::from(buf)))
            .map_err(HttpError::Http)
    }
}

#[cfg(test)]
mod tests {

    #[monoio::test(enable_timer = true)]
    async fn test_monoio_client() {
        use bytes::Bytes;
        use http::{Request, StatusCode};
        use http_body_util::Empty;

        use super::{HttpClient, MonoioClient};

        let request = Request::get("https://hyper.rs/")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let client = MonoioClient {};
        let response = client.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

use std::str::FromStr;

use bytes::Bytes;
use http::{Request, Response};
use http_body::Body;
use http_body_util::BodyExt;

use super::{HttpClient, HttpError};
use crate::{error::BoxedError, MaybeSync};

#[derive(Default)]
pub struct WasmClient;

impl WasmClient {
    pub fn new() -> Self {
        Default::default()
    }
}

impl HttpClient for WasmClient {
    type RespBody = http_body_util::Full<Bytes>;

    async fn send_request<B>(
        &self,
        request: Request<B>,
    ) -> Result<Response<Self::RespBody>, HttpError>
    where
        B: Body + Send + MaybeSync + 'static,
        B::Data: Into<Bytes>,
        B::Error: Into<BoxedError>,
    {
        let uri = request.uri().clone();
        let (parts, body) = request.into_parts();

        let url = reqwest::Url::from_str(&uri.to_string())?;
        let body = http_body_util::combinators::UnsyncBoxBody::new(body);

        match body.collect().await {
            Ok(body) => {
                let client = reqwest::Client::new();

                let mut builder = client.request(parts.method, url);
                builder = builder.body(reqwest::Body::from(body.to_bytes()));
                let response = builder.send().await?;
                let bytes = response.bytes().await?;

                Ok(Response::new(http_body_util::Full::new(bytes)))
            }
            Err(err) => Err(HttpError::Other(err.into())),
        }
    }
}

#[cfg(feature = "wasm-http")]
#[cfg(test)]
mod tests {
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    async fn test_wasm_client() {
        use bytes::Bytes;
        use http::{Request, StatusCode};
        use http_body_util::Empty;

        use super::{HttpClient, WasmClient};

        let request = Request::get("https://jsonplaceholder.typicode.com/users")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let client = WasmClient::new();
        let response = client.send_request(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[cfg(all(feature = "wasm-http", feature = "aws"))]
    #[wasm_bindgen_test]
    async fn list_and_remove_wasm() {
        use std::pin::pin;

        use futures_util::StreamExt;

        use crate::{
            fs::Fs,
            path::Path,
            remotes::aws::{fs::AmazonS3Builder, AwsCredential},
        };

        if option_env!("AWS_ACCESS_KEY_ID").is_none() {
            eprintln!("skipping AWS s3 test");
            return;
        }
        let key_id = option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = option_env!("AWS_SECRET_ACCESS_KEY").unwrap().to_string();

        let s3 = AmazonS3Builder::new("fusio-test".into())
            .credential(AwsCredential {
                key_id,
                secret_key,
                token: None,
            })
            .region("ap-southeast-1".into())
            .sign_payload(true)
            .build();

        let path = Path::parse("test").unwrap();
        let mut stream = pin!(s3.list(&path).await.unwrap());
        while let Some(meta) = stream.next().await {
            let meta = meta.unwrap();
            s3.remove(&meta.path).await.unwrap();
        }
    }
}

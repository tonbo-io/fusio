use reqsign_core::Context;

cfg_if::cfg_if! {
    if #[cfg(all(feature = "tokio", not(target_arch = "wasm32")))] {
        #[derive(Debug)]
        pub(crate) struct TokioFileRead;

        impl reqsign_core::FileRead for TokioFileRead {
            fn file_read<'a, 'b, 'c>(
                &'a self,
                path: &'b str,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = reqsign_core::Result<Vec<u8>>> + Send + 'c>,
            >
            where
                'a: 'c,
                'b: 'c,
                Self: 'c,
            {
                Box::pin(async move { tokio::fs::read(path).await.map_err(Into::into) })
            }
        }

        #[derive(Debug)]
        pub(crate) struct TokioCommandExecute;

        impl reqsign_core::CommandExecute for TokioCommandExecute {
            fn command_execute<'a, 'b, 'c, 'd, 'fut>(
                &'a self,
                program: &'b str,
                args: &'c [&'d str],
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = reqsign_core::Result<reqsign_core::CommandOutput>>
                        + Send
                        + 'fut,
                >,
            >
            where
                'a: 'fut,
                'b: 'fut,
                'c: 'fut,
                'd: 'fut,
                Self: 'fut,
            {
                let program = program.to_owned();
                let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
                Box::pin(async move {
                    let output = tokio::process::Command::new(&program)
                        .args(&args)
                        .output()
                        .await
                        .map_err(|e| {
                            reqsign_core::Error::unexpected(format!(
                                "execute command {program}: {e}"
                            ))
                        })?;
                    Ok(reqsign_core::CommandOutput {
                        status: output.status.code().unwrap_or(-1),
                        stdout: output.stdout,
                        stderr: output.stderr,
                    })
                })
            }
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(all(feature = "tokio-http", not(target_arch = "wasm32")))] {
        #[derive(Debug)]
        pub(crate) struct ReqwestHttpSend {
            client: reqwest::Client,
        }

        impl Default for ReqwestHttpSend {
            fn default() -> Self {
                Self {
                    client: reqwest::Client::new(),
                }
            }
        }

        impl reqsign_core::HttpSend for ReqwestHttpSend {
            fn http_send<'a, 'b>(
                &'a self,
                req: http::Request<bytes::Bytes>,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = reqsign_core::Result<http::Response<bytes::Bytes>>,
                        > + Send
                        + 'b,
                >,
            >
            where
                'a: 'b,
                Self: 'b,
            {
                Box::pin(async move {
                    let (parts, body) = req.into_parts();
                    let resp = self
                        .client
                        .request(parts.method, parts.uri.to_string())
                        .headers(parts.headers)
                        .body(body)
                        .send()
                        .await
                        .map_err(|e| {
                            reqsign_core::Error::unexpected(format!("http request failed: {e}"))
                        })?;

                    let status = resp.status();
                    let headers = resp.headers().clone();
                    let body = resp.bytes().await.map_err(|e| {
                        reqsign_core::Error::unexpected(format!("read response body: {e}"))
                    })?;

                    let mut builder = http::Response::builder().status(status);
                    *builder.headers_mut().unwrap() = headers;
                    builder.body(body).map_err(|e| {
                        reqsign_core::Error::unexpected(format!("build http response: {e}"))
                    })
                })
            }
        }
    }
}

pub(crate) fn default_context() -> Context {
    #[allow(unused_mut)]
    let mut ctx = Context::new();

    cfg_if::cfg_if! {
        if #[cfg(all(feature = "tokio", not(target_arch = "wasm32")))] {
            ctx = ctx
                .with_file_read(TokioFileRead)
                .with_command_execute(TokioCommandExecute);
        }
    }

    cfg_if::cfg_if! {
        if #[cfg(all(feature = "tokio-http", not(target_arch = "wasm32")))] {
            ctx = ctx.with_http_send(ReqwestHttpSend::default());
        }
    }

    ctx.with_env(reqsign_core::OsEnv)
}

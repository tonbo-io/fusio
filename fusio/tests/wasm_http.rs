#[cfg(all(feature = "wasm-http", target_arch = "wasm32", test))]
pub(crate) mod tests {

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
    use wasm_bindgen_test::wasm_bindgen_test;

    #[cfg(feature = "aws")]
    #[wasm_bindgen_test]
    async fn test_read_write_s3_wasm() {
        use fusio::{
            fs::{Fs, OpenOptions},
            remotes::aws::{fs::AmazonS3Builder, AwsCredential},
            Read, Write,
        };

        if option_env!("AWS_ACCESS_KEY_ID").is_none() {
            return;
        }
        let key_id = option_env!("AWS_ACCESS_KEY_ID").unwrap().to_string();
        let secret_key = option_env!("AWS_SECRET_ACCESS_KEY").unwrap().to_string();

        let bucket = std::option_env!("BUCKET_NAME")
            .expect("expected bucket not to be empty")
            .to_string();
        let region = std::option_env!("AWS_REGION")
            .expect("expected region not to be empty")
            .to_string();
        let token = std::option_env!("AWS_SESSION_TOKEN").map(|v| v.to_string());

        let s3 = AmazonS3Builder::new(bucket)
            .credential(AwsCredential {
                key_id,
                secret_key,
                token,
            })
            .region(region)
            .sign_payload(true)
            .build();

        {
            let mut s3_file = s3
                .open_options(
                    &"wasm/read-write.txt".into(),
                    OpenOptions::default().write(true).truncate(true),
                )
                .await
                .unwrap();

            let (result, _) = s3_file
                .write_all(&b"The answer of life, universe and everthing"[..])
                .await;

            result.unwrap();

            s3_file.close().await.unwrap();
        }
        let mut s3_file = s3.open(&"read-write.txt".into()).await.unwrap();

        let size = s3_file.size().await.unwrap();
        assert_eq!(size, 42);
        let buf = Vec::new();
        let (result, buf) = s3_file.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf, b"The answer of life, universe and everthing");
    }
}

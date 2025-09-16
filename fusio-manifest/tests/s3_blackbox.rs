#![cfg(all(feature = "aws-tokio"))]

use std::env;

use fusio_manifest as manifest;

#[tokio::test]
#[ignore] // Requires real AWS credentials and an existing bucket
async fn s3_end_to_end_manifest() {
    use fusio::{fs::Fs as _, impls::remotes::aws::credential::AwsCredential};
    use manifest::s3;

    // Env config
    let key_id = match env::var("AWS_ACCESS_KEY_ID") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping: AWS_ACCESS_KEY_ID missing");
            return;
        }
    };
    let secret_key = match env::var("AWS_SECRET_ACCESS_KEY") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping: AWS_SECRET_ACCESS_KEY missing");
            return;
        }
    };
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let bucket = match std::option_env!("BUCKET_NAME") {
        Some(b) => b.to_string(),
        None => match env::var("BUCKET_NAME") {
            Ok(v) => v,
            Err(_) => {
                eprintln!("skipping: BUCKET_NAME missing");
                return;
            }
        },
    };
    let token = env::var("AWS_SESSION_TOKEN").ok();

    // Unique test prefix
    let prefix = format!(
        "fusio-manifest-blackbox/{}/{}",
        std::env::var("USER").unwrap_or_else(|_| "user".into()),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Open manifest via new S3 builder (no direct AmazonS3 construction)
    let cfg = s3::Builder::new(bucket)
        .prefix(prefix.clone())
        .credential(AwsCredential {
            key_id,
            secret_key,
            token,
        })
        .region(region)
        .sign_payload(true)
        .build();
    let kv: s3::S3Manifest<String, String> = cfg.clone().into();

    // Write
    let mut s = kv.session_write().await.unwrap();
    s.put("a".into(), "1".into()).unwrap();
    s.put("b".into(), "2".into()).unwrap();
    let _ = s.commit().await.unwrap();

    // Read
    let snap = kv.snapshot().await.unwrap();
    let got_a = kv.session_at(snap.clone()).get(&"a".into()).await.unwrap();
    assert_eq!(got_a.as_deref(), Some("1"));

    // Compact once (publishes a checkpoint)
    let (_ckpt, _tag) = kv.compact_once().await.unwrap();

    // Optional best-effort cleanup: remove HEAD object
    let _ = cfg
        .s3
        .remove(&fusio::path::Path::from(cfg.head_key()))
        .await;
}

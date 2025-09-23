// This test exercises the GC plan phases against real S3. It is ignored by default
// and requires valid AWS credentials and an existing bucket (see env vars).

use std::{env, sync::Arc, time::Duration};

use fusio_manifest as manifest;

#[tokio::test]
#[ignore]
async fn s3_gc_plan_end_to_end() {
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
        "fusio-manifest-gc/{}/{}",
        std::env::var("USER").unwrap_or_else(|_| "user".into()),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Options: make GC not_before immediate
    let mut opts = manifest::options::Options::default();
    let retention = manifest::retention::DefaultRetention::default()
        .with_segments_min_ttl(Duration::ZERO)
        .with_checkpoints_min_ttl(Duration::ZERO);
    opts.set_retention(Arc::new(retention));

    // Manifest for writing
    let cfg = s3::Builder::new(bucket)
        .prefix(prefix.clone())
        .credential(AwsCredential {
            key_id,
            secret_key,
            token,
        })
        .region(region)
        .sign_payload(true)
        .with_options(opts.clone())
        .build();
    let kv: s3::S3Manifest<String, String> = cfg.clone().into();
    let comp: s3::S3Compactor<String, String> = kv.compactor();

    // Seed two segments (two commits)
    let mut s1 = kv.session_write().await.unwrap();
    s1.put("a".into(), "1".into()).unwrap();
    let _ = s1.commit().await.unwrap();

    let mut s2 = kv.session_write().await.unwrap();
    s2.put("b".into(), "2".into()).unwrap();
    let _ = s2.commit().await.unwrap();

    // Compact to create a checkpoint
    let (_ckpt_id, _tag) = comp.compact_once().await.unwrap();

    // Create a pinned read lease so watermark reflects current HEAD
    let _reader = kv.session_read().await.unwrap();

    // Compute
    let _ = comp.gc_compute().await.unwrap();
    // Apply
    let _ = comp.gc_apply().await.unwrap();
    // Delete + reset
    let _ = comp.gc_delete_and_reset().await.unwrap();

    // Cleanup: remove HEAD object (best-effort)
    let _ = cfg
        .s3
        .remove(&fusio::path::Path::from(cfg.head_key()))
        .await;
}

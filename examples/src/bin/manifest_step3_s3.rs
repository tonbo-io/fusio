use std::env;

use fusio_manifest::{
    options::Options,
    s3,
    types::{Error, Result},
    BackoffPolicy,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let setup = match load_env("manifest-step3")? {
        Some(cfg) => cfg,
        None => return Ok(()),
    };

    let opts = Options::default().with_backoff(BackoffPolicy {
        base_ms: 10,
        max_ms: 500,
        multiplier_times_100: 150,
        jitter_frac_times_100: 20,
        max_retries: 8,
        max_elapsed_ms: 5_000,
    });

    let cfg = setup.config.clone().with_options(opts.clone());
    let manifest: s3::S3Manifest<String, String> = cfg.clone().into();
    let compactor: s3::S3Compactor<String, String> = manifest.compactor();
    let mut tx = manifest.session_write().await?;
    tx.put("config".into(), "s3-example".into())?;
    tx.commit().await?;

    let snapshot = manifest.snapshot().await?;
    println!("S3 snapshot -> {:?}", snapshot);

    let (_ckpt, _tag) = compactor.compact_once().await?;
    println!("wrote checkpoint and published head");

    compactor.run_once().await?;

    println!("cleanup: S3 prefix {}", cfg.prefix);
    Ok(())
}

struct Setup {
    config: s3::Config,
}

fn load_env(prefix_tag: &str) -> Result<Option<Setup>> {
    let bucket = match env::var("FUSIO_MANIFEST_BUCKET") {
        Ok(v) => v,
        Err(_) => {
            eprintln!(
                "set AWS credentials and FUSIO_MANIFEST_BUCKET (LocalStack or AWS) to run this \
                 example"
            );
            return Ok(None);
        }
    };
    let key_id = env::var("AWS_ACCESS_KEY_ID").map_err(|_| missing("AWS_ACCESS_KEY_ID"))?;
    let secret = env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| missing("AWS_SECRET_ACCESS_KEY"))?;
    let token = env::var("AWS_SESSION_TOKEN").ok();
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let prefix = format!(
        "examples/{prefix_tag}/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );

    let cfg = s3::Builder::new(&bucket)
        .prefix(prefix)
        .region(region)
        .sign_payload(true)
        .credential(fusio::impls::remotes::aws::credential::AwsCredential {
            key_id,
            secret_key: secret,
            token,
        })
        .build();
    Ok(Some(Setup { config: cfg }))
}

fn missing(name: &str) -> Error {
    Error::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("missing env var {name}"),
    )))
}

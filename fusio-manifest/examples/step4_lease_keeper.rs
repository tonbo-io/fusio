use std::{
    env,
    sync::Arc,
    time::{Duration, SystemTime},
};

use fusio::executor::tokio::TokioExecutor;
use fusio_manifest::{
    context::ManifestContext,
    retention::DefaultRetention,
    s3,
    types::{Error, Result},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let setup = match load_env("manifest-lease-keeper")? {
        Some(cfg) => cfg,
        None => return Ok(()),
    };

    // Keep retention short so we can observe GC thresholds shifting quickly.
    let retention = DefaultRetention::default()
        .with_lease_ttl(Duration::from_secs(2))
        .with_checkpoints_min_ttl(Duration::from_secs(1))
        .with_segments_min_ttl(Duration::from_secs(1));
    let mut opts = ManifestContext::new(Arc::new(TokioExecutor::default()));
    opts.set_retention(Arc::new(retention));
    let manifest: s3::S3Manifest<String, String, TokioExecutor> =
        setup.config.clone().with_context(Arc::new(opts)).into();

    println!("== seed a value");
    let mut writer = manifest.session_write().await?;
    writer.put("user:1".into(), "alice".into());
    writer.commit().await?;

    println!("== open a read session and start the lease keeper");
    let pinned = manifest.session_read().await?;
    let keeper = pinned.start_lease_keeper()?;
    println!("pinned snapshot txn_id={}", pinned.snapshot().txn_id.0);

    println!("holding the lease for a few heartbeats...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("ending the pinned session");
    pinned.end().await?;

    println!("shutting down the lease keeper (releases lease eagerly)");
    keeper.shutdown().await;

    println!("done");
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

    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let prefix = format!("examples/{prefix_tag}/{}", now_ms);

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

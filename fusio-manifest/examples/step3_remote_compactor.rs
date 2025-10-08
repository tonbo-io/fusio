use std::{env, sync::Arc};

use fusio::executor::tokio::TokioExecutor;
use fusio_manifest::{
    context::ManifestContext,
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

    let opts = Arc::new(ManifestContext::new(TokioExecutor::default()).with_backoff(
        BackoffPolicy {
            base_ms: 10,
            max_ms: 500,
            multiplier_times_100: 150,
            jitter_frac_times_100: 20,
            max_retries: 8,
            max_elapsed_ms: 5_000,
            max_backoff_sleep_ms: 5_000,
        },
    ));

    let cfg = setup.config.clone().with_context(Arc::clone(&opts));
    let manifest: s3::S3Manifest<String, String, TokioExecutor> = cfg.clone().into();

    println!("== seed a few committed writes");
    for (k, v) in [
        ("config", "s3-example"),
        ("feature", "remote-compactor"),
        ("owner", "fusio"),
    ] {
        let mut writer = manifest.session_write().await?;
        writer.put(k.to_string(), v.to_string());
        writer.commit().await?;
    }

    let snapshot = manifest.snapshot().await?;
    println!("latest snapshot before compaction -> {:?}", snapshot);

    drop(manifest);

    println!("== simulate a remote compactor process using the shared config");
    let remote_compactor: s3::S3Compactor<String, String, TokioExecutor> = cfg.into();
    let (ckpt, head_tag) = remote_compactor.compact_once().await?;
    println!(
        "published checkpoint {:?} with head tag {:?}",
        ckpt, head_tag
    );

    if let Some(gc_tag) = remote_compactor.gc_compute().await? {
        println!("computed GC plan {:?}", gc_tag);
        remote_compactor.gc_apply().await?;
        println!("applied GC plan against HEAD");
        remote_compactor.gc_delete_and_reset().await?;
        println!("deleted segments/checkpoints and cleared plan");
    } else {
        println!("no GC work pending after compaction");
    }

    println!("cleanup: S3 prefix {}", setup.config.prefix);
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

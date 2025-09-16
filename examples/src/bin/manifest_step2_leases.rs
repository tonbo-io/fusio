use std::env;

use fusio_manifest::{
    options::Options,
    s3,
    types::{Error, Result},
};
use futures_util::{pin_mut, StreamExt};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let setup = match load_env("manifest-step2")? {
        Some(cfg) => cfg,
        None => return Ok(()),
    };
    let mut opts = Options::default();
    opts.retention.checkpoints_min_ttl_ms = 0;
    opts.retention.segments_min_ttl_ms = 0;

    let manifest: s3::S3Manifest<String, String> =
        setup.config.clone().with_options(opts.clone()).into();

    println!("== seed a few commits");
    for (k, v) in [("user:1", "alice"), ("user:2", "bob"), ("user:1", "carol")] {
        let mut session = manifest.session_write().await?;
        session.put(k.to_string(), v.to_string())?;
        session.commit().await?;
    }

    println!("== hold a pinned read lease before compaction");
    let pinned = manifest.session_read(true).await?;
    println!("pinned snapshot lsn={}", pinned.snapshot().lsn.0);

    let segs_before = list_segments(&setup).await?;
    println!("segments before compaction: {:?}", segs_before);

    let (_ckpt, _tag) = manifest.compact_and_gc().await?;
    let segs_after = list_segments(&setup).await?;
    println!("segments after GC (lease held): {:?}", segs_after);

    println!("== release lease and compact again");
    pinned.end().await?;
    let (_ckpt2, _tag2) = manifest.compact_and_gc().await?;
    let segs_final = list_segments(&setup).await?;
    println!("segments after lease released: {:?}", segs_final);

    println!("== run headless compactor once");
    let compactor: s3::S3Compactor<String, String> = setup.config.clone().into();
    compactor.run_once().await?;

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

async fn list_segments(setup: &Setup) -> Result<Vec<String>> {
    use fusio::{fs::Fs, path::Path};
    let mut out = Vec::new();
    let prefix = if setup.config.prefix.is_empty() {
        "segments".to_string()
    } else {
        format!("{}/segments", setup.config.prefix)
    };
    let stream = setup
        .config
        .s3
        .list(&Path::from(prefix))
        .await
        .map_err(|e| Error::Other(Box::new(e)))?;
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let meta = item.map_err(|e| Error::Other(Box::new(e)))?;
        if let Some(name) = meta.path.filename() {
            out.push(name.to_string());
        }
    }
    out.sort();
    Ok(out)
}

fn missing(name: &str) -> Error {
    Error::Other(Box::new(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("missing env var {name}"),
    )))
}

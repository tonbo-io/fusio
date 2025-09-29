use std::{env, sync::Arc, time::Duration};

use fusio::executor::tokio::TokioExecutor;
use fusio_manifest::{
    context::ManifestContext,
    retention::DefaultRetention,
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
    let retention = DefaultRetention::default()
        .with_checkpoints_min_ttl(Duration::ZERO)
        .with_segments_min_ttl(Duration::ZERO);
    let opts = Arc::new(ManifestContext::new(TokioExecutor::default()).with_retention(retention));

    let manifest: s3::S3Manifest<String, String, TokioExecutor> =
        setup.config.clone().with_context(Arc::clone(&opts)).into();
    let compactor: s3::S3Compactor<String, String, TokioExecutor> = manifest.compactor();

    println!("== seed a few commits");
    for (k, v) in [("user:1", "alice"), ("user:2", "bob"), ("user:1", "carol")] {
        let mut session = manifest.session_write().await?;
        session.put(k.to_string(), v.to_string());
        session.commit().await?;
    }

    println!("== hold a pinned read lease before compaction");
    let pinned = manifest.session_read().await?;
    println!("pinned snapshot txn_id={}", pinned.snapshot().txn_id.0);

    let segs_before = list_segments(&setup).await?;
    println!("segments before compaction: {:?}", segs_before);

    let (_ckpt, _tag) = compactor.compact_once().await?;
    // Run the GC phases while the lease is held; deletes should be deferred.
    if let Some(tag) = compactor.gc_compute().await? {
        compactor.gc_apply().await?;
        compactor.gc_delete_and_reset().await?;
        println!("gc plan {:?} applied while lease held", tag);
    } else {
        println!("gc has nothing to delete while lease is pinned");
    }
    let segs_after = list_segments(&setup).await?;
    println!("segments after GC (lease held): {:?}", segs_after);

    println!("== hand off snapshot before releasing lease");
    // Stash the snapshot but immediately reopen it so a lease remains active; once the
    // last lease ends, GC is free to collect the snapshot's segments.
    let stored_snapshot = pinned.snapshot().clone();
    let handoff = manifest.session_at(stored_snapshot).await?;
    let ga = handoff.get(&"user:1".into()).await?;
    println!("handoff snapshot read -> {:?}", ga);
    pinned.end().await?;
    println!("original lease released while handoff stays pinned");
    handoff.end().await?;
    println!("handoff finished; no active leases remain");

    println!("== compact again now that no leases remain");
    let (_ckpt2, _tag2) = compactor.compact_once().await?;
    if let Some(tag) = compactor.gc_compute().await? {
        compactor.gc_apply().await?;
        compactor.gc_delete_and_reset().await?;
        println!("gc plan {:?} applied after lease released", tag);
    }
    let segs_final = list_segments(&setup).await?;
    println!("segments after lease released: {:?}", segs_final);

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
    let prefix_path = Path::from(prefix);
    let stream = setup.config.s3.list(&prefix_path).await?;
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let meta = item?;
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

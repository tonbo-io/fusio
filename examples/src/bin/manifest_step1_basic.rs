use std::env;

use fusio_manifest::{
    s3,
    types::{Error, Result},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let setup = match load_env("manifest-step1")? {
        Some(cfg) => cfg,
        None => return Ok(()),
    };

    let manifest: s3::S3Manifest<String, String> = setup.config.clone().into();

    println!("== step 1: begin a write session and commit two keys");
    let mut write = manifest.session_write().await?;
    write.put("user:1".into(), "alice".into())?;
    write.put("user:2".into(), "bob".into())?;
    let commit = write.commit().await?;
    println!(
        "commit lsn={} segment={:?}",
        commit.lsn.0, commit.segment_id
    );

    println!("== step 2: read back with a snapshot");
    let snap = manifest.snapshot().await?;
    let reader = manifest.session_at(snap.clone());
    for key in ["user:1", "user:2", "user:3"] {
        let value = reader.get(&key.to_string()).await?;
        println!("snapshot {key:?} -> {:?}", value);
    }

    println!("== step 3: pin a read session (holds a lease)");
    let pinned = manifest.session_read(true).await?;
    let contents = pinned.scan().await?;
    println!("pinned snapshot contents: {:?}", contents);
    pinned.end().await?;

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

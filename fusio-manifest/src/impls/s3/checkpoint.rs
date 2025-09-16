use bytes::Bytes;
use fusio::{
    fs::{Fs, OpenOptions},
    impls::remotes::aws::fs::AmazonS3,
    path::Path,
    Read, Write,
};
use fusio_core::MaybeSendFuture;

use crate::{
    checkpoint::{CheckpointId, CheckpointMeta, CheckpointStore},
    types::Result,
};

#[derive(Clone)]
#[allow(dead_code)] // staging: constructed by upper layers in follow-ups
pub struct S3CheckpointStore {
    s3: AmazonS3,
    prefix: String,
}

#[allow(dead_code)]
impl S3CheckpointStore {
    pub fn new(s3: AmazonS3, prefix: impl Into<String>) -> Self {
        Self {
            s3,
            prefix: prefix.into(),
        }
    }

    fn id_for(lsn: u64) -> CheckpointId {
        CheckpointId(format!("ckpt-{:020}", lsn))
    }

    fn keys_for(&self, id: &CheckpointId, payload_ext: &str) -> (String, String) {
        let base = if self.prefix.is_empty() {
            format!("checkpoints/{}", id.0)
        } else {
            format!("{}/checkpoints/{}", self.prefix, id.0)
        };
        (
            format!("{}.meta.json", base),
            format!("{}{}", base, payload_ext),
        )
    }

    fn payload_ext_for(format: &str) -> &'static str {
        match format {
            "application/json" => ".json",
            _ => ".bin",
        }
    }
}

impl CheckpointStore for S3CheckpointStore {
    fn put_checkpoint(
        &self,
        meta: &CheckpointMeta,
        payload: &[u8],
        content_type: &str,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<CheckpointId>>>> {
        let s3 = self.s3.clone();
        let id = Self::id_for(meta.lsn);
        let (meta_key, data_key) = self.keys_for(&id, Self::payload_ext_for(content_type));
        let meta_json = match serde_json::to_vec(meta) {
            Ok(v) => v,
            Err(e) => {
                return Box::pin(async move {
                    Err(crate::types::Error::Corrupt(format!(
                        "ckpt meta encode: {e}"
                    )))
                })
            }
        };
        let payload_vec = payload.to_vec();
        Box::pin(async move {
            // Write payload first, then meta
            {
                let mut f = s3
                    .open_options(
                        &Path::from(data_key.clone()),
                        OpenOptions::default()
                            .create(true)
                            .truncate(true)
                            .write(true),
                    )
                    .await
                    .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                let (res, _) = f.write_all(Bytes::from(payload_vec)).await;
                res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            }
            {
                let mut f = s3
                    .open_options(
                        &Path::from(meta_key.clone()),
                        OpenOptions::default()
                            .create(true)
                            .truncate(true)
                            .write(true),
                    )
                    .await
                    .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                let (res, _) = f.write_all(Bytes::from(meta_json)).await;
                res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            }
            Ok(id)
        })
    }

    fn get_checkpoint(
        &self,
        id: &CheckpointId,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>>>> {
        let s3 = self.s3.clone();
        // We do not know the payload extension here; try both .json and .bin
        let (meta_key, data_key_json) = self.keys_for(id, ".json");
        let (_mk2, data_key_bin) = self.keys_for(id, ".bin");
        Box::pin(async move {
            use fusio::fs::Fs as _;
            // Read meta
            let mut mf = s3
                .open(&Path::from(meta_key.clone()))
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            let (res, meta_bytes) = mf.read_to_end_at(Vec::new(), 0).await;
            res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            let meta: CheckpointMeta = serde_json::from_slice(&meta_bytes)
                .map_err(|e| crate::types::Error::Corrupt(format!("ckpt meta decode: {e}")))?;
            // Read payload
            let read_payload = |k: String| {
                let s3c = s3.clone();
                async move {
                    let mut f = s3c
                        .open(&Path::from(k))
                        .await
                        .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                    let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                    res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                    Ok::<_, crate::types::Error>(bytes)
                }
            };
            let payload = match read_payload(data_key_json.clone()).await {
                Ok(v) => v,
                Err(_) => read_payload(data_key_bin.clone()).await?,
            };
            Ok((meta, payload))
        })
    }

    fn list(
        &self,
    ) -> core::pin::Pin<
        Box<dyn MaybeSendFuture<Output = Result<Vec<(CheckpointId, CheckpointMeta)>>>>,
    >
    where
        Self: Sized,
    {
        let s3 = self.s3.clone();
        let dir = if self.prefix.is_empty() {
            "checkpoints/".to_string()
        } else {
            format!("{}/checkpoints/", self.prefix)
        };
        Box::pin(async move {
            use fusio::{fs::Fs as _, path::Path as FusioPath};
            use futures_util::StreamExt;
            let mut out = Vec::new();
            let prefix_path = FusioPath::from(dir.clone());
            let stream = s3
                .list(&prefix_path)
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                if let Some(filename) = meta.path.filename() {
                    if !filename.ends_with(".meta.json") {
                        continue;
                    }
                    // filename is like: ckpt-<lsn>.meta.json
                    let id_str = filename.trim_end_matches(".meta.json");
                    let id = CheckpointId(id_str.to_string());
                    // read meta
                    let mut f = s3
                        .open(&meta.path)
                        .await
                        .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                    let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                    res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                    let m: CheckpointMeta = serde_json::from_slice(&bytes).map_err(|e| {
                        crate::types::Error::Corrupt(format!("ckpt meta decode: {e}"))
                    })?;
                    out.push((id, m));
                }
            }
            Ok(out)
        })
    }

    fn delete(
        &self,
        id: &CheckpointId,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>>
    where
        Self: Sized,
    {
        let s3 = self.s3.clone();
        let (meta_key, data_key_json) = self.keys_for(id, ".json");
        let (_mk2, data_key_bin) = self.keys_for(id, ".bin");
        Box::pin(async move {
            use fusio::{fs::Fs as _, path::Path as FusioPath};
            let _ = s3.remove(&FusioPath::from(meta_key)).await;
            let _ = s3.remove(&FusioPath::from(data_key_json)).await;
            let _ = s3.remove(&FusioPath::from(data_key_bin)).await;
            Ok(())
        })
    }
}

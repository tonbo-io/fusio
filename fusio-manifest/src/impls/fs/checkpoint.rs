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
    types::{Error, Result},
};

#[derive(Clone)]
pub struct FsCheckpointStore {
    fs: AmazonS3,
    prefix: String,
}

impl FsCheckpointStore {
    pub fn new(fs: AmazonS3, prefix: impl Into<String>) -> Self {
        Self {
            fs,
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

impl CheckpointStore for FsCheckpointStore {
    fn put_checkpoint(
        &self,
        meta: &CheckpointMeta,
        payload: &[u8],
        content_type: &str,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<CheckpointId>>>> {
        let fs = self.fs.clone();
        let id = Self::id_for(meta.lsn);
        let (meta_key, data_key) = self.keys_for(&id, Self::payload_ext_for(content_type));
        let meta_json = match serde_json::to_vec(meta) {
            Ok(v) => v,
            Err(e) => {
                return Box::pin(
                    async move { Err(Error::Corrupt(format!("ckpt meta encode: {e}"))) },
                )
            }
        };
        let payload_bytes = Bytes::copy_from_slice(payload);
        let meta_bytes = Bytes::from(meta_json);
        Box::pin(async move {
            {
                let mut f = fs
                    .open_options(
                        &Path::from(data_key.clone()),
                        OpenOptions::default()
                            .create(true)
                            .truncate(true)
                            .write(true),
                    )
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                let (res, _) = f.write_all(payload_bytes.clone()).await;
                res.map_err(|e| Error::Other(Box::new(e)))?;
            }
            {
                let mut f = fs
                    .open_options(
                        &Path::from(meta_key.clone()),
                        OpenOptions::default()
                            .create(true)
                            .truncate(true)
                            .write(true),
                    )
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                let (res, _) = f.write_all(meta_bytes.clone()).await;
                res.map_err(|e| Error::Other(Box::new(e)))?;
            }
            Ok(id)
        })
    }

    fn get_checkpoint(
        &self,
        id: &CheckpointId,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>>>> {
        let fs = self.fs.clone();
        let (meta_key, data_key_json) = self.keys_for(id, ".json");
        let (_mk2, data_key_bin) = self.keys_for(id, ".bin");
        Box::pin(async move {
            let meta = FsCheckpointStore::load_meta(fs.clone(), meta_key).await?;

            let read_payload = |k: String| {
                let fs_clone = fs.clone();
                async move {
                    let mut f = fs_clone
                        .open(&Path::from(k))
                        .await
                        .map_err(|e| Error::Other(Box::new(e)))?;
                    let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                    res.map_err(|e| Error::Other(Box::new(e)))?;
                    Ok::<_, Error>(bytes)
                }
            };

            let payload = match read_payload(data_key_json.clone()).await {
                Ok(v) => v,
                Err(_) => read_payload(data_key_bin.clone()).await?,
            };
            Ok((meta, payload))
        })
    }

    fn get_checkpoint_meta(
        &self,
        id: &CheckpointId,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<CheckpointMeta>>>> {
        let fs = self.fs.clone();
        let (meta_key, _) = self.keys_for(id, ".json");
        Box::pin(async move { FsCheckpointStore::load_meta(fs, meta_key).await })
    }

    fn list(
        &self,
    ) -> core::pin::Pin<
        Box<dyn MaybeSendFuture<Output = Result<Vec<(CheckpointId, CheckpointMeta)>>>>,
    >
    where
        Self: Sized,
    {
        let fs = self.fs.clone();
        let dir = if self.prefix.is_empty() {
            "checkpoints/".to_string()
        } else {
            format!("{}/checkpoints/", self.prefix)
        };
        Box::pin(async move {
            use fusio::path::Path as FusioPath;
            use futures_util::StreamExt;
            let mut out = Vec::new();
            let prefix_path = FusioPath::from(dir.clone());
            let stream = fs
                .list(&prefix_path)
                .await
                .map_err(|e| Error::Other(Box::new(e)))?;
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| Error::Other(Box::new(e)))?;
                if let Some(filename) = meta.path.filename() {
                    if !filename.ends_with(".meta.json") {
                        continue;
                    }
                    let id_str = filename.trim_end_matches(".meta.json");
                    let id = CheckpointId(id_str.to_string());
                    let mut f = fs
                        .open(&meta.path)
                        .await
                        .map_err(|e| Error::Other(Box::new(e)))?;
                    let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                    res.map_err(|e| Error::Other(Box::new(e)))?;
                    let m: CheckpointMeta = serde_json::from_slice(&bytes)
                        .map_err(|e| Error::Corrupt(format!("ckpt meta decode: {e}")))?;
                    out.push((id, m));
                }
            }
            Ok(out)
        })
    }
}

impl FsCheckpointStore {
    async fn load_meta(fs: AmazonS3, meta_key: String) -> Result<CheckpointMeta> {
        let mut mf = fs
            .open(&Path::from(meta_key))
            .await
            .map_err(|e| Error::Other(Box::new(e)))?;
        let (res, meta_bytes) = mf.read_to_end_at(Vec::new(), 0).await;
        res.map_err(|e| Error::Other(Box::new(e)))?;
        let meta: CheckpointMeta = serde_json::from_slice(&meta_bytes)
            .map_err(|e| Error::Corrupt(format!("ckpt meta decode: {e}")))?;
        Ok(meta)
    }
}

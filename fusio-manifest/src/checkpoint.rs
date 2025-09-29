use std::io::ErrorKind;

use async_stream::try_stream;
use bytes::Bytes;
use fusio::{
    fs::{Fs, OpenOptions},
    path::Path,
    Error as FsError, FileCommit, Read, Write,
};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use futures_util::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::types::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointId(pub String);

impl CheckpointId {
    /// Construct a checkpoint id using the canonical `ckpt-<lsn>` format.
    pub fn new(lsn: u64) -> Self {
        Self(format!("ckpt-{lsn:020}"))
    }

    /// Borrow the underlying string representation (e.g., for object keys).
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Attempt to extract the LSN from a canonical checkpoint id.
    pub fn lsn(&self) -> Option<u64> {
        self.0
            .strip_prefix("ckpt-")
            .and_then(|rest| rest.parse::<u64>().ok())
    }
}

impl From<String> for CheckpointId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for CheckpointId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMeta {
    pub lsn: u64,
    pub key_count: usize,
    pub byte_size: usize,
    pub created_at_ms: u64,
    pub format: String,
    /// The last segment sequence number included in this checkpoint.
    pub last_segment_seq_at_ckpt: u64,
}

pub trait CheckpointStore: MaybeSend + MaybeSync + Clone {
    fn put_checkpoint(
        &self,
        meta: &CheckpointMeta,
        payload: &[u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointId>> + '_;

    fn get_checkpoint(
        &self,
        id: &CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>> + '_;

    fn get_checkpoint_meta(
        &self,
        id: &CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointMeta>> + '_;

    /// List all checkpoints (meta only).
    fn list(
        &self,
    ) -> impl MaybeSendFuture<
        Output = Result<impl Stream<Item = Result<(CheckpointId, CheckpointMeta)>> + '_>,
    > + '_;

    /// Delete a checkpoint by id.
    fn delete(&self, id: &CheckpointId) -> impl MaybeSendFuture<Output = Result<()>> + '_;
}

#[derive(Clone)]
pub struct FsCheckpointStore<FS> {
    fs: FS,
    prefix: String,
}

impl<FS> FsCheckpointStore<FS> {
    pub fn new(fs: FS, prefix: impl Into<String>) -> Self {
        Self {
            fs,
            prefix: prefix.into(),
        }
    }

    fn keys_for(&self, id: &CheckpointId, payload_ext: &str) -> (String, String) {
        let base = if self.prefix.is_empty() {
            format!("checkpoints/{}", id.as_str())
        } else {
            format!("{}/checkpoints/{}", self.prefix, id.as_str())
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

impl<FS> CheckpointStore for FsCheckpointStore<FS>
where
    FS: Fs + Clone + Send + Sync + 'static,
    <FS as Fs>::File: FileCommit,
{
    fn put_checkpoint(
        &self,
        meta: &CheckpointMeta,
        payload: &[u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointId>> + '_ {
        let id = CheckpointId::new(meta.lsn);
        let (meta_key, data_key) = self.keys_for(&id, Self::payload_ext_for(content_type));
        let payload_bytes = Bytes::copy_from_slice(payload);
        let meta_bytes = serde_json::to_vec(meta)
            .map(Bytes::from)
            .map_err(|e| Error::Corrupt(format!("ckpt meta encode: {e}")));
        async move {
            {
                let mut f = self
                    .fs
                    .open_options(
                        &Path::from(data_key),
                        OpenOptions::default()
                            .create(true)
                            .truncate(true)
                            .write(true),
                    )
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                let (res, _) = f.write_all(payload_bytes).await;
                res.map_err(|e| Error::Other(Box::new(e)))?;
                f.commit().await.map_err(|e| Error::Other(Box::new(e)))?;
            }
            {
                let meta_bytes = meta_bytes?;
                let mut f = self
                    .fs
                    .open_options(
                        &Path::from(meta_key),
                        OpenOptions::default()
                            .create(true)
                            .truncate(true)
                            .write(true),
                    )
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                let (res, _meta_bytes) = f.write_all(meta_bytes).await;
                res.map_err(|e| Error::Other(Box::new(e)))?;
                f.commit().await.map_err(|e| Error::Other(Box::new(e)))?;
            }
            Ok(id)
        }
    }

    fn get_checkpoint(
        &self,
        id: &CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>> + '_ {
        let (meta_key, data_key_json) = self.keys_for(id, ".json");
        let (_mk2, data_key_bin) = self.keys_for(id, ".bin");
        async move {
            let meta = FsCheckpointStore::load_meta(&self.fs, meta_key).await?;
            let fs = &self.fs;
            let read_payload = |k: String| async move {
                let mut f = fs
                    .open(&Path::from(k))
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                res.map_err(|e| Error::Other(Box::new(e)))?;
                Ok::<_, Error>(bytes)
            };

            let payload = match read_payload(data_key_json).await {
                Ok(v) => v,
                Err(_) => read_payload(data_key_bin).await?,
            };
            Ok((meta, payload))
        }
    }

    fn get_checkpoint_meta(
        &self,
        id: &CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointMeta>> + '_ {
        let (meta_key, _) = self.keys_for(id, ".json");
        let fs = &self.fs;
        async move { FsCheckpointStore::load_meta(fs, meta_key).await }
    }

    fn list(
        &self,
    ) -> impl MaybeSendFuture<
        Output = Result<impl Stream<Item = Result<(CheckpointId, CheckpointMeta)>> + '_>,
    > + '_ {
        let dir = if self.prefix.is_empty() {
            "checkpoints/".to_string()
        } else {
            format!("{}/checkpoints/", self.prefix)
        };
        let fs = &self.fs;
        async move {
            let stream = try_stream! {
                let prefix_path = Path::from(dir);
                let listing = fs
                    .list(&prefix_path)
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                futures_util::pin_mut!(listing);
                while let Some(item) = listing.next().await {
                    let meta = item.map_err(|e| Error::Other(Box::new(e)))?;
                    let Some(filename) = meta.path.filename() else {
                        continue;
                    };
                    if !filename.ends_with(".meta.json") {
                        continue;
                    }
                    let id_str = filename.trim_end_matches(".meta.json");
                    let id = CheckpointId::from(id_str);
                    let mut f = fs
                        .open(&meta.path)
                        .await
                        .map_err(|e| Error::Other(Box::new(e)))?;
                    let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                    res.map_err(|e| Error::Other(Box::new(e)))?;
                    let m: CheckpointMeta = serde_json::from_slice(&bytes)
                        .map_err(|e| Error::Corrupt(format!("ckpt meta decode: {e}")))?;
                    yield (id, m);
                }
            };
            Ok(stream)
        }
    }

    fn delete(&self, id: &CheckpointId) -> impl MaybeSendFuture<Output = Result<()>> + '_ {
        let (meta_key, data_key_json) = self.keys_for(id, ".json");
        let (_, data_key_bin) = self.keys_for(id, ".bin");
        async move {
            for key in [meta_key, data_key_json, data_key_bin] {
                let path = Path::from(key);
                match self.fs.remove(&path).await {
                    Ok(()) => {}
                    Err(FsError::Io(err)) if err.kind() == ErrorKind::NotFound => {}
                    Err(FsError::PreconditionFailed) => return Err(Error::PreconditionFailed),
                    Err(other) => return Err(Error::Other(Box::new(other))),
                }
            }
            Ok(())
        }
    }
}

impl<FS> FsCheckpointStore<FS>
where
    FS: Fs + Send + Sync,
{
    async fn load_meta(fs: &FS, meta_key: String) -> Result<CheckpointMeta> {
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

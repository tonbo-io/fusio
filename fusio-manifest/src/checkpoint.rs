use std::io::ErrorKind;

use async_stream::try_stream;
use bytes::Bytes;
use fusio::{
    fs::{Fs, FsCas, OpenOptions},
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
    fn put_checkpoint<'s>(
        &'s self,
        meta: &CheckpointMeta,
        payload: &'s [u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointId>> + 's;

    fn get_checkpoint<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>> + 'a;

    fn get_checkpoint_with_etag<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>, Option<String>)>> + 'a {
        let fut = self.get_checkpoint(id);
        async move {
            let (meta, payload) = fut.await?;
            Ok((meta, payload, None))
        }
    }

    fn get_checkpoint_meta<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointMeta>> + 'a;

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
pub struct CheckpointStoreImpl<FS> {
    fs: FS,
    prefix: String,
}

impl<FS> CheckpointStoreImpl<FS> {
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

impl<FS> CheckpointStore for CheckpointStoreImpl<FS>
where
    FS: Fs + FsCas + Clone + Send + Sync + 'static,
    <FS as Fs>::File: FileCommit,
{
    fn put_checkpoint<'s>(
        &'s self,
        meta: &CheckpointMeta,
        payload: &'s [u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointId>> + 's {
        let id = CheckpointId::new(meta.lsn);
        let (meta_key, data_key) = self.keys_for(&id, Self::payload_ext_for(content_type));
        let meta_bytes = serde_json::to_vec(meta)
            .map(Bytes::from)
            .map_err(|e| Error::Corrupt(format!("ckpt meta encode: {e}")));
        // Copy payload to owned Vec for completion-based I/O compatibility
        let payload_owned = payload.to_vec();
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
                    .await?;
                let (res, _) = f.write_all(payload_owned).await;
                res?;
                f.commit().await?;
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
                    .await?;
                let (res, _meta_bytes) = f.write_all(meta_bytes).await;
                res?;
                f.commit().await?;
            }
            Ok(id)
        }
    }

    fn get_checkpoint<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>> + 'a {
        async move {
            let (meta, payload, _etag) =
                <Self as CheckpointStore>::get_checkpoint_with_etag(self, id).await?;
            Ok((meta, payload))
        }
    }

    fn get_checkpoint_with_etag<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>, Option<String>)>> + 'a {
        let (meta_key, data_key_json) = self.keys_for(id, ".json");
        let (_mk2, data_key_bin) = self.keys_for(id, ".bin");
        async move {
            let meta = CheckpointStoreImpl::load_meta(&self.fs, meta_key).await?;
            let try_key = |key: String, fs: FS| async move {
                let path = Path::from(key.clone());
                match fs.load_with_tag(&path).await.map_err(Error::from)? {
                    Some((bytes, etag)) => Ok((bytes, Some(etag))),
                    None => Err(Error::Corrupt(format!(
                        "checkpoint payload missing object for key {key}"
                    ))),
                }
            };

            let (payload, etag) = match try_key(data_key_json, self.fs.clone()).await {
                Ok(res) => res,
                Err(Error::Corrupt(_)) => try_key(data_key_bin, self.fs.clone()).await?,
                Err(e) => return Err(e),
            };
            Ok((meta, payload, etag))
        }
    }

    fn get_checkpoint_meta(
        &self,
        id: &CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointMeta>> + '_ {
        let (meta_key, _) = self.keys_for(id, ".json");
        let fs = &self.fs;
        async move { CheckpointStoreImpl::load_meta(fs, meta_key).await }
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
                    .await?;
                futures_util::pin_mut!(listing);
                while let Some(item) = listing.next().await {
                    let meta = item?;
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
                        .await?;
                    let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                    res?;
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
                    Err(other) => return Err(other.into()),
                }
            }
            Ok(())
        }
    }
}

impl<FS> CheckpointStoreImpl<FS>
where
    FS: Fs + Send + Sync,
{
    async fn load_meta(fs: &FS, meta_key: String) -> Result<CheckpointMeta> {
        let mut mf = fs.open(&Path::from(meta_key)).await?;
        let (res, meta_bytes) = mf.read_to_end_at(Vec::new(), 0).await;
        res?;
        let meta: CheckpointMeta = serde_json::from_slice(&meta_bytes)
            .map_err(|e| Error::Corrupt(format!("ckpt meta decode: {e}")))?;
        Ok(meta)
    }
}

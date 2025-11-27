use std::{collections::HashMap, fmt, io::ErrorKind, pin::Pin};

use fusio::{
    fs::{CasCondition, Fs, FsCas},
    impls::{mem::fs::InMemoryFs, remotes::aws::fs::AmazonS3},
    path::Path,
    Error as FsError,
};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use futures_util::StreamExt;
use serde::Deserialize;

use crate::types::{Error, Result, SegmentId};

/// Abstraction for immutable segment IO on durable storage.
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub txn_id: u64,
}

pub trait SegmentIo: MaybeSend + MaybeSync {
    /// Write the given payload as a new segment with a writer-provided sequence number.
    /// Returns the durable segment identifier.
    fn put_next<'s>(
        &'s self,
        seq: u64,
        txn_id: u64,
        payload: &'s [u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<SegmentId>> + 's;

    /// Fetch a previously written segment payload by id.
    fn get<'a>(&'a self, id: &'a SegmentId) -> impl MaybeSendFuture<Output = Result<Vec<u8>>> + 'a;

    /// Fetch a segment payload together with an optional strong revision identifier (e.g., ETag).
    fn get_with_etag<'a>(
        &'a self,
        id: &'a SegmentId,
    ) -> impl MaybeSendFuture<Output = Result<(Vec<u8>, Option<String>)>> + 'a {
        // Default impl keeps existing backends unchanged: return the payload and surface
        // `None` for the coherence tag. Stores with real ETag support should override.
        let fut = self.get(id);
        async move {
            let bytes = fut.await?;
            Ok((bytes, None))
        }
    }

    /// Fetch segment metadata if available without reading the payload.
    fn load_meta(&self, id: &SegmentId) -> impl MaybeSendFuture<Output = Result<SegmentMeta>> + '_;

    /// List segment ids starting from a minimum sequence number (inclusive), up to `limit` items.
    fn list_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> impl MaybeSendFuture<Output = Result<Vec<SegmentId>>> + '_;

    /// Delete all segments with sequence number <= upto (best-effort; idempotent).
    fn delete_upto(&self, upto_seq: u64) -> impl MaybeSendFuture<Output = Result<()>> + '_;
}

#[derive(Debug, Clone)]
pub struct SegmentStoreImpl<FS> {
    fs: FS,
    pub(crate) prefix: String,
}

const TXN_ID_HEADER: &str = "x-amz-meta-fusio-txn-id";

trait ObjectHead {
    fn head_metadata<'a>(
        &'a self,
        path: &'a Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<HashMap<String, String>>, FsError>> + 'a>>;
}

impl<FS> SegmentStoreImpl<FS> {
    pub fn new(fs: FS, prefix: impl Into<String>) -> Self {
        Self {
            fs,
            prefix: prefix.into(),
        }
    }

    fn key_for(&self, seq: u64, ext: &str) -> String {
        if self.prefix.is_empty() {
            format!("seg-{:020}{}", seq, ext)
        } else {
            format!("{}/seg-{:020}{}", self.prefix, seq, ext)
        }
    }

    fn parse_seq(filename: &str) -> Option<u64> {
        if let Some(rest) = filename.strip_prefix("seg-") {
            let (num, _) = rest.split_once('.').unwrap_or((rest, ""));
            if let Ok(v) = num.parse::<u64>() {
                return Some(v);
            }
        }
        None
    }
}

impl<FS> SegmentIo for SegmentStoreImpl<FS>
where
    FS: Fs + FsCas + ObjectHead + Clone + MaybeSend + MaybeSync + 'static,
{
    fn put_next<'s>(
        &'s self,
        seq: u64,
        txn_id: u64,
        payload: &'s [u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<SegmentId>> + 's {
        let ext = match content_type {
            "application/json" => ".json",
            _ => ".bin",
        };
        let key = self.key_for(seq, ext);
        let ct = if content_type.is_empty() {
            None
        } else {
            Some(content_type.to_string())
        };
        let meta_headers = vec![(TXN_ID_HEADER.to_string(), txn_id.to_string())];
        async move {
            let path = Path::parse(&key).map_err(Error::other)?;
            self.fs
                .put_conditional(
                    &path,
                    payload,
                    ct.as_deref(),
                    Some(meta_headers),
                    CasCondition::IfNotExists,
                )
                .await
                .map_err(map_fs_error)?;
            Ok(SegmentId { seq })
        }
    }

    fn get<'a>(&'a self, id: &'a SegmentId) -> impl MaybeSendFuture<Output = Result<Vec<u8>>> + 'a {
        async move {
            let (bytes, _) = <Self as SegmentIo>::get_with_etag(self, id).await?;
            Ok(bytes)
        }
    }

    fn get_with_etag<'a>(
        &'a self,
        id: &'a SegmentId,
    ) -> impl MaybeSendFuture<Output = Result<(Vec<u8>, Option<String>)>> + 'a {
        let fs = self.fs.clone();
        let key_json = self.key_for(id.seq, ".json");
        let key_bin = self.key_for(id.seq, ".bin");
        async move {
            let try_key = |key: String, fs: FS| async move {
                let path = Path::parse(&key).map_err(Error::other)?;
                match fs.load_with_tag(&path).await.map_err(Error::from)? {
                    Some((bytes, etag)) => Ok((bytes, Some(etag))),
                    None => Err(Error::Corrupt(format!(
                        "segment payload missing object for key {key}"
                    ))),
                }
            };

            match try_key(key_json, fs.clone()).await {
                Ok(res) => Ok(res),
                Err(Error::Corrupt(_)) => try_key(key_bin, fs).await,
                Err(e) => Err(e),
            }
        }
    }

    fn load_meta(&self, id: &SegmentId) -> impl MaybeSendFuture<Output = Result<SegmentMeta>> + '_ {
        let fs = self.fs.clone();
        let key_json = self.key_for(id.seq, ".json");
        let key_bin = self.key_for(id.seq, ".bin");
        async move {
            let try_key = |key: String, fs: FS| async move {
                let path = Path::parse(&key).map_err(Error::other)?;
                match fs.head_metadata(&path).await {
                    Ok(Some(metadata)) => {
                        let txn = metadata.get(TXN_ID_HEADER).ok_or_else(|| {
                            Error::Corrupt(format!(
                                "segment metadata missing txn header for key {key}"
                            ))
                        })?;
                        let parsed = txn.parse::<u64>().map_err(|e| {
                            Error::Corrupt(format!("segment txn header parse error: {e}"))
                        })?;
                        Ok(SegmentMeta { txn_id: parsed })
                    }
                    Ok(None) => Err(Error::Corrupt(format!(
                        "segment metadata missing object for key {key}"
                    ))),
                    Err(e) => Err(e.into()),
                }
            };

            match try_key(key_json, fs.clone()).await {
                Ok(meta) => Ok(meta),
                Err(Error::Corrupt(_)) => try_key(key_bin, fs).await,
                Err(e) => Err(e),
            }
        }
    }

    fn list_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> impl MaybeSendFuture<Output = Result<Vec<SegmentId>>> + '_ {
        async move {
            let mut out = Vec::new();
            let prefix_path = if self.prefix.is_empty() {
                Path::default()
            } else {
                Path::parse(&self.prefix).map_err(Error::other)?
            };
            let stream = self.fs.list(&prefix_path).await?;
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let meta = item?;
                if let Some(filename) = meta.path.filename() {
                    if let Some(seq) = SegmentStoreImpl::<FS>::parse_seq(filename) {
                        if seq >= from_seq {
                            out.push(SegmentId { seq });
                        }
                    }
                }
                if out.len() >= limit {
                    break;
                }
            }
            out.sort_by_key(|s| s.seq);
            Ok(out)
        }
    }

    fn delete_upto(&self, upto_seq: u64) -> impl MaybeSendFuture<Output = Result<()>> + '_ {
        async move {
            let prefix_path = if self.prefix.is_empty() {
                Path::default()
            } else {
                Path::parse(&self.prefix).map_err(Error::other)?
            };
            let stream = self.fs.list(&prefix_path).await?;
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let meta = item?;
                if let Some(filename) = meta.path.filename() {
                    if let Some(seq) = SegmentStoreImpl::<FS>::parse_seq(filename) {
                        if seq <= upto_seq {
                            match self.fs.remove(&meta.path).await {
                                Ok(()) => {}
                                Err(FsError::Io(err)) if err.kind() == ErrorKind::NotFound => {}
                                Err(err) => return Err(map_fs_error(err)),
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

fn map_fs_error(err: FsError) -> Error {
    match err {
        FsError::PreconditionFailed => Error::PreconditionFailed,
        other => Error::Io(other),
    }
}

#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[derive(Deserialize)]
struct SegmentTxnOnly {
    txn_id: u64,
}

#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[derive(Debug)]
struct SegmentMetadataParseError {
    path: String,
    source: serde_json::Error,
}

impl SegmentMetadataParseError {
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    fn new(path: &Path, source: serde_json::Error) -> Self {
        Self {
            path: path.to_string(),
            source,
        }
    }
}

impl fmt::Display for SegmentMetadataParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to parse txn metadata from segment {}", self.path)
    }
}

impl std::error::Error for SegmentMetadataParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ObjectHead for fusio::impls::disk::TokioFs {
    fn head_metadata<'a>(
        &'a self,
        path: &'a Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<HashMap<String, String>>, FsError>> + 'a>>
    {
        Box::pin(async move {
            let path_clone = path.clone();
            let ext = path_clone.extension().map(str::to_owned);
            match FsCas::load_with_tag(self, &path_clone).await? {
                Some((bytes, _)) if matches!(ext.as_deref(), Some("json")) => {
                    let header: SegmentTxnOnly = serde_json::from_slice(&bytes).map_err(|err| {
                        FsError::Other(Box::new(SegmentMetadataParseError::new(&path_clone, err)))
                    })?;
                    let mut meta = HashMap::with_capacity(1);
                    meta.insert(TXN_ID_HEADER.to_string(), header.txn_id.to_string());
                    Ok(Some(meta))
                }
                Some(_) => Ok(None),
                None => Ok(None),
            }
        })
    }
}

impl ObjectHead for AmazonS3 {
    fn head_metadata<'a>(
        &'a self,
        path: &'a Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<HashMap<String, String>>, FsError>> + 'a>>
    {
        Box::pin(async move {
            match self.head_object(path).await {
                Ok(Some(head)) => Ok(Some(head.metadata)),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        })
    }
}

impl ObjectHead for InMemoryFs {
    fn head_metadata<'a>(
        &'a self,
        path: &'a Path,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<HashMap<String, String>>, FsError>> + 'a>>
    {
        Box::pin(async move { Ok(self.head_object(path).await?.map(|head| head.metadata)) })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use fusio::disk::LocalFs;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn segment_store_localfs_round_trip() {
        let tmp = TempDir::new().expect("create temp dir");
        let segments_root = tmp.path().join("segments");
        std::fs::create_dir_all(&segments_root).expect("create segments dir");

        let prefix_path = fusio::path::Path::from_absolute_path(&segments_root)
            .expect("convert absolute path to fusio path");
        let prefix: String = prefix_path.into();

        let store = SegmentStoreImpl::new(LocalFs {}, prefix);

        let payload = br#"{"txn_id":42,"records":[]}"#;
        let seg_id = store
            .put_next(1, 42, payload, "application/json")
            .await
            .expect("write segment");
        assert_eq!(seg_id.seq, 1);

        let meta = store.load_meta(&seg_id).await.expect("load metadata");
        assert_eq!(meta.txn_id, 42);

        let listed = store.list_from(0, 10).await.expect("list segments");
        assert_eq!(listed, vec![SegmentId { seq: 1 }]);

        let bytes = store.get(&seg_id).await.expect("fetch segment payload");
        assert_eq!(bytes, payload);
    }
}

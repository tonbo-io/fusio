use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    fmt,
    io::ErrorKind,
    pin::Pin,
};

#[cfg(all(feature = "web", target_arch = "wasm32"))]
use fusio::impls::disk::OPFS;
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
    /// JSON-encoded key bounds for fast negative filtering in L0 point gets.
    pub key_min_json: Option<String>,
    pub key_max_json: Option<String>,
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

    /// List segment ids starting from `from_seq` (inclusive), returning the smallest `limit`
    /// sequence numbers in ascending order.
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
const KEY_MIN_HEADER: &str = "x-amz-meta-fusio-key-min";
const KEY_MAX_HEADER: &str = "x-amz-meta-fusio-key-max";

/// Capability for fetching object metadata without downloading the payload.
pub trait ObjectHead {
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
        let key_bounds = if content_type == "application/json" {
            extract_segment_key_bounds(payload)
        } else {
            None
        };
        async move {
            let mut meta_headers = vec![(TXN_ID_HEADER.to_string(), txn_id.to_string())];
            if let Some((min_key, max_key)) = key_bounds {
                meta_headers.push((KEY_MIN_HEADER.to_string(), min_key));
                meta_headers.push((KEY_MAX_HEADER.to_string(), max_key));
            }
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
                        let key_min_json = metadata.get(KEY_MIN_HEADER).cloned();
                        let key_max_json = metadata.get(KEY_MAX_HEADER).cloned();
                        let (key_min_json, key_max_json) =
                            if key_min_json.is_some() && key_max_json.is_some() {
                                (key_min_json, key_max_json)
                            } else {
                                (None, None)
                            };
                        Ok(SegmentMeta {
                            txn_id: parsed,
                            key_min_json,
                            key_max_json,
                        })
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
            if limit == 0 {
                return Ok(Vec::new());
            }

            // Keep only the smallest `limit` seq values >= `from_seq`, independent of backend
            // listing order. This preserves correctness for paged scans and compaction cursors.
            let mut selected = BinaryHeap::<u64>::with_capacity(limit);
            let mut dedup = HashSet::<u64>::with_capacity(limit);
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
                            if dedup.contains(&seq) {
                                continue;
                            }
                            if selected.len() < limit {
                                selected.push(seq);
                                dedup.insert(seq);
                                continue;
                            }
                            if let Some(&largest) = selected.peek() {
                                if seq < largest {
                                    if let Some(evicted) = selected.pop() {
                                        dedup.remove(&evicted);
                                    }
                                    selected.push(seq);
                                    dedup.insert(seq);
                                }
                            }
                        }
                    }
                }
            }

            let mut out: Vec<SegmentId> = selected
                .into_vec()
                .into_iter()
                .map(|seq| SegmentId { seq })
                .collect();
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

fn extract_segment_key_bounds(payload: &[u8]) -> Option<(String, String)> {
    let header: SegmentHeaderOnly = serde_json::from_slice(payload).ok()?;
    let first = header.records.first();
    let last = header.records.last();
    if let (Some(first), Some(last)) = (first, last) {
        return Some((first.key.to_string(), last.key.to_string()));
    }
    None
}

#[cfg_attr(
    not(any(feature = "tokio", all(feature = "web", target_arch = "wasm32"))),
    allow(dead_code)
)]
fn segment_headers_from_payload(
    path: &Path,
    bytes: &[u8],
) -> Result<HashMap<String, String>, FsError> {
    let header: SegmentHeaderOnly = serde_json::from_slice(bytes)
        .map_err(|err| FsError::Other(Box::new(SegmentMetadataParseError::new(path, err))))?;
    let mut meta = HashMap::with_capacity(3);
    meta.insert(TXN_ID_HEADER.to_string(), header.txn_id.to_string());
    if let (Some(first), Some(last)) = (header.records.first(), header.records.last()) {
        meta.insert(KEY_MIN_HEADER.to_string(), first.key.to_string());
        meta.insert(KEY_MAX_HEADER.to_string(), last.key.to_string());
    }
    Ok(meta)
}

#[cfg_attr(not(feature = "tokio"), allow(dead_code))]
#[derive(Deserialize)]
struct SegmentHeaderOnly {
    txn_id: u64,
    #[serde(default)]
    records: Vec<SegmentRecordKeyOnly>,
}

#[derive(Deserialize)]
struct SegmentRecordKeyOnly {
    key: serde_json::Value,
}

#[cfg_attr(not(feature = "tokio"), allow(dead_code))]
#[derive(Debug)]
struct SegmentMetadataParseError {
    path: String,
    source: serde_json::Error,
}

impl SegmentMetadataParseError {
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
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

#[cfg(feature = "tokio")]
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
                    let meta = segment_headers_from_payload(&path_clone, &bytes)?;
                    Ok(Some(meta))
                }
                Some(_) => Ok(None),
                None => Ok(None),
            }
        })
    }
}

#[cfg(all(feature = "web", target_arch = "wasm32"))]
impl ObjectHead for OPFS {
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
                    let meta = segment_headers_from_payload(&path_clone, &bytes)?;
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

#[cfg(all(test, feature = "tokio"))]
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
        assert!(meta.key_min_json.is_none());
        assert!(meta.key_max_json.is_none());

        let listed = store.list_from(0, 10).await.expect("list segments");
        assert_eq!(listed, vec![SegmentId { seq: 1 }]);

        let bytes = store.get(&seg_id).await.expect("fetch segment payload");
        assert_eq!(bytes, payload);
    }

    #[tokio::test]
    async fn segment_store_localfs_key_bounds_round_trip() {
        let tmp = TempDir::new().expect("create temp dir");
        let segments_root = tmp.path().join("segments");
        std::fs::create_dir_all(&segments_root).expect("create segments dir");

        let prefix_path = fusio::path::Path::from_absolute_path(&segments_root)
            .expect("convert absolute path to fusio path");
        let prefix: String = prefix_path.into();

        let store = SegmentStoreImpl::new(LocalFs {}, prefix);
        let payload = br#"{"txn_id":7,"records":[{"key":"a"},{"key":"z"}]}"#;
        let seg_id = store
            .put_next(1, 7, payload, "application/json")
            .await
            .expect("write segment");

        let meta = store.load_meta(&seg_id).await.expect("load metadata");
        assert_eq!(meta.txn_id, 7);
        assert_eq!(meta.key_min_json.as_deref(), Some("\"a\""));
        assert_eq!(meta.key_max_json.as_deref(), Some("\"z\""));
    }

    #[tokio::test]
    async fn segment_store_localfs_list_from_returns_lowest_window() {
        let tmp = TempDir::new().expect("create temp dir");
        let segments_root = tmp.path().join("segments");
        std::fs::create_dir_all(&segments_root).expect("create segments dir");

        let prefix_path = fusio::path::Path::from_absolute_path(&segments_root)
            .expect("convert absolute path to fusio path");
        let prefix: String = prefix_path.into();
        let store = SegmentStoreImpl::new(LocalFs {}, prefix);

        for seq in 1_u64..=320_u64 {
            let payload = format!("{{\"txn_id\":{seq},\"records\":[]}}");
            store
                .put_next(seq, seq, payload.as_bytes(), "application/json")
                .await
                .expect("write segment");
        }

        let first_page = store.list_from(1, 256).await.expect("list first page");
        assert_eq!(first_page.len(), 256);
        assert_eq!(first_page.first().map(|id| id.seq), Some(1));
        assert_eq!(first_page.last().map(|id| id.seq), Some(256));
        for window in first_page.windows(2) {
            assert!(
                window[0].seq < window[1].seq,
                "first page should be ascending"
            );
        }

        let second_page = store.list_from(257, 256).await.expect("list second page");
        assert_eq!(second_page.len(), 64);
        assert_eq!(second_page.first().map(|id| id.seq), Some(257));
        assert_eq!(second_page.last().map(|id| id.seq), Some(320));
        for window in second_page.windows(2) {
            assert!(
                window[0].seq < window[1].seq,
                "second page should be ascending"
            );
        }
    }
}

use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use fusio_core::MaybeSendFuture;
use moka::sync::Cache;

use crate::{
    checkpoint::{CheckpointId, CheckpointMeta, CheckpointStore},
    segment::SegmentIo,
    types::{Result, SegmentId},
};

/// Discriminator for cached blob types to avoid key collisions when the same logical
/// identifier space is shared across resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheKind {
    Checkpoint,
    Segment,
}

/// Composite cache key derived from logical identifier and optional strong revision id (ETag).
#[derive(Clone)]
pub struct CacheKey {
    kind: CacheKind,
    key: Box<str>,
    etag: Option<Box<str>>,
}

impl CacheKey {
    pub fn new(kind: CacheKind, key: impl Into<String>, etag: Option<String>) -> Self {
        Self {
            kind,
            key: key.into().into_boxed_str(),
            etag: etag.map(|e| e.into_boxed_str()),
        }
    }

    pub fn kind(&self) -> CacheKind {
        self.kind
    }

    pub fn identifier(&self) -> &str {
        &self.key
    }

    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
}

impl fmt::Debug for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheKey")
            .field("kind", &self.kind)
            .field("identifier", &self.key)
            .field("etag", &self.etag)
            .finish()
    }
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.key == other.key && self.etag == other.etag
    }
}

impl Eq for CacheKey {}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.key.hash(state);
        self.etag.hash(state);
    }
}

/// Cache entry payload shared among readers.
#[derive(Clone, Default)]
pub struct CachedPayload(pub Arc<[u8]>);

impl CachedPayload {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes.into_boxed_slice().into())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for CachedPayload {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

#[derive(Clone)]
pub struct CheckpointCacheEntry {
    pub meta: Arc<CheckpointMeta>,
    pub payload: CachedPayload,
    pub etag: Option<String>,
}

impl CheckpointCacheEntry {
    pub fn new(meta: CheckpointMeta, payload: Vec<u8>, etag: Option<String>) -> Self {
        Self {
            meta: Arc::new(meta),
            payload: CachedPayload::new(payload),
            etag,
        }
    }
}

/// Unified cache value enum so we can share the underlying LRU for segments and checkpoints.
#[derive(Clone)]
pub enum CacheValue {
    Segment(CachedPayload),
    Checkpoint(CheckpointCacheEntry),
}

/// Basic cache statistics exposed to callers for observability.
#[derive(Default, Debug, Clone, Copy)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
}

pub trait BlobCache: Send + Sync {
    fn get(&self, key: &CacheKey) -> Option<CacheValue>;
    fn insert(&self, key: CacheKey, value: CacheValue);
    fn invalidate(&self, key: &CacheKey);
    fn metrics(&self) -> CacheMetrics;
}

/// Cache configuration passed into manifest builders.
#[derive(Clone)]
pub enum CacheLayer {
    Memory { max_bytes: u64 },
    Shared(Arc<dyn BlobCache>),
}

impl fmt::Debug for CacheLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheLayer::Memory { max_bytes } => f
                .debug_struct("CacheLayer::Memory")
                .field("max_bytes", max_bytes)
                .finish(),
            CacheLayer::Shared(_) => f
                .debug_struct("CacheLayer::Shared")
                .field("cache", &"Arc<dyn BlobCache>")
                .finish(),
        }
    }
}

impl CacheLayer {
    pub fn into_cache(self) -> Arc<dyn BlobCache> {
        match self {
            CacheLayer::Memory { max_bytes } => Arc::new(MemoryBlobCache::new(max_bytes)),
            CacheLayer::Shared(cache) => cache,
        }
    }
}

/// In-memory weighted LRU cache backed by moka with per-entry byte accounting.
pub struct MemoryBlobCache {
    cache: Cache<CacheKey, CacheValue>,
    hits: AtomicU64,
    misses: AtomicU64,
    insertions: AtomicU64,
    evictions: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct CachedSegmentStore<S> {
    inner: Arc<S>,
    cache: Option<Arc<dyn BlobCache>>,
    etags: Arc<Mutex<HashMap<String, Option<String>>>>,
}

impl<S> CachedSegmentStore<S> {
    pub fn new(inner: S, cache: Option<Arc<dyn BlobCache>>) -> Self {
        Self {
            inner: Arc::new(inner),
            cache,
            etags: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn identifier(id: &SegmentId) -> String {
        format!("seg-{:020}", id.seq)
    }
}

impl<S> SegmentIo for CachedSegmentStore<S>
where
    S: SegmentIo + Send + Sync,
{
    fn put_next<'s>(
        &'s self,
        seq: u64,
        txn_id: u64,
        payload: &'s [u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<SegmentId>> + 's {
        self.inner.put_next(seq, txn_id, payload, content_type)
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
        let cache_lookup = self.cache.clone();
        let cache_store = self.cache.clone();
        let inner = self.inner.clone();
        let etags = self.etags.clone();
        let seg_id = *id;
        let identifier = CachedSegmentStore::<S>::identifier(id);
        let known_etag = {
            self.etags
                .lock()
                .ok()
                .and_then(|map| map.get(&identifier).cloned())
                .flatten()
        };
        async move {
            if let Some(cache) = cache_lookup.as_ref() {
                let primary =
                    CacheKey::new(CacheKind::Segment, identifier.clone(), known_etag.clone());
                if let Some(CacheValue::Segment(bytes)) = cache.get(&primary) {
                    return Ok((bytes.as_slice().to_vec(), known_etag));
                }

                if known_etag.is_some() {
                    let fallback = CacheKey::new(CacheKind::Segment, identifier.clone(), None);
                    if let Some(CacheValue::Segment(bytes)) = cache.get(&fallback) {
                        return Ok((bytes.as_slice().to_vec(), None));
                    }
                }
            }

            let (bytes, etag) = inner.get_with_etag(&seg_id).await?;

            if let Some(cache) = cache_store {
                match &etag {
                    Some(tag) => {
                        cache.invalidate(&CacheKey::new(
                            CacheKind::Segment,
                            identifier.clone(),
                            None,
                        ));
                        cache.insert(
                            CacheKey::new(
                                CacheKind::Segment,
                                identifier.clone(),
                                Some(tag.clone()),
                            ),
                            CacheValue::Segment(CachedPayload::new(bytes.clone())),
                        );
                    }
                    None => {
                        cache.insert(
                            CacheKey::new(CacheKind::Segment, identifier.clone(), None),
                            CacheValue::Segment(CachedPayload::new(bytes.clone())),
                        );
                    }
                }
            }

            if let Ok(mut map) = etags.lock() {
                map.insert(identifier, etag.clone());
            }

            Ok((bytes, etag))
        }
    }

    fn load_meta(
        &self,
        id: &SegmentId,
    ) -> impl MaybeSendFuture<Output = Result<crate::segment::SegmentMeta>> + '_ {
        self.inner.load_meta(id)
    }

    fn list_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> impl MaybeSendFuture<Output = Result<Vec<SegmentId>>> + '_ {
        self.inner.list_from(from_seq, limit)
    }

    fn delete_upto(&self, upto_seq: u64) -> impl MaybeSendFuture<Output = Result<()>> + '_ {
        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let etags = self.etags.clone();
        async move {
            inner.delete_upto(upto_seq).await?;

            if let Ok(mut map) = etags.lock() {
                for seq in 0..=upto_seq {
                    let id = SegmentId { seq };
                    let identifier = CachedSegmentStore::<S>::identifier(&id);

                    if let Some(cache) = cache.as_ref() {
                        cache.invalidate(&CacheKey::new(
                            CacheKind::Segment,
                            identifier.clone(),
                            None,
                        ));
                    }

                    if let Some(tag) = map.remove(&identifier) {
                        if let (Some(cache), Some(tag)) = (cache.as_ref(), tag) {
                            cache.invalidate(&CacheKey::new(
                                CacheKind::Segment,
                                identifier.clone(),
                                Some(tag),
                            ));
                        }
                    }
                }
            }

            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct CachedCheckpointStore<S> {
    inner: Arc<S>,
    cache: Option<Arc<dyn BlobCache>>,
    etags: Arc<Mutex<HashMap<String, Option<String>>>>,
}

impl<S> CachedCheckpointStore<S> {
    pub fn new(inner: S, cache: Option<Arc<dyn BlobCache>>) -> Self {
        Self {
            inner: Arc::new(inner),
            cache,
            etags: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn cache_key(id: &CheckpointId) -> CacheKey {
        // Checkpoints are logically immutable per LSN. Cache invalidation on delete
        // ensures coherency. ETags would add overhead (extra HEAD requests) for negligible benefit.
        CacheKey::new(CacheKind::Checkpoint, id.as_str(), None)
    }

    fn identifier(id: &CheckpointId) -> String {
        id.as_str().to_owned()
    }
}

impl<S> CheckpointStore for CachedCheckpointStore<S>
where
    S: CheckpointStore + Send + Sync,
{
    fn put_checkpoint<'s>(
        &'s self,
        meta: &CheckpointMeta,
        payload: &'s [u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointId>> + 's {
        let cache = self.cache.clone();
        let meta_clone = meta.clone();
        let payload_owned = payload.to_vec();
        let inner = self.inner.clone();
        let content_type_owned = content_type.to_owned();
        let etags = self.etags.clone();
        async move {
            let id = inner
                .put_checkpoint(&meta_clone, &payload_owned, &content_type_owned)
                .await?;
            let key = CachedCheckpointStore::<S>::cache_key(&id);
            if let Some(cache) = cache {
                cache.insert(
                    key,
                    CacheValue::Checkpoint(CheckpointCacheEntry::new(
                        meta_clone.clone(),
                        payload_owned.clone(),
                        None,
                    )),
                );
            }
            if let Ok(mut map) = etags.lock() {
                map.insert(CachedCheckpointStore::<S>::identifier(&id), None);
            }
            Ok(id)
        }
    }

    fn get_checkpoint<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>> + 'a {
        async move {
            let (meta, payload, _) =
                <Self as CheckpointStore>::get_checkpoint_with_etag(self, id).await?;
            Ok((meta, payload))
        }
    }

    fn get_checkpoint_with_etag<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>, Option<String>)>> + 'a {
        let cache_lookup = self.cache.clone();
        let cache_store = self.cache.clone();
        let inner = self.inner.clone();
        let etags = self.etags.clone();
        let ckpt_id = id.clone();
        let identifier = CachedCheckpointStore::<S>::identifier(id);
        let known_etag = {
            self.etags
                .lock()
                .ok()
                .and_then(|map| map.get(&identifier).cloned())
                .flatten()
        };
        async move {
            if let Some(cache) = cache_lookup.as_ref() {
                let primary = CacheKey::new(
                    CacheKind::Checkpoint,
                    identifier.clone(),
                    known_etag.clone(),
                );
                if let Some(CacheValue::Checkpoint(entry)) = cache.get(&primary) {
                    return Ok((
                        (*entry.meta).clone(),
                        entry.payload.as_slice().to_vec(),
                        known_etag,
                    ));
                }

                if known_etag.is_some() {
                    let fallback = CacheKey::new(CacheKind::Checkpoint, identifier.clone(), None);
                    if let Some(CacheValue::Checkpoint(entry)) = cache.get(&fallback) {
                        return Ok((
                            (*entry.meta).clone(),
                            entry.payload.as_slice().to_vec(),
                            entry.etag.clone(),
                        ));
                    }
                }
            }

            let (meta, payload, etag) = inner.get_checkpoint_with_etag(&ckpt_id).await?;

            if let Some(cache) = cache_store {
                match &etag {
                    Some(tag) => {
                        cache.invalidate(&CacheKey::new(
                            CacheKind::Checkpoint,
                            identifier.clone(),
                            None,
                        ));
                        cache.insert(
                            CacheKey::new(
                                CacheKind::Checkpoint,
                                identifier.clone(),
                                Some(tag.clone()),
                            ),
                            CacheValue::Checkpoint(CheckpointCacheEntry::new(
                                meta.clone(),
                                payload.clone(),
                                Some(tag.clone()),
                            )),
                        );
                    }
                    None => {
                        cache.insert(
                            CacheKey::new(CacheKind::Checkpoint, identifier.clone(), None),
                            CacheValue::Checkpoint(CheckpointCacheEntry::new(
                                meta.clone(),
                                payload.clone(),
                                None,
                            )),
                        );
                    }
                }
            }

            if let Ok(mut map) = etags.lock() {
                map.insert(identifier, etag.clone());
            }

            Ok((meta, payload, etag))
        }
    }

    fn get_checkpoint_meta<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointMeta>> + 'a {
        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let identifier = CachedCheckpointStore::<S>::identifier(id);
        let known_etag = {
            self.etags
                .lock()
                .ok()
                .and_then(|map| map.get(&identifier).cloned())
                .flatten()
        };
        async move {
            if let Some(cache) = cache.as_ref() {
                let primary = CacheKey::new(
                    CacheKind::Checkpoint,
                    identifier.clone(),
                    known_etag.clone(),
                );
                if let Some(CacheValue::Checkpoint(entry)) = cache.get(&primary) {
                    return Ok((*entry.meta).clone());
                }

                if known_etag.is_some() {
                    let fallback = CacheKey::new(CacheKind::Checkpoint, identifier.clone(), None);
                    if let Some(CacheValue::Checkpoint(entry)) = cache.get(&fallback) {
                        return Ok((*entry.meta).clone());
                    }
                }
            }

            inner.get_checkpoint_meta(id).await
        }
    }

    fn list(
        &self,
    ) -> impl MaybeSendFuture<
        Output = Result<
            impl futures_util::Stream<Item = Result<(CheckpointId, CheckpointMeta)>> + '_,
        >,
    > + '_ {
        self.inner.list()
    }

    fn delete(&self, id: &CheckpointId) -> impl MaybeSendFuture<Output = Result<()>> + '_ {
        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let ckpt_id = id.clone();
        let etags = self.etags.clone();
        async move {
            inner.delete(&ckpt_id).await?;
            let identifier = CachedCheckpointStore::<S>::identifier(&ckpt_id);

            if let Some(cache) = cache.as_ref() {
                cache.invalidate(&CacheKey::new(
                    CacheKind::Checkpoint,
                    identifier.clone(),
                    None,
                ));
            }

            if let Ok(mut map) = etags.lock() {
                if let Some(tag) = map.remove(&identifier) {
                    if let (Some(cache), Some(tag)) = (cache.as_ref(), tag) {
                        cache.invalidate(&CacheKey::new(
                            CacheKind::Checkpoint,
                            identifier,
                            Some(tag),
                        ));
                    }
                }
            }
            Ok(())
        }
    }
}

impl MemoryBlobCache {
    pub fn new(max_bytes: u64) -> Self {
        let hits = AtomicU64::new(0);
        let misses = AtomicU64::new(0);
        let insertions = AtomicU64::new(0);
        let evictions = Arc::new(AtomicU64::new(0));
        let eviction_counter = evictions.clone();

        let cache = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_, value: &CacheValue| match value {
                CacheValue::Segment(bytes) => bytes.len() as u32,
                CacheValue::Checkpoint(entry) => {
                    // Count payload bytes only; meta size is negligible.
                    entry.payload.len() as u32
                }
            })
            .eviction_listener(move |_key, _value, _cause| {
                eviction_counter.fetch_add(1, Ordering::Relaxed);
            })
            .build();

        Self {
            cache,
            hits,
            misses,
            insertions,
            evictions,
        }
    }
}

impl BlobCache for MemoryBlobCache {
    fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        match self.cache.get(key) {
            Some(val) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(val)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    fn insert(&self, key: CacheKey, value: CacheValue) {
        self.cache.insert(key, value);
        self.insertions.fetch_add(1, Ordering::Relaxed);
    }

    fn invalidate(&self, key: &CacheKey) {
        self.cache.invalidate(key);
    }

    fn metrics(&self) -> CacheMetrics {
        CacheMetrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            insertions: self.insertions.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;

    use super::*;
    use crate::{checkpoint::CheckpointMeta, testing::new_inmemory_stores};

    #[test]
    fn segment_cache_hits_after_warm() {
        let cache = Arc::new(MemoryBlobCache::new(1024));
        let (_head, segment, _checkpoint, _leases) = new_inmemory_stores();
        let cached = CachedSegmentStore::new(segment.clone(), Some(cache.clone()));

        block_on(async {
            let id = segment
                .put_next(1, 42, br#"{"records":[]}"#, "application/json")
                .await
                .unwrap();
            let first = cached.get(&id).await.unwrap();
            assert!(!first.is_empty());
            let miss_metrics = cache.metrics();
            assert_eq!(miss_metrics.misses, 1);
            let second = cached.get(&id).await.unwrap();
            assert_eq!(second, first);
            let hit_metrics = cache.metrics();
            assert_eq!(hit_metrics.hits, 1);
        });
    }

    #[test]
    fn checkpoint_cache_serves_from_memory() {
        let cache = Arc::new(MemoryBlobCache::new(1024));
        let (_head, _segment, checkpoint, _leases) = new_inmemory_stores();
        let cached = CachedCheckpointStore::new(checkpoint.clone(), Some(cache.clone()));

        block_on(async {
            let payload = br#"{"entries":[]}"#;
            let meta = CheckpointMeta {
                lsn: 7,
                key_count: 0,
                byte_size: payload.len(),
                created_at_ms: 0,
                format: "application/json".into(),
                last_segment_seq_at_ckpt: 0,
            };
            let id = cached
                .put_checkpoint(&meta, payload, "application/json")
                .await
                .unwrap();
            // First read should already hit thanks to warm insert.
            let (_meta, bytes) = cached.get_checkpoint(&id).await.unwrap();
            assert_eq!(bytes, payload);
            let metrics = cache.metrics();
            assert_eq!(metrics.hits, 1);
            assert_eq!(metrics.misses, 0);
        });
    }

    #[test]
    fn checkpoint_cache_invalidate_by_etag() {
        let cache = Arc::new(MemoryBlobCache::new(1024));
        let (_head, _segment, checkpoint, _leases) = new_inmemory_stores();
        let cached = CachedCheckpointStore::new(checkpoint.clone(), Some(cache.clone()));

        block_on(async {
            let payload = br#"{"entries":[]}"#;
            let meta = CheckpointMeta {
                lsn: 9,
                key_count: 0,
                byte_size: payload.len(),
                created_at_ms: 0,
                format: "application/json".into(),
                last_segment_seq_at_ckpt: 0,
            };

            let id = cached
                .put_checkpoint(&meta, payload, "application/json")
                .await
                .unwrap();

            let (_meta, bytes, _etag) = cached.get_checkpoint_with_etag(&id).await.unwrap();
            assert_eq!(bytes, payload);

            let identifier = id.as_str().to_owned();
            let key_none = CacheKey::new(CacheKind::Checkpoint, identifier.clone(), None);
            assert!(cache.get(&key_none).is_some());

            // Simulate an entry cached under a strong revision tag.
            let fake_tag = "etag-simulated".to_string();
            cache.insert(
                CacheKey::new(
                    CacheKind::Checkpoint,
                    identifier.clone(),
                    Some(fake_tag.clone()),
                ),
                CacheValue::Checkpoint(CheckpointCacheEntry::new(
                    meta.clone(),
                    payload.to_vec(),
                    Some(fake_tag.clone()),
                )),
            );
            if let Ok(mut map) = cached.etags.lock() {
                map.insert(identifier.clone(), Some(fake_tag.clone()));
            }

            let key_etag = CacheKey::new(
                CacheKind::Checkpoint,
                identifier.clone(),
                Some(fake_tag.clone()),
            );
            assert!(cache.get(&key_etag).is_some());

            cached.delete(&id).await.unwrap();

            assert!(cache.get(&key_etag).is_none());
            assert!(cache.get(&key_none).is_none());
        });
    }
}

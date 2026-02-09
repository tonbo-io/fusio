use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use fusio::{executor::tokio::TokioExecutor, impls::disk::TokioFs};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use fusio_manifest::{
    context::ManifestContext,
    manifest::Manifest,
    retention::DefaultRetention,
    types::{Result, SegmentId},
    BackoffPolicy, CacheLayer, CheckpointId, CheckpointMeta, CheckpointStore, CheckpointStoreImpl,
    HeadStoreImpl, LeaseStoreImpl, SegmentIo, SegmentMeta, SegmentStoreImpl,
};

pub type BenchManifest = Manifest<
    String,
    String,
    HeadStoreImpl<TokioFs>,
    CountingSegmentStore<SegmentStoreImpl<TokioFs>>,
    CountingCheckpointStore<CheckpointStoreImpl<TokioFs>>,
    LeaseStoreImpl<TokioFs, TokioExecutor>,
    TokioExecutor,
    DefaultRetention,
>;

#[derive(Clone, Default)]
pub struct IoCounters {
    segment_load_meta: Arc<AtomicU64>,
    segment_get: Arc<AtomicU64>,
    checkpoint_index_get: Arc<AtomicU64>,
    checkpoint_meta_get: Arc<AtomicU64>,
    checkpoint_full_get: Arc<AtomicU64>,
    checkpoint_range_get: Arc<AtomicU64>,
    checkpoint_payload_range_get: Arc<AtomicU64>,
    checkpoint_requested_bytes: Arc<AtomicU64>,
}

impl IoCounters {
    pub fn reset(&self) {
        self.segment_load_meta.store(0, Ordering::Relaxed);
        self.segment_get.store(0, Ordering::Relaxed);
        self.checkpoint_index_get.store(0, Ordering::Relaxed);
        self.checkpoint_meta_get.store(0, Ordering::Relaxed);
        self.checkpoint_full_get.store(0, Ordering::Relaxed);
        self.checkpoint_range_get.store(0, Ordering::Relaxed);
        self.checkpoint_payload_range_get
            .store(0, Ordering::Relaxed);
        self.checkpoint_requested_bytes.store(0, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> IoSnapshot {
        IoSnapshot {
            segment_load_meta: self.segment_load_meta.load(Ordering::Relaxed),
            segment_get: self.segment_get.load(Ordering::Relaxed),
            checkpoint_index_get: self.checkpoint_index_get.load(Ordering::Relaxed),
            checkpoint_meta_get: self.checkpoint_meta_get.load(Ordering::Relaxed),
            checkpoint_full_get: self.checkpoint_full_get.load(Ordering::Relaxed),
            checkpoint_range_get: self.checkpoint_range_get.load(Ordering::Relaxed),
            checkpoint_payload_range_get: self.checkpoint_payload_range_get.load(Ordering::Relaxed),
            checkpoint_requested_bytes: self.checkpoint_requested_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct IoSnapshot {
    pub segment_load_meta: u64,
    pub segment_get: u64,
    pub checkpoint_index_get: u64,
    pub checkpoint_meta_get: u64,
    pub checkpoint_full_get: u64,
    pub checkpoint_range_get: u64,
    pub checkpoint_payload_range_get: u64,
    pub checkpoint_requested_bytes: u64,
}

#[derive(Clone)]
pub struct CountingSegmentStore<S> {
    inner: S,
    counters: IoCounters,
}

impl<S> CountingSegmentStore<S> {
    fn new(inner: S, counters: IoCounters) -> Self {
        Self { inner, counters }
    }
}

impl<S> SegmentIo for CountingSegmentStore<S>
where
    S: SegmentIo + Clone + MaybeSend + MaybeSync,
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
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let seg = *id;
        async move {
            counters.segment_get.fetch_add(1, Ordering::Relaxed);
            inner.get(&seg).await
        }
    }

    fn get_with_etag<'a>(
        &'a self,
        id: &'a SegmentId,
    ) -> impl MaybeSendFuture<Output = Result<(Vec<u8>, Option<String>)>> + 'a {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let seg = *id;
        async move {
            counters.segment_get.fetch_add(1, Ordering::Relaxed);
            inner.get_with_etag(&seg).await
        }
    }

    fn load_meta(&self, id: &SegmentId) -> impl MaybeSendFuture<Output = Result<SegmentMeta>> + '_ {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let seg = *id;
        async move {
            counters.segment_load_meta.fetch_add(1, Ordering::Relaxed);
            inner.load_meta(&seg).await
        }
    }

    fn list_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> impl MaybeSendFuture<Output = Result<Vec<SegmentId>>> + '_ {
        self.inner.list_from(from_seq, limit)
    }

    fn delete_upto(&self, upto_seq: u64) -> impl MaybeSendFuture<Output = Result<()>> + '_ {
        self.inner.delete_upto(upto_seq)
    }
}

#[derive(Clone)]
pub struct CountingCheckpointStore<S> {
    inner: S,
    counters: IoCounters,
}

impl<S> CountingCheckpointStore<S> {
    fn new(inner: S, counters: IoCounters) -> Self {
        Self { inner, counters }
    }
}

impl<S> CheckpointStore for CountingCheckpointStore<S>
where
    S: CheckpointStore + Clone + MaybeSend + MaybeSync,
{
    fn put_checkpoint<'s>(
        &'s self,
        meta: &CheckpointMeta,
        payload: &'s [u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointId>> + 's {
        self.inner.put_checkpoint(meta, payload, content_type)
    }

    fn get_checkpoint<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>> + 'a {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let ckpt = id.clone();
        async move {
            counters.checkpoint_full_get.fetch_add(1, Ordering::Relaxed);
            inner.get_checkpoint(&ckpt).await
        }
    }

    fn get_checkpoint_with_etag<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>, Option<String>)>> + 'a {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let ckpt = id.clone();
        async move {
            counters.checkpoint_full_get.fetch_add(1, Ordering::Relaxed);
            inner.get_checkpoint_with_etag(&ckpt).await
        }
    }

    fn put_checkpoint_index<'s>(
        &'s self,
        id: &'s CheckpointId,
        payload: &'s [u8],
        content_type: &str,
    ) -> impl MaybeSendFuture<Output = Result<()>> + 's {
        self.inner.put_checkpoint_index(id, payload, content_type)
    }

    fn get_checkpoint_index_with_etag<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<(Option<Vec<u8>>, Option<String>)>> + 'a {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let ckpt = id.clone();
        async move {
            counters
                .checkpoint_index_get
                .fetch_add(1, Ordering::Relaxed);
            inner.get_checkpoint_index_with_etag(&ckpt).await
        }
    }

    fn get_checkpoint_index<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<Option<Vec<u8>>>> + 'a {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let ckpt = id.clone();
        async move {
            counters
                .checkpoint_index_get
                .fetch_add(1, Ordering::Relaxed);
            inner.get_checkpoint_index(&ckpt).await
        }
    }

    fn get_checkpoint_meta<'a>(
        &'a self,
        id: &'a CheckpointId,
    ) -> impl MaybeSendFuture<Output = Result<CheckpointMeta>> + 'a {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let ckpt = id.clone();
        async move {
            counters.checkpoint_meta_get.fetch_add(1, Ordering::Relaxed);
            inner.get_checkpoint_meta(&ckpt).await
        }
    }

    fn get_checkpoint_range<'a>(
        &'a self,
        id: &'a CheckpointId,
        offset: u64,
        len: usize,
    ) -> impl MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>> + 'a {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let ckpt = id.clone();
        async move {
            counters
                .checkpoint_range_get
                .fetch_add(1, Ordering::Relaxed);
            counters
                .checkpoint_requested_bytes
                .fetch_add(len as u64, Ordering::Relaxed);
            inner.get_checkpoint_range(&ckpt, offset, len).await
        }
    }

    fn get_checkpoint_payload_range<'a>(
        &'a self,
        id: &'a CheckpointId,
        offset: u64,
        len: usize,
    ) -> impl MaybeSendFuture<Output = Result<Vec<u8>>> + 'a {
        let inner = self.inner.clone();
        let counters = self.counters.clone();
        let ckpt = id.clone();
        async move {
            counters
                .checkpoint_payload_range_get
                .fetch_add(1, Ordering::Relaxed);
            counters
                .checkpoint_requested_bytes
                .fetch_add(len as u64, Ordering::Relaxed);
            inner.get_checkpoint_payload_range(&ckpt, offset, len).await
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
        self.inner.delete(id)
    }
}

pub struct BenchConfig {
    pub key_count: usize,
    pub value_bytes: usize,
    pub segment_batch: usize,
    pub query_count: usize,
    pub scan_query_count: usize,
    pub scan_range_width: usize,
    pub prewarm_count: usize,
    pub sparse_stride: usize,
    pub run_block_target_bytes: usize,
    pub run_block_max_records: usize,
    pub multi_level_epochs: usize,
    pub multi_level_keys_per_epoch: usize,
    pub cache_enabled: bool,
    pub cache_bytes: u64,
    pub bloom_enabled: bool,
}

impl BenchConfig {
    pub fn from_env() -> Self {
        Self {
            key_count: env_usize("FUSIO_MANIFEST_BENCH_KEYS", 100_000).max(1),
            value_bytes: env_usize("FUSIO_MANIFEST_BENCH_VALUE_BYTES", 128),
            segment_batch: env_usize("FUSIO_MANIFEST_BENCH_SEGMENT_BATCH", 64).max(1),
            query_count: env_usize("FUSIO_MANIFEST_BENCH_QUERY_COUNT", 20_000).max(1),
            scan_query_count: env_usize("FUSIO_MANIFEST_BENCH_SCAN_QUERY_COUNT", 2_000).max(1),
            scan_range_width: env_usize("FUSIO_MANIFEST_BENCH_SCAN_RANGE_WIDTH", 64).max(1),
            prewarm_count: env_usize("FUSIO_MANIFEST_BENCH_PREWARM", 2_000),
            sparse_stride: env_usize("FUSIO_MANIFEST_BENCH_SPARSE_STRIDE", 64).max(1),
            run_block_target_bytes: env_usize(
                "FUSIO_MANIFEST_BENCH_RUN_BLOCK_TARGET_BYTES",
                256 * 1024,
            )
            .max(1),
            run_block_max_records: env_usize("FUSIO_MANIFEST_BENCH_RUN_BLOCK_MAX_RECORDS", 4096)
                .max(1),
            multi_level_epochs: env_usize("FUSIO_MANIFEST_BENCH_MULTI_LEVEL_EPOCHS", 31).max(1),
            multi_level_keys_per_epoch: env_usize(
                "FUSIO_MANIFEST_BENCH_MULTI_LEVEL_KEYS_PER_EPOCH",
                1024,
            )
            .max(1),
            cache_enabled: env_bool("FUSIO_MANIFEST_BENCH_ENABLE_CACHE", false),
            cache_bytes: env_usize("FUSIO_MANIFEST_BENCH_CACHE_BYTES", 64 * 1024 * 1024) as u64,
            bloom_enabled: env_bool("FUSIO_MANIFEST_BENCH_ENABLE_BLOOM", true),
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

pub fn criterion_cli_quiet() -> bool {
    std::env::args().any(|arg| arg == "--quiet")
}

pub fn print_bench_header(label: &str, cfg: &BenchConfig, quiet: bool) {
    if quiet {
        return;
    }

    #[cfg(feature = "cache-moka")]
    eprintln!(
        "[{label}] keys={} value_bytes={} segment_batch={} query_count={} scan_query_count={} \
         scan_range_width={} prewarm={} sparse_stride={} multi_epochs={} multi_keys_per_epoch={} \
         cache_enabled={} cache_bytes={} bloom_enabled={} run_block_target_bytes={} \
         run_block_max_records={}",
        cfg.key_count,
        cfg.value_bytes,
        cfg.segment_batch,
        cfg.query_count,
        cfg.scan_query_count,
        cfg.scan_range_width,
        cfg.prewarm_count,
        cfg.sparse_stride,
        cfg.multi_level_epochs,
        cfg.multi_level_keys_per_epoch,
        cfg.cache_enabled,
        cfg.cache_bytes,
        cfg.bloom_enabled,
        cfg.run_block_target_bytes,
        cfg.run_block_max_records
    );

    #[cfg(not(feature = "cache-moka"))]
    eprintln!(
        "[{label}] keys={} value_bytes={} segment_batch={} query_count={} scan_query_count={} \
         scan_range_width={} prewarm={} sparse_stride={} multi_epochs={} multi_keys_per_epoch={} \
         cache_enabled={} bloom_enabled={} run_block_target_bytes={} run_block_max_records={} \
         (cache-moka disabled)",
        cfg.key_count,
        cfg.value_bytes,
        cfg.segment_batch,
        cfg.query_count,
        cfg.scan_query_count,
        cfg.scan_range_width,
        cfg.prewarm_count,
        cfg.sparse_stride,
        cfg.multi_level_epochs,
        cfg.multi_level_keys_per_epoch,
        cfg.cache_enabled,
        cfg.bloom_enabled,
        cfg.run_block_target_bytes,
        cfg.run_block_max_records
    );
}

pub fn base_key(index: usize) -> String {
    format!("k{index:08}")
}

pub fn multi_level_key(epoch: usize, index: usize) -> String {
    format!("e{epoch:02}:k{index:08}")
}

pub fn value_blob(len: usize) -> String {
    "x".repeat(len.max(1))
}

pub fn percentile_us(sorted_nanos: &[u64], percentile: f64) -> f64 {
    if sorted_nanos.is_empty() {
        return 0.0;
    }
    let rank = ((sorted_nanos.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    sorted_nanos[rank] as f64 / 1_000.0
}

fn create_case_dir(root: &Path) {
    std::fs::create_dir_all(root).expect("create benchmark root");
    std::fs::create_dir_all(root.join("checkpoints")).expect("create checkpoints dir");
    std::fs::create_dir_all(root.join("leases")).expect("create leases dir");
}

pub fn build_manifest(root: &Path, cfg: &BenchConfig) -> (Arc<BenchManifest>, IoCounters) {
    create_case_dir(root);
    let prefix = fusio::path::Path::from_absolute_path(root)
        .expect("convert local path to fusio path")
        .to_string();

    let fs = TokioFs;
    let head = HeadStoreImpl::new(fs.clone(), format!("{prefix}/head.json"));
    let segment = SegmentStoreImpl::new(fs.clone(), prefix.clone());
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), prefix.clone());
    let lease = LeaseStoreImpl::new(
        fs,
        prefix,
        BackoffPolicy::default(),
        TokioExecutor::default(),
    );

    let counters = IoCounters::default();
    let segment = CountingSegmentStore::new(segment, counters.clone());
    let checkpoint = CountingCheckpointStore::new(checkpoint, counters.clone());
    let mut context = ManifestContext::new(TokioExecutor::default())
        .with_sparse_stride(cfg.sparse_stride)
        .with_run_bloom_enabled(cfg.bloom_enabled)
        .with_run_block_target_bytes(cfg.run_block_target_bytes)
        .with_run_block_max_records(cfg.run_block_max_records);
    #[cfg(feature = "cache-moka")]
    if cfg.cache_enabled {
        context = context.with_cache(Some(
            CacheLayer::Memory {
                max_bytes: cfg.cache_bytes,
            }
            .into_cache(),
        ));
    }
    let context = Arc::new(context);
    let manifest = Manifest::new_with_context(head, segment, checkpoint, lease, context);
    (Arc::new(manifest), counters)
}

pub async fn write_base_segments(
    manifest: &BenchManifest,
    cfg: &BenchConfig,
    value: &str,
) -> Result<()> {
    for start in (0..cfg.key_count).step_by(cfg.segment_batch) {
        let mut write = manifest.session_write().await?;
        let end = (start + cfg.segment_batch).min(cfg.key_count);
        for idx in start..end {
            write.put(base_key(idx), value.to_owned());
        }
        write.commit().await?;
    }
    Ok(())
}

pub async fn write_multi_level_segments(
    manifest: &BenchManifest,
    cfg: &BenchConfig,
    value: &str,
) -> Result<()> {
    let compactor = manifest.compactor();
    for epoch in 0..cfg.multi_level_epochs {
        let mut write = manifest.session_write().await?;
        for idx in 0..cfg.multi_level_keys_per_epoch {
            write.put(multi_level_key(epoch, idx), value.to_owned());
        }
        write.commit().await?;
        compactor.compact_once().await?;
    }
    Ok(())
}

fn workspace_criterion_output_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|workspace_root| workspace_root.join("target").join("criterion"))
        .unwrap_or_else(|| PathBuf::from("target").join("criterion"))
}

pub fn criterion_config() -> criterion::Criterion {
    let output_dir = workspace_criterion_output_dir();
    criterion::Criterion::default().output_directory(&output_dir)
}

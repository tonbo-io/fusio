use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, Throughput};
use fusio::{executor::tokio::TokioExecutor, impls::disk::TokioFs};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use fusio_manifest::{
    context::ManifestContext,
    manifest::Manifest,
    retention::DefaultRetention,
    snapshot::Snapshot,
    types::{Result, SegmentId},
    BackoffPolicy, CacheLayer, CheckpointId, CheckpointMeta, CheckpointStore, CheckpointStoreImpl,
    HeadStoreImpl, LeaseStoreImpl, SegmentIo, SegmentMeta, SegmentStoreImpl,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use tempfile::TempDir;

type BenchManifest = Manifest<
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
struct IoCounters {
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
    fn reset(&self) {
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

    fn snapshot(&self) -> IoSnapshot {
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
struct IoSnapshot {
    segment_load_meta: u64,
    segment_get: u64,
    checkpoint_index_get: u64,
    checkpoint_meta_get: u64,
    checkpoint_full_get: u64,
    checkpoint_range_get: u64,
    checkpoint_payload_range_get: u64,
    checkpoint_requested_bytes: u64,
}

#[derive(Clone)]
struct CountingSegmentStore<S> {
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
struct CountingCheckpointStore<S> {
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

struct BenchConfig {
    key_count: usize,
    value_bytes: usize,
    segment_batch: usize,
    query_count: usize,
    prewarm_count: usize,
    sparse_stride: usize,
    multi_level_epochs: usize,
    multi_level_keys_per_epoch: usize,
    cache_enabled: bool,
    cache_bytes: u64,
    bloom_enabled: bool,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            key_count: env_usize("FUSIO_MANIFEST_BENCH_KEYS", 100_000),
            value_bytes: env_usize("FUSIO_MANIFEST_BENCH_VALUE_BYTES", 128),
            segment_batch: env_usize("FUSIO_MANIFEST_BENCH_SEGMENT_BATCH", 64).max(1),
            query_count: env_usize("FUSIO_MANIFEST_BENCH_QUERY_COUNT", 20_000).max(1),
            prewarm_count: env_usize("FUSIO_MANIFEST_BENCH_PREWARM", 2_000),
            sparse_stride: env_usize("FUSIO_MANIFEST_BENCH_SPARSE_STRIDE", 64).max(1),
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

struct PreparedCase {
    name: &'static str,
    manifest: Arc<BenchManifest>,
    snapshot: Snapshot,
    queries: Arc<Vec<String>>,
    expected_hits: usize,
    counters: IoCounters,
    _tempdir: TempDir,
}

struct ProbeStats {
    total: Duration,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    hits: usize,
    io: IoSnapshot,
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

fn base_key(index: usize) -> String {
    format!("k{index:08}")
}

fn multi_level_key(epoch: usize, index: usize) -> String {
    format!("e{epoch:02}:k{index:08}")
}

fn value_blob(len: usize) -> String {
    "x".repeat(len.max(1))
}

fn build_hit_queries_for_base(key_count: usize, query_count: usize, seed: u64) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..query_count)
        .map(|_| {
            let idx = rng.gen_range(0..key_count);
            base_key(idx)
        })
        .collect()
}

fn build_hit_queries_for_multilevel(
    epoch: usize,
    keys_per_epoch: usize,
    query_count: usize,
    seed: u64,
) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..query_count)
        .map(|_| {
            let idx = rng.gen_range(0..keys_per_epoch);
            multi_level_key(epoch, idx)
        })
        .collect()
}

fn build_miss_queries(query_count: usize) -> Vec<String> {
    (0..query_count)
        .map(|idx| format!("zz-miss-{idx:08}"))
        .collect()
}

fn build_in_range_miss_queries_for_base(
    key_count: usize,
    query_count: usize,
    seed: u64,
) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(seed);
    let upper = key_count.saturating_sub(1).max(1);
    (0..query_count)
        .map(|_| {
            let idx = rng.gen_range(0..upper);
            format!("{}~", base_key(idx))
        })
        .collect()
}

fn build_in_range_miss_queries_for_multilevel(
    epoch: usize,
    keys_per_epoch: usize,
    query_count: usize,
    seed: u64,
) -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(seed);
    let upper = keys_per_epoch.saturating_sub(1).max(1);
    (0..query_count)
        .map(|_| {
            let idx = rng.gen_range(0..upper);
            format!("{}~", multi_level_key(epoch, idx))
        })
        .collect()
}

fn percentile_us(sorted_nanos: &[u64], percentile: f64) -> f64 {
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

fn build_manifest(root: &Path, cfg: &BenchConfig) -> (Arc<BenchManifest>, IoCounters) {
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
        .with_run_bloom_enabled(cfg.bloom_enabled);
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

async fn write_base_segments(
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

async fn write_multi_level_segments(
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

async fn run_query_batch(
    manifest: &BenchManifest,
    snapshot: &Snapshot,
    queries: &[String],
) -> Result<usize> {
    let session = manifest.session_at(snapshot.clone()).await?;
    let mut hits = 0;
    for key in queries {
        if session.get(key).await?.is_some() {
            hits += 1;
        }
    }
    session.end().await?;
    Ok(hits)
}

async fn run_probe(
    manifest: &BenchManifest,
    snapshot: &Snapshot,
    queries: &[String],
    counters: &IoCounters,
) -> Result<ProbeStats> {
    counters.reset();
    let session = manifest.session_at(snapshot.clone()).await?;
    let mut hits = 0;
    let mut latencies = Vec::with_capacity(queries.len());
    let batch_start = Instant::now();
    for key in queries {
        let start = Instant::now();
        if session.get(key).await?.is_some() {
            hits += 1;
        }
        latencies.push(start.elapsed().as_nanos() as u64);
    }
    let total = batch_start.elapsed();
    session.end().await?;
    latencies.sort_unstable();
    Ok(ProbeStats {
        total,
        p50_us: percentile_us(&latencies, 0.50),
        p95_us: percentile_us(&latencies, 0.95),
        p99_us: percentile_us(&latencies, 0.99),
        hits,
        io: counters.snapshot(),
    })
}

async fn prepare_l0_case(cfg: &BenchConfig, name: &'static str, hit: bool) -> Result<PreparedCase> {
    let tempdir = TempDir::new().expect("create temp dir");
    let root = tempdir.path().join(name);
    let (manifest, counters) = build_manifest(&root, cfg);
    let value = value_blob(cfg.value_bytes);
    write_base_segments(manifest.as_ref(), cfg, &value).await?;
    let snapshot = manifest.snapshot().await?;
    let queries = if hit {
        build_hit_queries_for_base(cfg.key_count, cfg.query_count, 41)
    } else {
        build_miss_queries(cfg.query_count)
    };
    Ok(PreparedCase {
        name,
        manifest,
        snapshot,
        queries: Arc::new(queries),
        expected_hits: if hit { cfg.query_count } else { 0 },
        counters,
        _tempdir: tempdir,
    })
}

async fn prepare_l1_case(cfg: &BenchConfig, name: &'static str, hit: bool) -> Result<PreparedCase> {
    let tempdir = TempDir::new().expect("create temp dir");
    let root = tempdir.path().join(name);
    let (manifest, counters) = build_manifest(&root, cfg);
    let value = value_blob(cfg.value_bytes);
    write_base_segments(manifest.as_ref(), cfg, &value).await?;
    manifest.compactor().compact_once().await?;
    let snapshot = manifest.snapshot().await?;
    let queries = if hit {
        build_hit_queries_for_base(cfg.key_count, cfg.query_count, 43)
    } else {
        build_miss_queries(cfg.query_count)
    };
    Ok(PreparedCase {
        name,
        manifest,
        snapshot,
        queries: Arc::new(queries),
        expected_hits: if hit { cfg.query_count } else { 0 },
        counters,
        _tempdir: tempdir,
    })
}

async fn prepare_l1_in_range_miss_case(
    cfg: &BenchConfig,
    name: &'static str,
) -> Result<PreparedCase> {
    let tempdir = TempDir::new().expect("create temp dir");
    let root = tempdir.path().join(name);
    let (manifest, counters) = build_manifest(&root, cfg);
    let value = value_blob(cfg.value_bytes);
    write_base_segments(manifest.as_ref(), cfg, &value).await?;
    manifest.compactor().compact_once().await?;
    let snapshot = manifest.snapshot().await?;
    let queries = build_in_range_miss_queries_for_base(cfg.key_count, cfg.query_count, 53);
    Ok(PreparedCase {
        name,
        manifest,
        snapshot,
        queries: Arc::new(queries),
        expected_hits: 0,
        counters,
        _tempdir: tempdir,
    })
}

async fn prepare_multi_level_case(
    cfg: &BenchConfig,
    name: &'static str,
    hit: bool,
) -> Result<PreparedCase> {
    let tempdir = TempDir::new().expect("create temp dir");
    let root = tempdir.path().join(name);
    let (manifest, counters) = build_manifest(&root, cfg);
    let value = value_blob(cfg.value_bytes);
    write_multi_level_segments(manifest.as_ref(), cfg, &value).await?;
    let snapshot = manifest.snapshot().await?;
    let queries = if hit {
        build_hit_queries_for_multilevel(0, cfg.multi_level_keys_per_epoch, cfg.query_count, 47)
    } else {
        build_miss_queries(cfg.query_count)
    };
    Ok(PreparedCase {
        name,
        manifest,
        snapshot,
        queries: Arc::new(queries),
        expected_hits: if hit { cfg.query_count } else { 0 },
        counters,
        _tempdir: tempdir,
    })
}

async fn prepare_multi_level_in_range_miss_case(
    cfg: &BenchConfig,
    name: &'static str,
) -> Result<PreparedCase> {
    let tempdir = TempDir::new().expect("create temp dir");
    let root = tempdir.path().join(name);
    let (manifest, counters) = build_manifest(&root, cfg);
    let value = value_blob(cfg.value_bytes);
    write_multi_level_segments(manifest.as_ref(), cfg, &value).await?;
    let snapshot = manifest.snapshot().await?;
    let queries = build_in_range_miss_queries_for_multilevel(
        0,
        cfg.multi_level_keys_per_epoch,
        cfg.query_count,
        59,
    );
    Ok(PreparedCase {
        name,
        manifest,
        snapshot,
        queries: Arc::new(queries),
        expected_hits: 0,
        counters,
        _tempdir: tempdir,
    })
}

fn point_get_local(c: &mut criterion::Criterion) {
    let cfg = BenchConfig::from_env();
    #[cfg(feature = "cache-moka")]
    eprintln!(
        "[point_get_local] keys={} value_bytes={} segment_batch={} query_count={} prewarm={} \
         sparse_stride={} multi_epochs={} multi_keys_per_epoch={} cache_enabled={} cache_bytes={} \
         bloom_enabled={}",
        cfg.key_count,
        cfg.value_bytes,
        cfg.segment_batch,
        cfg.query_count,
        cfg.prewarm_count,
        cfg.sparse_stride,
        cfg.multi_level_epochs,
        cfg.multi_level_keys_per_epoch,
        cfg.cache_enabled,
        cfg.cache_bytes,
        cfg.bloom_enabled
    );
    #[cfg(not(feature = "cache-moka"))]
    eprintln!(
        "[point_get_local] keys={} value_bytes={} segment_batch={} query_count={} prewarm={} \
         sparse_stride={} multi_epochs={} multi_keys_per_epoch={} cache_enabled={} \
         bloom_enabled={} (cache-moka disabled)",
        cfg.key_count,
        cfg.value_bytes,
        cfg.segment_batch,
        cfg.query_count,
        cfg.prewarm_count,
        cfg.sparse_stride,
        cfg.multi_level_epochs,
        cfg.multi_level_keys_per_epoch,
        cfg.cache_enabled,
        cfg.bloom_enabled
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let cases = runtime.block_on(async {
        let l0_hit = prepare_l0_case(&cfg, "L0-Hit", true).await?;
        let l0_miss = prepare_l0_case(&cfg, "L0-Miss", false).await?;
        let l1_hit = prepare_l1_case(&cfg, "L1-Hit", true).await?;
        let l1_miss = prepare_l1_case(&cfg, "L1-Miss", false).await?;
        let l1_in_range_miss = prepare_l1_in_range_miss_case(&cfg, "L1-InRangeMiss").await?;
        let ml_hit = prepare_multi_level_case(&cfg, "MultiLevel-Hit", true).await?;
        let ml_miss = prepare_multi_level_case(&cfg, "MultiLevel-Miss", false).await?;
        let ml_in_range_miss =
            prepare_multi_level_in_range_miss_case(&cfg, "MultiLevel-InRangeMiss").await?;
        Ok::<_, fusio_manifest::types::Error>(vec![
            l0_hit,
            l0_miss,
            l1_hit,
            l1_miss,
            l1_in_range_miss,
            ml_hit,
            ml_miss,
            ml_in_range_miss,
        ])
    });
    let cases = cases.expect("prepare benchmark cases");

    for case in &cases {
        let prewarm = cfg.prewarm_count.min(case.queries.len());
        runtime.block_on(async {
            case.counters.reset();
            let warm_hits = run_query_batch(
                case.manifest.as_ref(),
                &case.snapshot,
                &case.queries[..prewarm],
            )
            .await
            .expect("prewarm query batch");
            if case.expected_hits == 0 {
                assert_eq!(warm_hits, 0, "warmup miss case should stay miss");
            }
            let probe = run_probe(
                case.manifest.as_ref(),
                &case.snapshot,
                case.queries.as_ref(),
                &case.counters,
            )
            .await
            .expect("probe query batch");
            assert_eq!(
                probe.hits, case.expected_hits,
                "probe hit count mismatch for {}",
                case.name
            );
            let ops_per_sec = case.queries.len() as f64 / probe.total.as_secs_f64();
            eprintln!(
                "[{}] probe ops/s={:.2} p50={:.2}us p95={:.2}us p99={:.2}us | seg_meta={} \
                 seg_get={} ckpt_index={} ckpt_meta={} ckpt_full={} ckpt_range={} \
                 ckpt_payload_range={} requested_bytes={}",
                case.name,
                ops_per_sec,
                probe.p50_us,
                probe.p95_us,
                probe.p99_us,
                probe.io.segment_load_meta,
                probe.io.segment_get,
                probe.io.checkpoint_index_get,
                probe.io.checkpoint_meta_get,
                probe.io.checkpoint_full_get,
                probe.io.checkpoint_range_get,
                probe.io.checkpoint_payload_range_get,
                probe.io.checkpoint_requested_bytes
            );
            case.counters.reset();
        });
    }

    let mut group = c.benchmark_group("fusio_manifest_point_get_localfs");
    for case in &cases {
        group.throughput(Throughput::Elements(case.queries.len() as u64));
        let manifest = case.manifest.clone();
        let snapshot = case.snapshot.clone();
        let queries = case.queries.clone();
        let expected_hits = case.expected_hits;
        group.bench_function(case.name, |b| {
            b.to_async(&runtime).iter(|| {
                let manifest = manifest.clone();
                let snapshot = snapshot.clone();
                let queries = queries.clone();
                async move {
                    let hits = run_query_batch(manifest.as_ref(), &snapshot, queries.as_ref())
                        .await
                        .expect("benchmark query batch");
                    assert_eq!(hits, expected_hits, "benchmark hit count mismatch");
                }
            });
        });
    }
    group.finish();
}

fn workspace_criterion_output_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|workspace_root| workspace_root.join("target").join("criterion"))
        .unwrap_or_else(|| PathBuf::from("target").join("criterion"))
}

fn criterion_config() -> criterion::Criterion {
    let output_dir = workspace_criterion_output_dir();
    criterion::Criterion::default().output_directory(&output_dir)
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = point_get_local
}
criterion_main!(benches);

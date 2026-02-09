mod common;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    base_key, build_manifest, criterion_cli_quiet, multi_level_key, percentile_us,
    print_bench_header, value_blob, write_base_segments, write_multi_level_segments, BenchConfig,
    BenchManifest, IoCounters, IoSnapshot,
};
use criterion::{criterion_group, criterion_main, Throughput};
use fusio_manifest::{snapshot::Snapshot, types::Result};
use rand::{rngs::StdRng, Rng, SeedableRng};
use tempfile::TempDir;

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
    let cli_quiet = criterion_cli_quiet();
    print_bench_header("point_get_local", &cfg, cli_quiet);

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
            if !cli_quiet {
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
            }
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

criterion_group! {
    name = benches;
    config = common::criterion_config();
    targets = point_get_local
}
criterion_main!(benches);

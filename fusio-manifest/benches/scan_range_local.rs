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
use fusio_manifest::{
    snapshot::{ScanRange, Snapshot},
    types::Result,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use tempfile::TempDir;

#[derive(Clone)]
struct ScanRangeQuery {
    start: Option<String>,
    end: Option<String>,
    expected_rows: usize,
}

struct PreparedScanCase {
    name: &'static str,
    manifest: Arc<BenchManifest>,
    snapshot: Snapshot,
    ranges: Arc<Vec<ScanRangeQuery>>,
    expected_rows: usize,
    counters: IoCounters,
    _tempdir: TempDir,
}

struct ScanProbeStats {
    total: Duration,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    rows: usize,
    io: IoSnapshot,
}

#[derive(Clone, Copy)]
enum ScanCaseKind {
    Hit,
    Miss,
    InRangeMiss,
}

fn build_hit_scan_ranges_for_base(
    key_count: usize,
    range_width: usize,
    query_count: usize,
    seed: u64,
) -> Vec<ScanRangeQuery> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..query_count)
        .map(|_| {
            let start = rng.gen_range(0..key_count);
            let end = (start + range_width).min(key_count);
            ScanRangeQuery {
                start: Some(base_key(start)),
                end: Some(base_key(end)),
                expected_rows: end.saturating_sub(start),
            }
        })
        .collect()
}

fn build_hit_scan_ranges_for_multilevel(
    epoch: usize,
    keys_per_epoch: usize,
    range_width: usize,
    query_count: usize,
    seed: u64,
) -> Vec<ScanRangeQuery> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..query_count)
        .map(|_| {
            let start = rng.gen_range(0..keys_per_epoch);
            let end = (start + range_width).min(keys_per_epoch);
            ScanRangeQuery {
                start: Some(multi_level_key(epoch, start)),
                end: Some(multi_level_key(epoch, end)),
                expected_rows: end.saturating_sub(start),
            }
        })
        .collect()
}

fn build_out_of_range_miss_scan_ranges(query_count: usize) -> Vec<ScanRangeQuery> {
    (0..query_count)
        .map(|idx| {
            let start = format!("zz-miss-{idx:08}");
            let end = format!("{start}~");
            ScanRangeQuery {
                start: Some(start),
                end: Some(end),
                expected_rows: 0,
            }
        })
        .collect()
}

fn build_in_range_miss_scan_ranges_for_base(
    key_count: usize,
    query_count: usize,
    seed: u64,
) -> Vec<ScanRangeQuery> {
    let mut rng = StdRng::seed_from_u64(seed);
    let upper = key_count.saturating_sub(1).max(1);
    (0..query_count)
        .map(|_| {
            let idx = rng.gen_range(0..upper);
            let start = format!("{}~", base_key(idx));
            let end = format!("{start}~");
            ScanRangeQuery {
                start: Some(start),
                end: Some(end),
                expected_rows: 0,
            }
        })
        .collect()
}

fn build_in_range_miss_scan_ranges_for_multilevel(
    epoch: usize,
    keys_per_epoch: usize,
    query_count: usize,
    seed: u64,
) -> Vec<ScanRangeQuery> {
    let mut rng = StdRng::seed_from_u64(seed);
    let upper = keys_per_epoch.saturating_sub(1).max(1);
    (0..query_count)
        .map(|_| {
            let idx = rng.gen_range(0..upper);
            let start = format!("{}~", multi_level_key(epoch, idx));
            let end = format!("{start}~");
            ScanRangeQuery {
                start: Some(start),
                end: Some(end),
                expected_rows: 0,
            }
        })
        .collect()
}

async fn run_scan_range_batch(
    manifest: &BenchManifest,
    snapshot: &Snapshot,
    ranges: &[ScanRangeQuery],
) -> Result<usize> {
    let session = manifest.session_at(snapshot.clone()).await?;
    let mut total_rows = 0usize;
    for range in ranges {
        let rows = session
            .scan_range(ScanRange {
                start: range.start.clone(),
                end: range.end.clone(),
            })
            .await?;
        total_rows += rows.len();
    }
    session.end().await?;
    Ok(total_rows)
}

async fn run_scan_range_probe(
    manifest: &BenchManifest,
    snapshot: &Snapshot,
    ranges: &[ScanRangeQuery],
    counters: &IoCounters,
) -> Result<ScanProbeStats> {
    counters.reset();
    let session = manifest.session_at(snapshot.clone()).await?;
    let mut total_rows = 0usize;
    let mut latencies = Vec::with_capacity(ranges.len());
    let batch_start = Instant::now();
    for range in ranges {
        let start = Instant::now();
        let rows = session
            .scan_range(ScanRange {
                start: range.start.clone(),
                end: range.end.clone(),
            })
            .await?;
        total_rows += rows.len();
        latencies.push(start.elapsed().as_nanos() as u64);
    }
    let total = batch_start.elapsed();
    session.end().await?;
    latencies.sort_unstable();
    Ok(ScanProbeStats {
        total,
        p50_us: percentile_us(&latencies, 0.50),
        p95_us: percentile_us(&latencies, 0.95),
        p99_us: percentile_us(&latencies, 0.99),
        rows: total_rows,
        io: counters.snapshot(),
    })
}

async fn prepare_l1_scan_range_case(
    cfg: &BenchConfig,
    name: &'static str,
    kind: ScanCaseKind,
) -> Result<PreparedScanCase> {
    let tempdir = TempDir::new().expect("create temp dir");
    let root = tempdir.path().join(name);
    let (manifest, counters) = build_manifest(&root, cfg);
    let value = value_blob(cfg.value_bytes);
    write_base_segments(manifest.as_ref(), cfg, &value).await?;
    manifest.compactor().compact_once().await?;
    let snapshot = manifest.snapshot().await?;
    let ranges = match kind {
        ScanCaseKind::Hit => build_hit_scan_ranges_for_base(
            cfg.key_count,
            cfg.scan_range_width,
            cfg.scan_query_count,
            61,
        ),
        ScanCaseKind::Miss => build_out_of_range_miss_scan_ranges(cfg.scan_query_count),
        ScanCaseKind::InRangeMiss => {
            build_in_range_miss_scan_ranges_for_base(cfg.key_count, cfg.scan_query_count, 67)
        }
    };
    let expected_rows = ranges.iter().map(|r| r.expected_rows).sum();
    Ok(PreparedScanCase {
        name,
        manifest,
        snapshot,
        ranges: Arc::new(ranges),
        expected_rows,
        counters,
        _tempdir: tempdir,
    })
}

async fn prepare_multi_level_scan_range_case(
    cfg: &BenchConfig,
    name: &'static str,
    kind: ScanCaseKind,
) -> Result<PreparedScanCase> {
    let tempdir = TempDir::new().expect("create temp dir");
    let root = tempdir.path().join(name);
    let (manifest, counters) = build_manifest(&root, cfg);
    let value = value_blob(cfg.value_bytes);
    write_multi_level_segments(manifest.as_ref(), cfg, &value).await?;
    let snapshot = manifest.snapshot().await?;
    let ranges = match kind {
        ScanCaseKind::Hit => build_hit_scan_ranges_for_multilevel(
            0,
            cfg.multi_level_keys_per_epoch,
            cfg.scan_range_width,
            cfg.scan_query_count,
            71,
        ),
        ScanCaseKind::Miss => build_out_of_range_miss_scan_ranges(cfg.scan_query_count),
        ScanCaseKind::InRangeMiss => build_in_range_miss_scan_ranges_for_multilevel(
            0,
            cfg.multi_level_keys_per_epoch,
            cfg.scan_query_count,
            73,
        ),
    };
    let expected_rows = ranges.iter().map(|r| r.expected_rows).sum();
    Ok(PreparedScanCase {
        name,
        manifest,
        snapshot,
        ranges: Arc::new(ranges),
        expected_rows,
        counters,
        _tempdir: tempdir,
    })
}

fn scan_range_local(c: &mut criterion::Criterion) {
    let cfg = BenchConfig::from_env();
    let cli_quiet = criterion_cli_quiet();
    print_bench_header("scan_range_local", &cfg, cli_quiet);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let cases = runtime.block_on(async {
        let l1_hit = prepare_l1_scan_range_case(&cfg, "L1-ScanRangeHit", ScanCaseKind::Hit).await?;
        let l1_miss =
            prepare_l1_scan_range_case(&cfg, "L1-ScanRangeMiss", ScanCaseKind::Miss).await?;
        let l1_in_range_miss =
            prepare_l1_scan_range_case(&cfg, "L1-ScanRangeInRangeMiss", ScanCaseKind::InRangeMiss)
                .await?;
        let ml_hit =
            prepare_multi_level_scan_range_case(&cfg, "MultiLevel-ScanRangeHit", ScanCaseKind::Hit)
                .await?;
        let ml_miss = prepare_multi_level_scan_range_case(
            &cfg,
            "MultiLevel-ScanRangeMiss",
            ScanCaseKind::Miss,
        )
        .await?;
        let ml_in_range_miss = prepare_multi_level_scan_range_case(
            &cfg,
            "MultiLevel-ScanRangeInRangeMiss",
            ScanCaseKind::InRangeMiss,
        )
        .await?;
        Ok::<_, fusio_manifest::types::Error>(vec![
            l1_hit,
            l1_miss,
            l1_in_range_miss,
            ml_hit,
            ml_miss,
            ml_in_range_miss,
        ])
    });
    let cases = cases.expect("prepare scan-range benchmark cases");

    for case in &cases {
        let prewarm = cfg.prewarm_count.min(case.ranges.len());
        runtime.block_on(async {
            case.counters.reset();
            let warm_rows = run_scan_range_batch(
                case.manifest.as_ref(),
                &case.snapshot,
                &case.ranges[..prewarm],
            )
            .await
            .expect("prewarm scan-range batch");
            if case.expected_rows == 0 {
                assert_eq!(warm_rows, 0, "warmup miss case should stay miss");
            }
            let probe = run_scan_range_probe(
                case.manifest.as_ref(),
                &case.snapshot,
                case.ranges.as_ref(),
                &case.counters,
            )
            .await
            .expect("probe scan-range batch");
            assert_eq!(
                probe.rows, case.expected_rows,
                "probe rows mismatch for {}",
                case.name
            );
            if !cli_quiet {
                let ops_per_sec = case.ranges.len() as f64 / probe.total.as_secs_f64();
                let rows_per_sec = probe.rows as f64 / probe.total.as_secs_f64();
                eprintln!(
                    "[{}] scan probe ops/s={:.2} rows/s={:.2} p50={:.2}us p95={:.2}us p99={:.2}us \
                     | seg_meta={} seg_get={} ckpt_index={} ckpt_meta={} ckpt_full={} \
                     ckpt_range={} ckpt_payload_range={} requested_bytes={}",
                    case.name,
                    ops_per_sec,
                    rows_per_sec,
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

    let mut group = c.benchmark_group("fusio_manifest_scan_range_localfs");
    for case in &cases {
        group.throughput(Throughput::Elements(case.ranges.len() as u64));
        let manifest = case.manifest.clone();
        let snapshot = case.snapshot.clone();
        let ranges = case.ranges.clone();
        let expected_rows = case.expected_rows;
        group.bench_function(case.name, |b| {
            b.to_async(&runtime).iter(|| {
                let manifest = manifest.clone();
                let snapshot = snapshot.clone();
                let ranges = ranges.clone();
                async move {
                    let rows = run_scan_range_batch(manifest.as_ref(), &snapshot, ranges.as_ref())
                        .await
                        .expect("benchmark scan-range batch");
                    assert_eq!(rows, expected_rows, "benchmark scan-range rows mismatch");
                }
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = common::criterion_config();
    targets = scan_range_local
}
criterion_main!(benches);

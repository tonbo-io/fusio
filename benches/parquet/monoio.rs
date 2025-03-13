use common::{read_parquet, write_parquet};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use monoio::RuntimeBuilder;
use tempfile::tempdir;

mod common;

fn multi_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();
    let path = fusio::path::Path::from_filesystem_path(tmp_dir.path())
        .unwrap()
        .child("monoio");

    c.bench_with_input(
        BenchmarkId::new("parquet_monoio_write", path.clone()),
        &path,
        |b, path| {
            b.iter(|| {
                RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .with_entries(32768)
                    .build()
                    .unwrap()
                    .block_on(write_parquet(path.clone()))
            });
        },
    );
}

fn random_read(c: &mut Criterion) {
    let path = fusio::path::Path::from_filesystem_path("../benches/parquet")
        .unwrap()
        .child("data.parquet");

    c.bench_with_input(
        BenchmarkId::new("parquet_monoio_random_read", path.clone()),
        &path,
        |b, path| {
            b.iter(|| {
                RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .with_entries(32768)
                    .build()
                    .unwrap()
                    .block_on(read_parquet(path.clone()))
            });
        },
    );
}

criterion_group!(benches, multi_write, random_read);
criterion_main!(benches);

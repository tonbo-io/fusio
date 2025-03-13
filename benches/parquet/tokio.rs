use common::{read_parquet, write_parquet};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tempfile::tempdir;
use tokio::runtime::Builder;

mod common;

fn multi_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();

    let path = fusio::path::Path::from_filesystem_path(tmp_dir.path())
        .unwrap()
        .child("tokio");

    c.bench_with_input(
        BenchmarkId::new("parquet_tokio_write", path.clone()),
        &path,
        |b, path| {
            b.iter(|| {
                Builder::new_multi_thread()
                    .worker_threads(8)
                    .enable_all()
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
        BenchmarkId::new("parquet_tokio_random_read", path.clone()),
        &path,
        |b, path| {
            b.iter(|| {
                Builder::new_multi_thread()
                    .worker_threads(8)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(read_parquet(path.clone()))
            });
        },
    );
}

criterion_group!(benches, multi_write, random_read);
criterion_main!(benches);

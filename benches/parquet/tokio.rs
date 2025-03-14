use common::{read_parquet, read_raw_parquet, write_parquet, write_raw_tokio_parquet};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tempfile::tempdir;
use tokio::runtime::Builder;

mod common;

fn write(c: &mut Criterion) {
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
                    .worker_threads(1)
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
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(read_parquet(path.clone()))
            });
        },
    );
}

fn write_raw(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();

    let path = tmp_dir.path().join("raw_tokio").to_path_buf();
    c.bench_function("parquet_raw_write", |b| {
        b.iter(|| {
            Builder::new_current_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap()
                .block_on(write_raw_tokio_parquet(path.clone()))
        });
    });
}

fn random_read_raw(c: &mut Criterion) {
    c.bench_function("parquet_raw_random_read", |b| {
        b.iter(|| {
            Builder::new_current_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap()
                .block_on(read_raw_parquet("../benches/parquet/data.parquet"))
        });
    });
}

criterion_group!(benches, write, random_read, write_raw, random_read_raw);
criterion_main!(benches);

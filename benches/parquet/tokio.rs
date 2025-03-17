use common::{
    generate_record_batch, load_data, read_parquet, read_raw_parquet, write_parquet,
    write_raw_tokio_parquet, READ_PARQUET_FILE_PATH,
};
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;

mod common;

fn bench_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();
    let fusio_path = fusio::path::Path::from_filesystem_path(tmp_dir.path())
        .unwrap()
        .child("fusio");
    let tokio_path = tmp_dir.path().join("tokio").to_path_buf();

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();

    let data = generate_record_batch();

    let mut group = c.benchmark_group("write");

    group.bench_with_input("raw tokio write", &data, |b, data| {
        b.to_async(&tokio_runtime)
            .iter(|| async { write_raw_tokio_parquet(&tokio_path, data).await });
    });
    group.bench_with_input("fusio tokio write", &data, |b, data| {
        b.to_async(&tokio_runtime).iter(|| async {
            write_parquet(&fusio_path, data).await;
        });
    });
}

fn bench_read(c: &mut Criterion) {
    let path = std::path::Path::new(READ_PARQUET_FILE_PATH);
    if !path.exists() {
        load_data();
    }
    let fusio_path = fusio::path::Path::from_filesystem_path(READ_PARQUET_FILE_PATH).unwrap();
    let tokio_path = READ_PARQUET_FILE_PATH;

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap();

    let mut group = c.benchmark_group("random read");

    group.bench_with_input("raw tokio read", &tokio_path, |b, path| {
        b.to_async(&tokio_runtime)
            .iter(|| read_raw_parquet(path.into()));
    });
    group.bench_with_input("fusio tokio read", &fusio_path, |b, path| {
        b.to_async(&tokio_runtime)
            .iter(|| read_parquet(path.clone()));
    });
}

criterion_group!(benches, bench_read, bench_write);
criterion_main!(benches);

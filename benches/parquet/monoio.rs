use common::{generate_record_batches, read_parquet, write_parquet, READ_PARQUET_FILE_PATH};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use monoio::RuntimeBuilder;
use tempfile::tempdir;

mod common;

fn bench_monoio_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();
    let path = fusio::path::Path::from_filesystem_path(tmp_dir.path())
        .unwrap()
        .child("monoio");
    let data = generate_record_batches();
    c.bench_with_input("parquet_monoio_write", &data, |b, data| {
        b.iter(|| {
            RuntimeBuilder::<monoio::IoUringDriver>::new()
                .with_entries(32768)
                .build()
                .unwrap()
                .block_on(write_parquet(&path, data))
        });
    });
}

fn bench_monoio_read(c: &mut Criterion) {
    let path = std::path::Path::new(READ_PARQUET_FILE_PATH);
    if !path.exists() {
        load_data();
    }
    let path = fusio::path::Path::from_filesystem_path(READ_PARQUET_FILE_PATH).unwrap();
    c.bench_with_input("parquet_monoio_random_read", &path, |b, path| {
        b.iter(|| {
            RuntimeBuilder::<monoio::IoUringDriver>::new()
                .with_entries(32768)
                .build()
                .unwrap()
                .block_on(read_parquet(path))
        });
    });
}

criterion_group!(benches, bench_monoio_write, bench_monoio_read);
criterion_main!(benches);

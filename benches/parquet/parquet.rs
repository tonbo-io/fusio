use common::{read_raw_parquet, write_raw_tokio_parquet};
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;
use tokio::runtime::Builder;

mod common;

fn multi_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();

    let path = tmp_dir.path().join("raw_tokio").to_path_buf();
    c.bench_function("parquet_raw_write", |b| {
        b.iter(|| {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(write_raw_tokio_parquet(path.clone()))
        });
    });
}

fn random_read(c: &mut Criterion) {
    c.bench_function("parquet_raw_random_read", |b| {
        b.iter(|| {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(read_raw_parquet("../benches/parquet/data.parquet"))
        });
    });
}

criterion_group!(benches, multi_write, random_read);
criterion_main!(benches);

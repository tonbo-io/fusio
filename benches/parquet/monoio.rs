use std::{cell::RefCell, future::Future};

use common::{
    generate_record_batch, load_data, read_parquet, write_parquet, READ_PARQUET_FILE_PATH,
};
use criterion::{
    async_executor::{AsyncExecutor, FuturesExecutor},
    criterion_group, criterion_main, BenchmarkId, Criterion,
};
use monoio::{spawn, RuntimeBuilder};
use tempfile::tempdir;

mod common;

fn bench_monoio_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();
    let path = fusio::path::Path::from_filesystem_path(tmp_dir.path())
        .unwrap()
        .child("monoio");
    let data = generate_record_batch();
    c.bench_with_input(BenchmarkId::new("monoio write", ""), &data, |b, data| {
        let mut runtime = RuntimeBuilder::<monoio::IoUringDriver>::new()
            .build()
            .unwrap();
        b.iter(|| runtime.block_on(write_parquet(&path, data)));
    });
}

fn bench_monoio_read(c: &mut Criterion) {
    let path = std::path::Path::new(READ_PARQUET_FILE_PATH);
    if !path.exists() {
        load_data();
    }
    let path = fusio::path::Path::from_filesystem_path(READ_PARQUET_FILE_PATH).unwrap();
    c.bench_with_input(
        BenchmarkId::new("monoio random read", ""),
        &path,
        |b, path| {
            let runtime = RuntimeBuilder::<monoio::IoUringDriver>::new()
                .build()
                .unwrap();
            b.to_async(MonoioExecutor {
                runtime: RefCell::new(runtime),
            })
            .iter(|| read_parquet(path.clone()));
        },
    );
}

struct MonoioExecutor {
    runtime: RefCell<monoio::Runtime<monoio::IoUringDriver>>,
}

impl AsyncExecutor for MonoioExecutor {
    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.runtime.borrow_mut().block_on(future)
    }
}

criterion_group!(benches, bench_monoio_read, bench_monoio_write);
criterion_main!(benches);

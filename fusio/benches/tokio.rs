use std::mem;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use fusio::{
    fs::{Fs, OpenOptions},
    local::TokioFs,
    path::Path,
    Write,
};
use rand::Rng;
use tempfile::{NamedTempFile, TempDir};
use tokio::{fs::File, io::AsyncWriteExt};

fn write(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("write");

    let mut write_bytes = [0u8; 4096];
    rand::thread_rng().fill(&mut write_bytes);

    let temp_file = NamedTempFile::new().unwrap();
    let path = Path::from_filesystem_path(temp_file.path()).unwrap();

    let fs = TokioFs;

    group.bench_function("write 4k", |b| {
        b.to_async(&runtime).iter_batched_ref(
            || {
                futures_executor::block_on(async {
                    fs.open_options(
                        &path,
                        OpenOptions::default().create(true).write(true).append(true),
                    )
                        .await
                        .unwrap()
                })
            },
            |file| async move {
                let (result, _) = fusio::Write::write_all(file, &write_bytes[..]).await;
                result.unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, write);
criterion_main!(benches);

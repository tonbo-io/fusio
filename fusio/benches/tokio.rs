use std::{cell::RefCell, io::SeekFrom, rc::Rc, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use fusio::{
    disk::TokioFs,
    fs::{Fs, OpenOptions},
    path::Path,
    Write,
};
use rand::Rng;
use tempfile::NamedTempFile;

fn write(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("write");

    let mut write_bytes = [0u8; 4096];
    rand::thread_rng().fill(&mut write_bytes);
    let write_bytes = Arc::new(write_bytes);

    let temp_file = NamedTempFile::new().unwrap();
    let path = Path::from_filesystem_path(temp_file.path()).unwrap();

    let fs = TokioFs;
    let file = Rc::new(RefCell::new(runtime.block_on(async {
        fs.open_options(&path, OpenOptions::default().write(true).append(true))
            .await
            .unwrap()
    })));

    group.bench_function("fusio write 4K", |b| {
        b.to_async(&runtime).iter(|| {
            let bytes = write_bytes.clone();
            let file = file.clone();

            async move {
                let file = &mut *(*file).borrow_mut();
                let (result, _) = fusio::Write::write_all(file, &bytes.as_ref()[..]).await;
                result.unwrap();
            }
        })
    });
    group.bench_function("tokio write 4K", |b| {
        b.to_async(&runtime).iter(|| {
            let bytes = write_bytes.clone();
            let file = file.clone();

            async move {
                tokio::io::AsyncWriteExt::write_all(
                    &mut *(*file).borrow_mut(),
                    &bytes.as_ref()[..],
                )
                .await
                .unwrap();
            }
        })
    });

    group.finish();
}

fn read(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("read");

    let mut write_bytes = [0u8; 4096];
    rand::thread_rng().fill(&mut write_bytes);

    let temp_file = NamedTempFile::new().unwrap();
    let path = Path::from_filesystem_path(temp_file.path()).unwrap();

    let fs = TokioFs;
    let file = Rc::new(RefCell::new(runtime.block_on(async {
        let mut file = fs
            .open_options(&path, OpenOptions::default().write(true).append(true))
            .await
            .unwrap();
        let (result, _) = file.write_all(&write_bytes[..]).await;
        result.unwrap();
        file
    })));

    group.bench_function("fusio read 4K", |b| {
        b.to_async(&runtime).iter(|| {
            let file = file.clone();
            let mut bytes = [0u8; 4096];

            async move {
                fusio::dynamic::DynSeek::seek(&mut *(*file).borrow_mut(), 0)
                    .await
                    .unwrap();
                let (result, _) =
                    fusio::Read::read(&mut *(*file).borrow_mut(), &mut bytes[..]).await;
                result.unwrap();
            }
        })
    });

    group.bench_function("tokio read 4K", |b| {
        b.to_async(&runtime).iter(|| {
            let file = file.clone();
            let mut bytes = [0u8; 4096];

            async move {
                let _ =
                    tokio::io::AsyncSeekExt::seek(&mut *(*file).borrow_mut(), SeekFrom::Start(0))
                        .await
                        .unwrap();
                let _ =
                    tokio::io::AsyncReadExt::read_exact(&mut *(*file).borrow_mut(), &mut bytes[..])
                        .await
                        .unwrap();
            }
        })
    });
}

criterion_group!(benches, write, read);
criterion_main!(benches);

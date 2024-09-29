use std::{cell::RefCell, io::SeekFrom, rc::Rc, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion};
use fusio::{
    fs::{Fs, OpenOptions},
    local::TokioFs,
    IoBuf, IoBufMut, Write,
};
use rand::Rng;
use tempfile::NamedTempFile;
use url::Url;

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
    let url = Url::from_file_path(temp_file.path()).unwrap();

    let fs = TokioFs;
    let file = Rc::new(RefCell::new(runtime.block_on(async {
        fs.open_options(&url, OpenOptions::default().write(true).append(true))
            .await
            .unwrap()
    })));

    group.bench_function("fusio write 4K", |b| {
        b.to_async(&runtime).iter(|| {
            let bytes = write_bytes.clone();
            let file = file.clone();

            async move {
                let (result, _) =
                    fusio::dynamic::DynWrite::write_all(&mut *(*file).borrow_mut(), unsafe {
                        (&bytes.as_ref()[..]).to_buf_nocopy()
                    })
                    .await;
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
    let url = Url::from_file_path(temp_file.path()).unwrap();

    let fs = TokioFs;
    let file = Rc::new(RefCell::new(runtime.block_on(async {
        let mut file = fs
            .open_options(&url, OpenOptions::default().write(true).append(true))
            .await
            .unwrap();
        let (result, _) = file.write_all(&write_bytes[..]).await;
        result.unwrap();
        file
    })));

    group.bench_function("fusio read 4K", |b| {
        b.to_async(&runtime).iter(|| {
            let file = file.clone();

            async move {
                let _ = fusio::dynamic::DynSeek::seek(&mut *(*file).borrow_mut(), 0)
                    .await
                    .unwrap();
                let _ = fusio::dynamic::DynRead::read_exact(&mut *(*file).borrow_mut(), unsafe {
                    vec![0u8; 4096].to_buf_mut_nocopy()
                })
                .await
                .unwrap();
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

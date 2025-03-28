use std::{cell::RefCell, future::Future, rc::Rc, sync::Arc};

use criterion::{async_executor::AsyncExecutor, criterion_group, criterion_main, Criterion};
use fusio::{
    disk::MonoIoFs,
    fs::{Fs, OpenOptions},
    path::Path,
    Write,
};
use monoio::RuntimeBuilder;
use rand::Rng;
use tempfile::NamedTempFile;

struct MonoioExecutor {
    runtime: Rc<RefCell<monoio::Runtime<monoio::IoUringDriver>>>,
}

impl AsyncExecutor for MonoioExecutor {
    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.runtime.borrow_mut().block_on(future)
    }
}

fn write(c: &mut Criterion) {
    let runtime = Rc::new(RefCell::new(
        RuntimeBuilder::<monoio::IoUringDriver>::new()
            .build()
            .unwrap(),
    ));

    let mut group = c.benchmark_group("write");

    let mut write_bytes = [0u8; 4096];
    rand::thread_rng().fill(&mut write_bytes);
    let write_bytes = Arc::new(write_bytes);

    let temp_file = NamedTempFile::new().unwrap();
    let path = Path::from_filesystem_path(temp_file.path()).unwrap();

    let fs = MonoIoFs {};
    let file = Rc::new(RefCell::new(runtime.borrow_mut().block_on(async {
        fs.open_options(&path, OpenOptions::default().write(true))
            .await
            .unwrap()
    })));

    group.bench_function("fusio/monoio write 4K", |b| {
        b.to_async(MonoioExecutor {
            runtime: runtime.clone(),
        })
        .iter(|| {
            let bytes = write_bytes.clone();
            let file = file.clone();

            async move {
                let file = &mut *(*file).borrow_mut();
                let (result, _) = fusio::Write::write_all(file, bytes.to_vec()).await;
                result.unwrap();
            }
        })
    });

    group.finish();
}

fn read(c: &mut Criterion) {
    let runtime = Rc::new(RefCell::new(
        RuntimeBuilder::<monoio::IoUringDriver>::new()
            .build()
            .unwrap(),
    ));

    let mut group = c.benchmark_group("read");

    let mut write_bytes = [0u8; 4096];
    rand::thread_rng().fill(&mut write_bytes);

    let temp_file = NamedTempFile::new().unwrap();
    let path = Path::from_filesystem_path(temp_file.path()).unwrap();

    let fs = MonoIoFs {};
    let file = Rc::new(RefCell::new(runtime.borrow_mut().block_on(async {
        let mut file = fs
            .open_options(&path, OpenOptions::default().write(true))
            .await
            .unwrap();
        for _ in 0..1024 * 1024 {
            let (result, _) = file.write_all(write_bytes.to_vec()).await;
            result.unwrap();
        }
        file
    })));

    group.bench_function("fusio/monoio read 4K", |b| {
        b.to_async(MonoioExecutor {
            runtime: runtime.clone(),
        })
        .iter(|| {
            let file = file.clone();
            let bytes = [0u8; 4096];

            async move {
                let random_pos = rand::thread_rng().gen_range(0..4096 * 1024 * 1024 - 4096);
                let file = file.clone();
                let (result, _) =
                    fusio::Read::read_exact_at(&mut *file.borrow_mut(), bytes.to_vec(), random_pos)
                        .await;
                result.unwrap();
            }
        })
    });
}

criterion_group!(benches, write, read);
criterion_main!(benches);

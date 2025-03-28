mod common;

use common::{generate_record_batch, load_data, random_read, schema, READ_PARQUET_FILE_PATH};
use criterion::{criterion_group, criterion_main, Criterion};
use opendal::Operator;
use parquet::arrow::AsyncArrowWriter;
use parquet_opendal::{AsyncReader, AsyncWriter};
use tempfile::tempdir;

fn bench_read(c: &mut Criterion) {
    let path = std::path::Path::new(READ_PARQUET_FILE_PATH);
    if !path.exists() {
        load_data();
    }

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap();

    let root = path.parent().unwrap().as_os_str().to_str().unwrap();
    let file_name = path.file_name().unwrap().to_str().unwrap();

    c.bench_function("random read/opendal", |b| {
        b.to_async(&tokio_runtime).iter(|| async {
            let builder = opendal::services::Fs::default().root(root);
            let operator = Operator::new(builder).unwrap().finish();
            let reader = operator.reader_with(file_name).await.unwrap();
            let content_len = operator.stat(file_name).await.unwrap().content_length();
            let reader =
                AsyncReader::new(reader, content_len).with_prefetch_footer_size(512 * 1024);
            random_read(reader).await;
        });
    });
}

fn bench_write(c: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap();

    let data = generate_record_batch();
    let opendal_path = tmp_dir.path().to_str().unwrap();

    c.bench_function("sequential write/opendal", |b| {
        b.to_async(&tokio_runtime).iter(|| async {
            let builder = opendal::services::Fs::default().root(opendal_path);
            let operator = Operator::new(builder).unwrap().finish();
            let writer =
                AsyncWriter::new(operator.writer_with("opendal").append(false).await.unwrap());
            let mut writer = AsyncArrowWriter::try_new(writer, schema(), None).unwrap();
            writer.write(&data).await.unwrap();
            writer.close().await.unwrap();
        });
    });
}

criterion_group!(benches, bench_read, bench_write);
criterion_main!(benches);

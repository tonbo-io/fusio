use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use fusio::{fs::OpenOptions, path::Path, Read, Write};
use fusio_dispatch::FsOptions;
use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
use futures::StreamExt;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub async fn write_to_opfs() {
    let fs_options = FsOptions::Local;
    let fs = fs_options.parse().unwrap();
    let mut file = fs
        .open_options(
            &Path::from_opfs_path("foo").unwrap(),
            OpenOptions::default().create(true),
        )
        .await
        .unwrap();

    let write_buf = "hello, fusio".as_bytes();

    let (result, buf) = file.write_all(write_buf).await;
    result.unwrap();

    file.close().await.unwrap();
    web_sys::console::log_1(&format!("write data: {:?}", buf).into());
}

#[wasm_bindgen]
pub async fn read_from_opfs() {
    let fs_options = FsOptions::Local;
    let fs = fs_options.parse().unwrap();
    let mut file = fs
        .open(&Path::from_opfs_path("foo").unwrap())
        .await
        .unwrap();

    let (result, read_buf) = file.read_to_end_at(vec![], 0).await;
    result.unwrap();
    assert_eq!(read_buf.as_slice(), b"hello, fusio");
    web_sys::console::log_1(&format!("read data: {:?}", read_buf).into());
}

#[wasm_bindgen]
pub async fn async_writer() {
    let fs_options = FsOptions::Local;
    let fs = fs_options.parse().unwrap();
    let file = fs
        .open_options(
            &Path::from_opfs_path("bar").unwrap(),
            OpenOptions::default().create(true),
        )
        .await
        .unwrap();
    let writer = AsyncWriter::new(file);
    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
    let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
    writer.write(&to_write).await.unwrap();
    writer.close().await.unwrap();
    web_sys::console::log_1(&format!("write data: {:?} to parquet", to_write).into());
}

#[wasm_bindgen]
pub async fn async_reader() {
    let fs_options = FsOptions::Local;
    let fs = fs_options.parse().unwrap();
    let file = fs
        .open(&Path::from_opfs_path("bar").unwrap())
        .await
        .unwrap();

    let size = file.size().await.unwrap();
    let reader = AsyncReader::new(file, size).await.unwrap();
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .build()
        .unwrap();

    let read = stream.next().await.unwrap().unwrap();

    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let expected = RecordBatch::try_from_iter([("col", col)]).unwrap();
    assert_eq!(expected, read);
    web_sys::console::log_1(&format!("read data: {:?} from parquet", read).into());
}

#[wasm_bindgen]
pub async fn remove_all_dir() {
    let fs_options = FsOptions::Local;
    let fs = fs_options.parse().unwrap();
    fs.remove(&Path::from_opfs_path("foo").unwrap())
        .await
        .unwrap();
    fs.remove(&Path::from_opfs_path("bar").unwrap())
        .await
        .unwrap();
}

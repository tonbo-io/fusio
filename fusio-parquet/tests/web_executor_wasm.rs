#![cfg(target_arch = "wasm32")]
#![cfg(feature = "executor-web")]

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array},
    record_batch::RecordBatch,
};
use fusio::{fs::OpenOptions, impls::mem::fs::InMemoryFs, path::Path, Fs, Read, WebExecutor};
use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
use futures::StreamExt;
use parquet::{
    arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
    file::properties::WriterProperties,
};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test(async)]
async fn parquet_roundtrip_with_web_executor() {
    let fs = InMemoryFs::new();
    let path = Path::parse("/parquet/web/roundtrip").unwrap();
    let options = OpenOptions::default().create(true).write(true);

    let executor = WebExecutor;
    let writer = AsyncWriter::new(
        Box::new(fs.open_options(&path, options).await.unwrap()),
        executor,
    );

    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3, 4])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
    let props = WriterProperties::builder().build();
    let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), Some(props)).unwrap();
    writer.write(&to_write).await.unwrap();
    writer.close().await.unwrap();

    let file = fs
        .open_options(&path, OpenOptions::default())
        .await
        .unwrap();
    let size = file.size().await.unwrap();

    let reader = AsyncReader::new(Box::new(file), size, executor)
        .await
        .unwrap();
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .unwrap()
        .build()
        .unwrap();

    let read = stream.next().await.unwrap().unwrap();
    assert_eq!(to_write, read);
}

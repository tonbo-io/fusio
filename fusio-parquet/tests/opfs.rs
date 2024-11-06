#[cfg(test)]
#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub(crate) mod tests {

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use bytes::Bytes;
    use fusio::{
        disk::LocalFs,
        fs::{Fs, OpenOptions},
        path::Path,
        Read,
    };
    use fusio_parquet::writer::AsyncWriter;
    use parquet::arrow::{
        arrow_reader::ParquetRecordBatchReaderBuilder, async_writer::AsyncFileWriter,
        AsyncArrowWriter,
    };
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    async fn test_opfs_basic_write() {
        let fs = LocalFs {};
        let path = Path::from_opfs_path("basic_write_file").unwrap();
        let options = OpenOptions::default().create(true).write(true);
        let mut writer = AsyncWriter::new(Box::new(fs.open_options(&path, options).await.unwrap()));

        let bytes = Bytes::from_static(b"Hello ");
        writer.write(bytes).await.unwrap();
        let bytes = Bytes::from_static(b"world!");
        writer.write(bytes).await.unwrap();
        writer.complete().await.unwrap();

        let buf = Vec::new();
        let mut file = fs
            .open_options(&path, OpenOptions::default())
            .await
            .unwrap();

        let (result, buf) = file.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf.as_slice(), b"Hello world!");

        fs.remove(&path).await.unwrap();
    }

    #[wasm_bindgen_test]
    async fn test_opfs_async_writer() {
        let fs = LocalFs {};
        let path = Path::from_opfs_path("async_writer_file").unwrap();

        let options = OpenOptions::default().create(true).write(true);
        let writer = AsyncWriter::new(Box::new(fs.open_options(&path, options).await.unwrap()));

        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
        let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let buf = Vec::new();
        let mut file = fs
            .open_options(&path, OpenOptions::default())
            .await
            .unwrap();
        let (result, buf) = file.read_to_end_at(buf, 0).await;
        result.unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf))
            .unwrap()
            .build()
            .unwrap();
        let read = reader.next().unwrap().unwrap();
        assert_eq!(to_write, read);

        fs.remove(&path).await.unwrap();
    }
}

#![allow(dead_code)]

use std::{ops::Range, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use fusio::{disk::LocalFs, fs::OpenOptions, path::Path, DynFs};
use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderBuilder, ArrowReaderOptions, RowSelection},
        async_reader::AsyncFileReader,
        AsyncArrowWriter,
    },
    file::properties::{EnabledStatistics, WriterProperties},
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};

const RECORD_PER_BATCH: usize = 1024;

pub(crate) const READ_PARQUET_FILE_PATH: &str = "../benches/parquet/data.parquet";

pub(crate) async fn write_parquet(path: &Path, data: &RecordBatch) {
    let fs = LocalFs {};
    let options = OpenOptions::default()
        .create(true)
        .truncate(true)
        .write(true);

    let writer = AsyncWriter::new(Box::new(fs.open_options(path, options).await.unwrap()));

    let mut writer = AsyncArrowWriter::try_new(writer, schema(), None).unwrap();
    writer.write(data).await.unwrap();
    writer.close().await.unwrap();
}

pub(crate) async fn write_raw_tokio_parquet(path: impl AsRef<std::path::Path>, data: &RecordBatch) {
    let file = tokio::fs::OpenOptions::default()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .await
        .unwrap();
    let mut writer = AsyncArrowWriter::try_new(file, schema(), None).unwrap();
    writer.write(data).await.unwrap();
    writer.close().await.unwrap();
}

pub async fn read_parquet(path: Path) {
    let fs = LocalFs {};
    let options = OpenOptions::default().create(true).write(true);

    let file = fs.open_options(&path, options).await.unwrap();
    let size = file.size().await.unwrap();

    let reader = AsyncReader::new(Box::new(file), size).await.unwrap();
    random_read(reader).await;
}

pub(crate) async fn read_raw_parquet(path: std::path::PathBuf) {
    let file = tokio::fs::File::open(path).await.unwrap();
    random_read(file).await;
}

pub(crate) async fn random_read<T>(reader: T)
where
    T: AsyncFileReader + Unpin + Send + 'static,
{
    let builder = ArrowReaderBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new().with_page_index(true),
    )
    .await
    .unwrap();
    let metadata = builder.metadata();
    let num_row_groups = metadata.num_row_groups();
    let selected_row_group = generate_num(0..num_row_groups);
    let row_group = metadata.row_group(selected_row_group);
    let num_rows = row_group.num_rows() as usize;

    let left = generate_num(0..num_rows - 512);

    let mut reader = builder
        .with_row_groups(vec![selected_row_group])
        .with_row_selection(RowSelection::from_consecutive_ranges(
            [left..left + 512].into_iter(),
            num_rows,
        ))
        .build()
        .unwrap();

    while let Some(group) = reader.next_row_group().await.unwrap() {
        for batch in group {
            batch.unwrap();
        }
    }
}

pub(crate) fn schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::UInt8, false),
    ]))
}

pub(crate) fn generate_record_batch() -> RecordBatch {
    let mut rng = thread_rng();
    let mut ids = Vec::with_capacity(RECORD_PER_BATCH);
    let mut names = Vec::with_capacity(RECORD_PER_BATCH);
    let mut ages = Vec::with_capacity(RECORD_PER_BATCH);
    for _ in 0..RECORD_PER_BATCH {
        ids.push(rng.gen::<u64>());
        ages.push(rng.gen::<u8>());
        let len: usize = rng.gen_range(0..=100);
        names.push(
            thread_rng()
                .sample_iter(&Alphanumeric)
                .take(len)
                .map(char::from)
                .collect::<String>(),
        );
    }

    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(UInt64Array::from(ids)) as ArrayRef),
        ("name", Arc::new(StringArray::from(names)) as ArrayRef),
        ("age", Arc::new(UInt8Array::from(ages)) as ArrayRef),
    ])
    .unwrap()
}

fn generate_num(range: Range<usize>) -> usize {
    let mut rng = thread_rng();
    rng.gen_range(range)
}

pub(crate) fn load_data() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let file = tokio::fs::OpenOptions::default()
                .create(true)
                .write(true)
                .open(READ_PARQUET_FILE_PATH)
                .await
                .unwrap();
            let prop = WriterProperties::builder()
                // .set_data_page_size_limit(8192)
                // .set_dictionary_page_size_limit(8192)
                .set_statistics_enabled(EnabledStatistics::Chunk)
                .build();
            let mut writer = AsyncArrowWriter::try_new(file, schema(), Some(prop)).unwrap();
            for _ in 0..1024 * 16 {
                writer.write(&generate_record_batch()).await.unwrap();
            }
            writer.close().await.unwrap();
        });
}

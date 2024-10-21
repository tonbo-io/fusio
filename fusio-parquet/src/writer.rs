use bytes::Bytes;
use fusio::{dynamic::DynFile, Write};
use futures::future::BoxFuture;
use parquet::{arrow::async_writer::AsyncFileWriter, errors::ParquetError};

pub struct AsyncWriter {
    inner: Option<Box<dyn DynFile>>,
}

impl AsyncWriter {
    pub fn new(writer: Box<dyn DynFile>) -> Self {
        Self {
            inner: Some(writer),
        }
    }
}

impl AsyncFileWriter for AsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            if let Some(writer) = self.inner.as_mut() {
                let (result, _) = writer.write_all(bs).await;
                result.map_err(|err| ParquetError::External(Box::new(err)))?;
            }
            Ok(())
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            if let Some(mut writer) = self.inner.take() {
                writer
                    .complete()
                    .await
                    .map_err(|err| ParquetError::External(Box::new(err)))?;
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Seek, SeekFrom},
        sync::Arc,
    };

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use bytes::Bytes;
    use parquet::arrow::{
        arrow_reader::ParquetRecordBatchReaderBuilder, async_writer::AsyncFileWriter,
        AsyncArrowWriter,
    };
    use tempfile::tempfile;
    use tokio::{
        fs::File,
        io::{AsyncReadExt, AsyncSeekExt},
    };

    use crate::writer::AsyncWriter;

    #[tokio::test]
    async fn test_basic() {
        let temp_file = tempfile().unwrap();
        let temp_file_clone = temp_file.try_clone().unwrap();

        let mut writer = AsyncWriter::new(Box::new(File::from_std(temp_file)));

        let bytes = Bytes::from_static(b"hello, world!");
        writer.write(bytes).await.unwrap();
        let bytes = Bytes::from_static(b"hello, Fusio!");
        writer.write(bytes).await.unwrap();
        writer.complete().await.unwrap();

        let mut buf = Vec::new();
        let mut file = File::from_std(temp_file_clone);

        file.seek(SeekFrom::Start(0)).await.unwrap();
        let _ = file.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.as_slice(), b"hello, world!hello, Fusio!");
    }

    #[tokio::test]
    async fn test_async_writer() {
        let temp_file = tempfile().unwrap();
        let mut temp_file_clone = temp_file.try_clone().unwrap();

        let writer = AsyncWriter::new(Box::new(File::from_std(temp_file)));

        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
        let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        temp_file_clone.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = Vec::new();
        let _ = temp_file_clone.read_to_end(&mut buf);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf))
            .unwrap()
            .build()
            .unwrap();
        let read = reader.next().unwrap().unwrap();
        assert_eq!(to_write, read);
    }
}

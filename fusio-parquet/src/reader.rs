use std::{cmp, ops::Range, sync::Arc};

use bytes::{Bytes, BytesMut};
use fusio::{dynamic::DynFile, Read};
use futures::{future::BoxFuture, FutureExt};
use parquet::{
    arrow::async_reader::AsyncFileReader,
    errors::ParquetError,
    file::{
        footer::{decode_footer, decode_metadata},
        metadata::ParquetMetaData,
        FOOTER_SIZE,
    },
};

const PREFETCH_FOOTER_SIZE: usize = 512 * 1024;

pub struct AsyncReader {
    inner: Box<dyn DynFile>,
    content_length: u64,
    // The prefetch size for fetching file footer.
    prefetch_footer_size: usize,
}

fn set_prefetch_footer_size(footer_size: usize, content_size: u64) -> usize {
    let footer_size = cmp::max(footer_size, FOOTER_SIZE);
    cmp::min(footer_size as u64, content_size) as usize
}

impl AsyncReader {
    pub fn new(reader: Box<dyn DynFile>, content_length: u64) -> Self {
        Self {
            inner: reader,
            content_length,
            prefetch_footer_size: set_prefetch_footer_size(PREFETCH_FOOTER_SIZE, content_length),
        }
    }

    pub fn with_prefetch_footer_size(mut self, footer_size: usize) -> Self {
        self.prefetch_footer_size = set_prefetch_footer_size(footer_size, self.content_length);
        self
    }
}

impl AsyncFileReader for AsyncReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let len = range.end - range.start;
            let mut buf = BytesMut::with_capacity(len);
            buf.resize(len, 0);

            self.inner
                .seek(range.start as u64)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?;
            let buf = self
                .inner
                .read_exact(buf)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?;
            Ok(buf.freeze())
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let footer_size = self.prefetch_footer_size;
            let mut buf = BytesMut::with_capacity(footer_size);
            buf.resize(footer_size, 0);

            self.inner
                .seek(self.content_length - footer_size as u64)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?;
            let prefetched_footer_content = self
                .inner
                .read_exact(buf)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))?;
            let prefetched_footer_slice = prefetched_footer_content.as_ref();
            let prefetched_footer_length = prefetched_footer_slice.len();

            // Decode the metadata length from the last 8 bytes of the file.
            let metadata_length = {
                let buf = &prefetched_footer_slice
                    [(prefetched_footer_length - FOOTER_SIZE)..prefetched_footer_length];
                debug_assert!(buf.len() == FOOTER_SIZE);
                decode_footer(buf.try_into().unwrap())?
            };

            // Try to read the metadata from the `prefetched_footer_content`.
            // Otherwise, fetch exact metadata from the remote.
            if prefetched_footer_length >= metadata_length + FOOTER_SIZE {
                let buf = &prefetched_footer_slice[(prefetched_footer_length
                    - metadata_length
                    - FOOTER_SIZE)
                    ..(prefetched_footer_length - FOOTER_SIZE)];
                Ok(Arc::new(decode_metadata(buf)?))
            } else {
                self.inner
                    .seek(self.content_length - metadata_length as u64 - FOOTER_SIZE as u64)
                    .await
                    .map_err(|err| ParquetError::External(Box::new(err)))?;

                let mut buf = BytesMut::with_capacity(metadata_length);
                buf.resize(metadata_length, 0);

                let bytes = self
                    .inner
                    .read_exact(buf)
                    .await
                    .map_err(|err| ParquetError::External(Box::new(err)))?;
                Ok(Arc::new(decode_metadata(&bytes)?))
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Seek, SeekFrom},
        sync::Arc,
    };

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use futures::StreamExt;
    use parquet::{
        arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
        file::properties::WriterProperties,
        format::KeyValue,
    };
    use rand::{distributions::Alphanumeric, Rng};
    use tempfile::tempfile;
    use tokio::fs::File;

    use crate::{
        reader::{AsyncReader, PREFETCH_FOOTER_SIZE},
        writer::AsyncWriter,
    };

    #[tokio::test]
    async fn test_async_reader_with_prefetch_footer_size() {
        let reader = AsyncReader::new(Box::new(File::from_std(tempfile().unwrap())), 1024);
        assert_eq!(reader.prefetch_footer_size, 1024);
        assert_eq!(reader.content_length, 1024);

        let reader = AsyncReader::new(Box::new(File::from_std(tempfile().unwrap())), 1024 * 1024);
        assert_eq!(reader.prefetch_footer_size, PREFETCH_FOOTER_SIZE);
        assert_eq!(reader.content_length, 1024 * 1024);

        let reader = AsyncReader::new(Box::new(File::from_std(tempfile().unwrap())), 1024 * 1024)
            .with_prefetch_footer_size(2048 * 1024);
        assert_eq!(reader.prefetch_footer_size, 1024 * 1024);
        assert_eq!(reader.content_length, 1024 * 1024);

        let reader = AsyncReader::new(Box::new(File::from_std(tempfile().unwrap())), 1024 * 1024)
            .with_prefetch_footer_size(1);
        assert_eq!(reader.prefetch_footer_size, 8);
        assert_eq!(reader.content_length, 1024 * 1024);
    }

    struct TestCase {
        metadata_size: usize,
        prefetch: Option<usize>,
    }

    fn gen_fixed_string(size: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .map(char::from)
            .collect()
    }

    #[tokio::test]
    async fn test_async_reader_with_large_metadata() {
        for case in [
            TestCase {
                metadata_size: 256 * 1024,
                prefetch: None,
            },
            TestCase {
                metadata_size: 1024 * 1024,
                prefetch: None,
            },
            TestCase {
                metadata_size: 256 * 1024,
                prefetch: Some(4),
            },
            TestCase {
                metadata_size: 1024 * 1024,
                prefetch: Some(4),
            },
        ] {
            let temp_file = tempfile().unwrap();
            let mut temp_file_clone = temp_file.try_clone().unwrap();

            let writer = AsyncWriter::new(Box::new(File::from_std(temp_file)));

            let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
            let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

            let mut writer = AsyncArrowWriter::try_new(
                writer,
                to_write.schema(),
                Some(
                    WriterProperties::builder()
                        .set_key_value_metadata(Some(vec![KeyValue {
                            key: "__metadata".to_string(),
                            value: Some(gen_fixed_string(case.metadata_size)),
                        }]))
                        .build(),
                ),
            )
            .unwrap();
            writer.write(&to_write).await.unwrap();
            writer.close().await.unwrap();

            temp_file_clone.seek(SeekFrom::Start(0)).unwrap();
            let metadata = temp_file_clone.metadata().unwrap();
            let content_len = metadata.len();
            let mut reader =
                AsyncReader::new(Box::new(File::from_std(temp_file_clone)), content_len);
            if let Some(footer_size) = case.prefetch {
                reader = reader.with_prefetch_footer_size(footer_size);
            }
            let mut stream = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .unwrap()
                .build()
                .unwrap();
            let read = stream.next().await.unwrap().unwrap();
            assert_eq!(to_write, read);
        }
    }
}

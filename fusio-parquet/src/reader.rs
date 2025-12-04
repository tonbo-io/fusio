use std::{cmp, ops::Range, sync::Arc};

use bytes::{Bytes, BytesMut};
use fusio::{
    dynamic::DynFile,
    executor::{Executor, Mutex},
    Read,
};
use futures::{channel::oneshot, future::BoxFuture, FutureExt};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, MetadataFetch},
    },
    errors::ParquetError,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        FOOTER_SIZE,
    },
};

const PREFETCH_FOOTER_SIZE: usize = 512 * 1024;

/// Async parquet file reader that works across different async runtimes.
///
/// Requires an executor to spawn I/O tasks. The executor handles runtime-specific
/// task spawning, making this reader work uniformly across tokio, monoio,
/// tokio-uring, and web/WASM environments.
pub struct AsyncReader<E: Executor> {
    #[allow(clippy::arc_with_non_send_sync)]
    inner: Arc<E::Mutex<Box<dyn DynFile>>>,
    content_length: u64,
    prefetch_footer_size: usize,
    executor: E,
}

unsafe impl<E: Executor> Send for AsyncReader<E> {}

fn set_prefetch_footer_size(footer_size: usize, content_size: u64) -> usize {
    let footer_size = cmp::max(footer_size, FOOTER_SIZE);
    cmp::min(footer_size as u64, content_size) as usize
}

impl<E: Executor> AsyncReader<E> {
    /// Create a new async reader with the specified executor.
    #[allow(clippy::arc_with_non_send_sync)]
    pub async fn new(
        reader: Box<dyn DynFile>,
        content_length: u64,
        executor: E,
    ) -> Result<Self, fusio::error::Error> {
        Ok(Self {
            inner: Arc::new(E::mutex(reader)),
            content_length,
            prefetch_footer_size: set_prefetch_footer_size(PREFETCH_FOOTER_SIZE, content_length),
            executor,
        })
    }

    pub fn with_prefetch_footer_size(mut self, footer_size: usize) -> Self {
        self.prefetch_footer_size = set_prefetch_footer_size(footer_size, self.content_length);
        self
    }

    async fn load_metadata<F: Read>(
        content_length: u64,
        prefetch_footer_size: usize,
        file: &mut F,
    ) -> Result<ParquetMetaData, ParquetError> {
        let buf = vec![0; prefetch_footer_size];

        let (result, prefetched_footer_content) = file
            .read_exact_at(buf, content_length - prefetch_footer_size as u64)
            .await;
        result.map_err(|err| ParquetError::External(Box::new(err)))?;

        let prefetched_footer_slice = &prefetched_footer_content[..];
        let prefetched_footer_length = prefetched_footer_slice.len();

        let metadata_length = {
            let buf = &prefetched_footer_slice
                [(prefetched_footer_length - FOOTER_SIZE)..prefetched_footer_length];
            debug_assert!(buf.len() == FOOTER_SIZE);
            ParquetMetaDataReader::decode_footer_tail(
                buf.try_into()
                    .expect("it must convert footer buffer to fixed-size array"),
            )?
            .metadata_length()
        };

        if prefetched_footer_length >= metadata_length + FOOTER_SIZE {
            let buf =
                &prefetched_footer_slice[(prefetched_footer_length - metadata_length - FOOTER_SIZE)
                    ..(prefetched_footer_length - FOOTER_SIZE)];

            ParquetMetaDataReader::decode_metadata(buf)
        } else {
            let mut buf = BytesMut::with_capacity(metadata_length);
            buf.resize(metadata_length, 0);

            let (result, bytes) = file
                .read_exact_at(
                    buf,
                    content_length - metadata_length as u64 - FOOTER_SIZE as u64,
                )
                .await;
            result.map_err(|err| ParquetError::External(Box::new(err)))?;

            ParquetMetaDataReader::decode_metadata(&bytes)
        }
    }

    async fn load_page_indexes<F: MetadataFetch>(
        metadata: ParquetMetaData,
        fetch: F,
    ) -> Result<ParquetMetaData, ParquetError> {
        let mut reader = ParquetMetaDataReader::new_with_metadata(metadata)
            .with_page_index_policy(PageIndexPolicy::Required);
        reader.load_page_index(fetch).await?;
        reader.finish()
    }

    async fn load_bytes<F: Read + ?Sized>(
        file: &mut F,
        range: Range<u64>,
    ) -> Result<Bytes, ParquetError> {
        let len = (range.end - range.start) as usize;
        let buf = vec![0; len];

        let (result, buf) = file.read_exact_at(buf, range.start).await;
        result.map_err(|err| ParquetError::External(Box::new(err)))?;

        Ok(buf.into())
    }
}

impl<E: Executor + Clone + 'static> AsyncFileReader for AsyncReader<E> {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let reader = self.inner.clone();
        let (tx, rx) = oneshot::channel();

        self.executor.spawn(async move {
            let mut guard = reader.lock().await;
            let result = Self::load_bytes(&mut *guard, range).await;
            let _ = tx.send(result);
        });

        async move {
            rx.await
                .map_err(|_| ParquetError::General("task canceled".to_string()))?
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        if self.content_length == 0 {
            return async { Err(ParquetError::EOF("file empty".to_string())) }.boxed();
        }

        let reader = self.inner.clone();
        let content_length = self.content_length;
        let prefetch_footer_size = self.prefetch_footer_size;
        let (tx, rx) = oneshot::channel();

        self.executor.spawn(async move {
            let mut guard = reader.lock().await;
            let result =
                Self::load_metadata(content_length, prefetch_footer_size, &mut *guard).await;
            let _ = tx.send(result);
        });

        let page_index = options.map(|options| options.page_index()).unwrap_or(false);
        async move {
            let metadata = rx
                .await
                .map_err(|_| ParquetError::General("task canceled".to_string()))??;
            if page_index {
                Self::load_page_indexes(metadata, self)
                    .await
                    .map(Arc::from)
                    .map_err(|err| ParquetError::External(Box::new(err)))
            } else {
                Ok(Arc::new(metadata))
            }
        }
        .boxed()
    }
}

#[cfg(test)]
#[cfg(any(feature = "monoio", feature = "tokio"))]
mod tests {
    use std::{marker::Unpin, sync::Arc};

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use fusio::{disk::LocalFs, executor::Executor, fs::OpenOptions, path::Path, DynFs};
    use futures::StreamExt;
    use parquet::{
        arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder},
        file::properties::WriterProperties,
        format::KeyValue,
    };
    use rand::{distributions::Alphanumeric, Rng};
    use tempfile::tempdir;

    use crate::{
        reader::{AsyncReader, PREFETCH_FOOTER_SIZE},
        writer::AsyncWriter,
    };

    async fn async_reader_with_prefetch_footer_size<E: Executor + Clone + Copy + 'static>(
        executor: E,
    ) {
        let tmp_dir = tempdir().unwrap();
        let fs = LocalFs {};
        let path = Path::from_filesystem_path(tmp_dir.path())
            .unwrap()
            .child("reader");
        {
            let file = fs
                .open_options(&path, OpenOptions::default().create(true))
                .await
                .unwrap();

            let reader = AsyncReader::new(Box::new(file), 1024, executor)
                .await
                .unwrap();
            assert_eq!(reader.prefetch_footer_size, 1024);
            assert_eq!(reader.content_length, 1024);
        }

        {
            let file = fs
                .open_options(&path, OpenOptions::default())
                .await
                .unwrap();

            let reader = AsyncReader::new(Box::new(file), 1024 * 1024, executor)
                .await
                .unwrap();
            assert_eq!(reader.prefetch_footer_size, PREFETCH_FOOTER_SIZE);
            assert_eq!(reader.content_length, 1024 * 1024);
        }

        {
            let file = fs
                .open_options(&path, OpenOptions::default())
                .await
                .unwrap();

            let reader = AsyncReader::new(Box::new(file), 1024 * 1024, executor)
                .await
                .unwrap()
                .with_prefetch_footer_size(2048 * 1024);
            assert_eq!(reader.prefetch_footer_size, 1024 * 1024);
            assert_eq!(reader.content_length, 1024 * 1024);
        }

        {
            let file = fs
                .open_options(&path, OpenOptions::default())
                .await
                .unwrap();

            let reader = AsyncReader::new(Box::new(file), 1024 * 1024, executor)
                .await
                .unwrap()
                .with_prefetch_footer_size(1);
            assert_eq!(reader.prefetch_footer_size, 8);
            assert_eq!(reader.content_length, 1024 * 1024);
        }
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

    async fn async_reader_with_large_metadata<E: Executor + Clone + Copy + Unpin + 'static>(
        executor: E,
    ) {
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
            let tmp_dir = tempdir().unwrap();
            let fs = LocalFs {};
            let path = Path::from_filesystem_path(tmp_dir.path())
                .unwrap()
                .child("reader");
            let options = OpenOptions::default().create(true).write(true);

            let writer = AsyncWriter::new(
                Box::new(fs.open_options(&path, options).await.unwrap()),
                executor,
            );

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

            let file = fs
                .open_options(&path, OpenOptions::default())
                .await
                .unwrap();
            let size = file.size().await.unwrap();

            let mut reader = AsyncReader::new(Box::new(file), size, executor)
                .await
                .unwrap();
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

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_async_reader_with_prefetch_footer_size() {
        use fusio::MonoioExecutor;
        async_reader_with_prefetch_footer_size(MonoioExecutor).await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tokio_async_reader_with_prefetch_footer_size() {
        use fusio::executor::NoopExecutor;
        async_reader_with_prefetch_footer_size(NoopExecutor).await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_async_reader_with_large_metadata() {
        use fusio::MonoioExecutor;
        async_reader_with_large_metadata(MonoioExecutor).await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tokio_async_reader_with_large_metadata() {
        use fusio::executor::NoopExecutor;
        async_reader_with_large_metadata(NoopExecutor).await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_async_reader_metadata_with_page_index() {
        use fusio::executor::NoopExecutor;
        use parquet::{
            arrow::arrow_reader::ArrowReaderOptions, file::properties::EnabledStatistics,
        };

        let executor = NoopExecutor;
        let tmp_dir = tempdir().unwrap();
        let fs = LocalFs {};
        let path = Path::from_filesystem_path(tmp_dir.path())
            .unwrap()
            .child("reader");
        let options = OpenOptions::default().create(true).write(true);

        let writer = AsyncWriter::new(
            Box::new(fs.open_options(&path, options).await.unwrap()),
            executor,
        );

        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
        let mut writer = AsyncArrowWriter::try_new(
            writer,
            to_write.schema(),
            Some(
                WriterProperties::builder()
                    .set_key_value_metadata(Some(vec![KeyValue {
                        key: "__metadata".to_string(),
                        value: Some(gen_fixed_string(20)),
                    }]))
                    .set_statistics_enabled(EnabledStatistics::Page)
                    .build(),
            ),
        )
        .unwrap();

        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        {
            let file = fs
                .open_options(&path, OpenOptions::default())
                .await
                .unwrap();
            let size = file.size().await.unwrap();
            let reader = AsyncReader::new(Box::new(file), size, executor)
                .await
                .unwrap();

            let builder = ParquetRecordBatchStreamBuilder::new_with_options(
                reader,
                ArrowReaderOptions::default().with_page_index(true),
            )
            .await
            .unwrap();
            let metadata = builder.metadata();
            assert!(metadata.offset_index().is_some());
            assert!(metadata.column_index().is_some());
        }
    }
}

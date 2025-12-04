use std::sync::Arc;

use bytes::Bytes;
use fusio::{
    dynamic::DynFile,
    executor::{Executor, Mutex},
    Write,
};
use futures::{channel::oneshot, future::BoxFuture, FutureExt};
use parquet::{arrow::async_writer::AsyncFileWriter, errors::ParquetError};

/// Async parquet file writer that works across different async runtimes.
///
/// Requires an executor to spawn I/O tasks. The executor handles runtime-specific
/// task spawning, making this writer work uniformly across tokio, monoio,
/// tokio-uring, and web/WASM environments.
pub struct AsyncWriter<E: Executor> {
    #[allow(clippy::arc_with_non_send_sync)]
    inner: Option<Arc<E::Mutex<Box<dyn DynFile>>>>,
    executor: E,
}

unsafe impl<E: Executor> Send for AsyncWriter<E> {}

impl<E: Executor> AsyncWriter<E> {
    /// Create a new async writer with the specified executor.
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(writer: Box<dyn DynFile>, executor: E) -> Self {
        Self {
            inner: Some(Arc::new(E::mutex(writer))),
            executor,
        }
    }
}

impl<E: Executor + Clone + 'static> AsyncFileWriter for AsyncWriter<E> {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        match self.inner.as_mut() {
            Some(writer) => {
                let writer = writer.clone();
                let (tx, rx) = oneshot::channel();

                self.executor.spawn(async move {
                    let mut guard = writer.lock().await;
                    let (result, _) = guard.write_all(bs).await;
                    let _ = tx.send(result.map_err(|err| ParquetError::External(Box::new(err))));
                });

                async move {
                    rx.await
                        .map_err(|_| ParquetError::General("task canceled".to_string()))??;
                    Ok(())
                }
                .boxed()
            }
            None => async move { Ok(()) }.boxed(),
        }
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        match self.inner.take() {
            Some(writer) => {
                let (tx, rx) = oneshot::channel();

                self.executor.spawn(async move {
                    let mut guard = writer.lock().await;
                    let result = guard
                        .close()
                        .await
                        .map_err(|err| ParquetError::External(Box::new(err)));
                    let _ = tx.send(result);
                });

                async move {
                    rx.await
                        .map_err(|_| ParquetError::General("task canceled".to_string()))?
                }
                .boxed()
            }
            None => async move { Ok(()) }.boxed(),
        }
    }
}

#[cfg(test)]
#[cfg(any(feature = "monoio", feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use bytes::Bytes;
    use fusio::{
        disk::LocalFs,
        executor::Executor,
        fs::{Fs, OpenOptions},
        path::Path,
        DynRead,
    };
    use parquet::arrow::{
        arrow_reader::ParquetRecordBatchReaderBuilder, async_writer::AsyncFileWriter,
        AsyncArrowWriter,
    };
    use tempfile::tempdir;

    use crate::writer::AsyncWriter;

    async fn basic_write<E: Executor + Clone + Copy + 'static>(executor: E) {
        let tmp_dir = tempdir().unwrap();
        let fs = LocalFs {};
        let file_path = Path::from_filesystem_path(tmp_dir.path())
            .unwrap()
            .child("basic");
        let options = OpenOptions::default().create(true).write(true);

        let mut writer = AsyncWriter::new(
            Box::new(fs.open_options(&file_path, options).await.unwrap()),
            executor,
        );
        let bytes = Bytes::from_static(b"Hello, world!");
        writer.write(bytes).await.unwrap();
        let bytes = Bytes::from_static(b"Hello, Fusio!");
        writer.write(bytes).await.unwrap();
        writer.complete().await.unwrap();

        let buf = Vec::new();
        let mut file = fs
            .open_options(&file_path, OpenOptions::default())
            .await
            .unwrap();

        let (result, buf) = file.read_to_end_at(buf, 0).await;
        result.unwrap();
        assert_eq!(buf.as_slice(), b"Hello, world!Hello, Fusio!");
    }

    async fn async_writer<E: Executor + Clone + Copy + 'static>(executor: E) {
        let tmp_dir = tempdir().unwrap();
        let fs = LocalFs {};
        let file_path = Path::from_filesystem_path(tmp_dir.path())
            .unwrap()
            .child("writer");
        let options = OpenOptions::default().create(true).write(true);

        let writer = AsyncWriter::new(
            Box::new(fs.open_options(&file_path, options).await.unwrap()),
            executor,
        );

        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
        let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let buf = Vec::new();
        let mut file = fs
            .open_options(&file_path, OpenOptions::default())
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
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_basic_write() {
        use fusio::MonoioExecutor;
        basic_write(MonoioExecutor).await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tokio_basic_write() {
        use fusio::executor::NoopExecutor;
        basic_write(NoopExecutor).await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_async_writer() {
        use fusio::MonoioExecutor;
        async_writer(MonoioExecutor).await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tokio_async_writer() {
        use fusio::executor::NoopExecutor;
        async_writer(NoopExecutor).await;
    }
}

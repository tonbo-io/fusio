use std::{cmp, ops::Range, sync::Arc};

use bytes::{Bytes, BytesMut};
#[allow(unused)]
use fusio::{dynamic::DynFile, Read};
use futures::{future::BoxFuture, FutureExt};
use parquet::{
    arrow::async_reader::AsyncFileReader,
    errors::ParquetError,
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        FOOTER_SIZE,
    },
};

const PREFETCH_FOOTER_SIZE: usize = 512 * 1024;

pub struct AsyncReader {
    #[cfg(any(feature = "web", feature = "monoio"))]
    inner: Arc<futures::lock::Mutex<Box<dyn DynFile>>>,
    #[cfg(not(any(feature = "web", feature = "monoio")))]
    inner: Box<dyn DynFile>,
    content_length: u64,
    // The prefetch size for fetching file footer.
    prefetch_footer_size: usize,
}

#[cfg(any(feature = "web", feature = "monoio"))]
unsafe impl Send for AsyncReader {}

fn set_prefetch_footer_size(footer_size: usize, content_size: u64) -> usize {
    let footer_size = cmp::max(footer_size, FOOTER_SIZE);
    cmp::min(footer_size as u64, content_size) as usize
}

impl AsyncReader {
    pub async fn new(reader: Box<dyn DynFile>, content_length: u64) -> Result<Self, fusio::Error> {
        #[cfg(any(feature = "web", feature = "monoio"))]
        #[allow(clippy::arc_with_non_send_sync)]
        let reader = Arc::new(futures::lock::Mutex::new(reader));
        Ok(Self {
            inner: reader,
            content_length,
            prefetch_footer_size: set_prefetch_footer_size(PREFETCH_FOOTER_SIZE, content_length),
        })
    }

    pub fn with_prefetch_footer_size(mut self, footer_size: usize) -> Self {
        self.prefetch_footer_size = set_prefetch_footer_size(footer_size, self.content_length);
        self
    }
}

impl AsyncFileReader for AsyncReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        cfg_if::cfg_if! {
            if #[cfg(all(feature = "web", target_arch = "wasm32"))] {
                let (sender, receiver) = futures::channel::oneshot::channel::<Result<Bytes, ParquetError>>();

                let reader = self.inner.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    let len = range.end - range.start;
                    let mut buf = Vec::with_capacity(len);
                    unsafe {
                        buf.set_len(len);
                    }

                    let result = {
                        let mut guard = reader.lock().await;
                        let (result, buf) = guard.read_exact_at(buf, range.start as u64).await;
                        match result {
                            Ok(_) => Ok(buf.into()),
                            Err(err) => Err(ParquetError::External(Box::new(err)))
                        }
                    };

                    let _ = sender.send(result);
                });

                async move {
                    receiver.await.unwrap()
                }
                .boxed()
            } else if #[cfg(feature = "monoio")] {
                let reader = self.inner.clone();
                monoio::spawn(async move {
                    let len = range.end - range.start;
                    let mut buf = Vec::with_capacity(len);
                    unsafe {
                        buf.set_len(len);
                    }

                    let mut guard = reader.lock().await;
                    let (result, buf) = guard.read_exact_at(buf, range.start as u64).await;
                    match result {
                        Ok(_) => Ok(buf.into()),
                        Err(err) => Err(ParquetError::External(Box::new(err)))
                    }
                }).boxed()
            } else {
                async move {
                    let len = range.end - range.start;
                    let mut buf = Vec::with_capacity(len);
                    unsafe {
                        buf.set_len(len);
                    }

                    let (result, _) = self.inner.read_exact_at(&mut buf[..], range.start as u64).await;
                    result.map_err(|err| ParquetError::External(Box::new(err)))?;
                    Ok(buf.into())
                }
                .boxed()
            }
        }
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        if self.content_length == 0 {
            return async { Err(ParquetError::EOF("file empty".to_string())) }.boxed();
        }
        let footer_size = self.prefetch_footer_size;
        let content_length = self.content_length;

        cfg_if::cfg_if! {
            if #[cfg(all(feature = "web", target_arch = "wasm32"))] {
                let mut buf = BytesMut::with_capacity(footer_size);
                buf.resize(footer_size, 0);
                let (sender, receiver) = futures::channel::oneshot::channel::<Result<Arc<ParquetMetaData>, ParquetError>>();

                let reader = self.inner.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    let mut guard = reader.lock().await;
                    let (result, prefetched_footer_content) = guard
                        .read_exact_at(buf, content_length - footer_size as u64)
                        .await;
                    if let Err(err) = result {
                        let _ = sender.send(Err(ParquetError::External(Box::new(err))));
                        return ;
                    }
                    let prefetched_footer_slice = prefetched_footer_content.as_ref();
                    let prefetched_footer_length = prefetched_footer_slice.len();

                    // Decode the metadata length from the last 8 bytes of the file.
                    let metadata_length = {
                        let buf = &prefetched_footer_slice
                            [(prefetched_footer_length - FOOTER_SIZE)..prefetched_footer_length];
                        debug_assert!(buf.len() == FOOTER_SIZE);
                        ParquetMetaDataReader::decode_footer_tail(buf.try_into().unwrap()).unwrap().metadata_length()
                    };

                    // Try to read the metadata from the `prefetched_footer_content`.
                    // Otherwise, fetch exact metadata from the remote.
                    if prefetched_footer_length >= metadata_length + FOOTER_SIZE {
                        let buf = &prefetched_footer_slice[(prefetched_footer_length
                            - metadata_length
                            - FOOTER_SIZE)
                            ..(prefetched_footer_length - FOOTER_SIZE)];

                        drop(guard);

                        let _ = sender.send(ParquetMetaDataReader::decode_metadata(buf)
                            .map(|meta| Arc::new(meta)));
                    } else {
                        let mut buf = BytesMut::with_capacity(metadata_length);
                        buf.resize(metadata_length, 0);

                        let (result, bytes) = guard
                            .read_exact_at(
                                buf,
                                content_length - metadata_length as u64 - FOOTER_SIZE as u64,
                            )
                            .await;

                        drop(guard);

                        if let Err(err) = result {
                            let _ = sender.send(Err(ParquetError::External(Box::new(err))));
                            return ;
                        }

                        let _ = sender.send(ParquetMetaDataReader::decode_metadata(&bytes)
                            .map(|meta| Arc::new(meta)));
                    }
                });
                async move {
                    receiver.await.unwrap()
                }.boxed()
            } else if #[cfg(feature = "monoio")] {
                let mut buf = BytesMut::with_capacity(footer_size);
                buf.resize(footer_size, 0);

                let reader = self.inner.clone();
                monoio::spawn(async move {
                    let mut guard = reader.lock().await;
                    let (result, prefetched_footer_content) = guard
                        .read_exact_at(buf, content_length - footer_size as u64)
                        .await;
                    result.map_err(|err| ParquetError::External(Box::new(err)))?;
                    let prefetched_footer_slice = prefetched_footer_content.as_ref();
                    let prefetched_footer_length = prefetched_footer_slice.len();

                    // Decode the metadata length from the last 8 bytes of the file.
                    let metadata_length = {
                        let buf = &prefetched_footer_slice
                            [(prefetched_footer_length - FOOTER_SIZE)..prefetched_footer_length];
                        debug_assert!(buf.len() == FOOTER_SIZE);
                        ParquetMetaDataReader::decode_footer_tail(buf.try_into().unwrap()).unwrap().metadata_length()
                    };

                    // Try to read the metadata from the `prefetched_footer_content`.
                    // Otherwise, fetch exact metadata from the remote.
                    if prefetched_footer_length >= metadata_length + FOOTER_SIZE {
                        let buf = &prefetched_footer_slice[(prefetched_footer_length
                            - metadata_length
                            - FOOTER_SIZE)
                            ..(prefetched_footer_length - FOOTER_SIZE)];
                        Ok(Arc::new(ParquetMetaDataReader::decode_metadata(buf)?))
                    } else {
                        let mut buf = BytesMut::with_capacity(metadata_length);
                        buf.resize(metadata_length, 0);

                        let (result, bytes) = guard
                            .read_exact_at(
                                buf,
                                content_length - metadata_length as u64 - FOOTER_SIZE as u64,
                            )
                            .await;
                        result.map_err(|err| ParquetError::External(Box::new(err)))?;

                        Ok(Arc::new(ParquetMetaDataReader::decode_metadata(&bytes)?))
                    }
                }).boxed()
            } else {
                async move {
                    let mut buf = BytesMut::with_capacity(footer_size);
                    buf.resize(footer_size, 0);
                    let (result, prefetched_footer_content) = self
                        .inner
                        .read_exact_at(buf, content_length - footer_size as u64)
                        .await;
                    result.map_err(|err| ParquetError::External(Box::new(err)))?;
                    let prefetched_footer_slice = prefetched_footer_content.as_ref();
                    let prefetched_footer_length = prefetched_footer_slice.len();

                    // Decode the metadata length from the last 8 bytes of the file.
                    let metadata_length = {
                        let buf = &prefetched_footer_slice
                            [(prefetched_footer_length - FOOTER_SIZE)..prefetched_footer_length];
                        debug_assert!(buf.len() == FOOTER_SIZE);
                        ParquetMetaDataReader::decode_footer_tail(buf.try_into().unwrap())?.metadata_length()
                    };

                    // Try to read the metadata from the `prefetched_footer_content`.
                    // Otherwise, fetch exact metadata from the remote.
                    if prefetched_footer_length >= metadata_length + FOOTER_SIZE {
                        let buf = &prefetched_footer_slice[(prefetched_footer_length
                            - metadata_length
                            - FOOTER_SIZE)
                            ..(prefetched_footer_length - FOOTER_SIZE)];
                        Ok(Arc::new(ParquetMetaDataReader::decode_metadata(buf)?))
                    } else {
                        let mut buf = BytesMut::with_capacity(metadata_length);
                        buf.resize(metadata_length, 0);

                        let (result, bytes) = self
                            .inner
                            .read_exact_at(
                                buf,
                                content_length - metadata_length as u64 - FOOTER_SIZE as u64,
                            )
                            .await;
                        result.map_err(|err| ParquetError::External(Box::new(err)))?;

                        Ok(Arc::new(ParquetMetaDataReader::decode_metadata(&bytes)?))
                    }
                }
                .boxed()
            }
        }
    }
}

#[cfg(test)]
#[cfg(any(feature = "monoio", feature = "tokio"))]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use fusio::{disk::LocalFs, fs::OpenOptions, path::Path, DynFs};
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

    async fn async_reader_with_prefetch_footer_size() {
        let tmp_dir = tempdir().unwrap();
        let fs = LocalFs {};
        let path = Path::from_filesystem_path(tmp_dir.path())
            .unwrap()
            .child("reader");
        // let options = OpenOptions::default().create(true).write(true);
        {
            let file = fs
                .open_options(&path, OpenOptions::default().create(true))
                .await
                .unwrap();

            let reader = AsyncReader::new(Box::new(file), 1024).await.unwrap();
            assert_eq!(reader.prefetch_footer_size, 1024);
            assert_eq!(reader.content_length, 1024);
        }

        {
            let file = fs
                .open_options(&path, OpenOptions::default())
                .await
                .unwrap();

            let reader = AsyncReader::new(Box::new(file), 1024 * 1024).await.unwrap();
            assert_eq!(reader.prefetch_footer_size, PREFETCH_FOOTER_SIZE);
            assert_eq!(reader.content_length, 1024 * 1024);
        }

        {
            let file = fs
                .open_options(&path, OpenOptions::default())
                .await
                .unwrap();

            let reader = AsyncReader::new(Box::new(file), 1024 * 1024)
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

            let reader = AsyncReader::new(Box::new(file), 1024 * 1024)
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

    async fn async_reader_with_large_metadata() {
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

            let writer = AsyncWriter::new(Box::new(fs.open_options(&path, options).await.unwrap()));

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

            let mut reader = AsyncReader::new(Box::new(file), size).await.unwrap();
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
        async_reader_with_prefetch_footer_size().await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tokio_async_reader_with_prefetch_footer_size() {
        async_reader_with_prefetch_footer_size().await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_async_reader_with_large_metadata() {
        async_reader_with_large_metadata().await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tokio_async_reader_with_large_metadata() {
        async_reader_with_large_metadata().await;
    }
}

use bytes::Bytes;
#[allow(unused)]
use fusio::{dynamic::DynFile, Write};
#[allow(unused_imports)]
use futures::{future::BoxFuture, FutureExt};
use parquet::{arrow::async_writer::AsyncFileWriter, errors::ParquetError};

pub struct AsyncWriter {
    #[cfg(any(feature = "web", feature = "monoio"))]
    #[allow(clippy::arc_with_non_send_sync)]
    inner: Option<std::sync::Arc<futures::lock::Mutex<Box<dyn DynFile>>>>,
    #[cfg(not(any(feature = "web", feature = "monoio")))]
    inner: Option<Box<dyn DynFile>>,
}

unsafe impl Send for AsyncWriter {}
impl AsyncWriter {
    pub fn new(writer: Box<dyn DynFile>) -> Self {
        #[cfg(any(feature = "web", feature = "monoio"))]
        #[allow(clippy::arc_with_non_send_sync)]
        let writer = std::sync::Arc::new(futures::lock::Mutex::new(writer));
        {
            Self {
                inner: Some(writer),
            }
        }
    }
}

impl AsyncFileWriter for AsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        cfg_if::cfg_if! {
            if #[cfg(all(feature = "web", target_arch = "wasm32"))] {
                match self.inner.as_mut() {
                    Some(writer) => {
                        let (sender, receiver) = futures::channel::oneshot::channel::<Result<(), ParquetError>>();
                        let writer = writer.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            let result = {
                                let mut guard = writer.lock().await;
                                let (result, _) = guard.write_all(bs).await;
                                result
                            };

                            let _ = sender.send(result
                                .map_err(|err| ParquetError::External(Box::new(err))));
                        });

                        Box::pin(async move {
                            receiver.await.unwrap()?;
                            Ok(())
                        })
                    },
                    None => Box::pin(async move {
                        Ok(())
                    })
                }
            } else if #[cfg(feature = "monoio")] {
                match self.inner.as_mut() {
                    Some(writer) => {
                        let writer = writer.clone();
                        monoio::spawn(async move  {
                            let mut guard = writer.lock().await;
                            let (result, _) = guard.write_all(bs).await;
                            result
                                .map_err(|err| ParquetError::External(Box::new(err)))
                        }).boxed()
                    },
                    None => Box::pin(async move {
                        Ok(())
                    })
                }
            } else {
                Box::pin(async move {
                    if let Some(writer) = self.inner.as_mut() {
                        let (result, _) = writer.write_all(bs).await;
                        result.map_err(|err| ParquetError::External(Box::new(err)))?;
                    }
                    Ok(())
                })
            }
        }
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        cfg_if::cfg_if! {
            if #[cfg(all(feature = "web", target_arch = "wasm32"))] {
                 match self.inner.take() {
                    Some(writer) => {
                        let (sender, receiver) = futures::channel::oneshot::channel::<Result<(), ParquetError>>();
                        wasm_bindgen_futures::spawn_local(async move {
                            let result = {
                                let mut guard = writer.lock().await;
                                guard.close().await
                            };
                            let _ = sender.send(result
                                .map_err(|err| ParquetError::External(Box::new(err))));
                        });
                        Box::pin(async move {
                            receiver.await.unwrap()
                        })
                    }
                    None => Box::pin(async move {
                        Ok(())
                    })
                }
            } else if #[cfg(feature = "monoio")] {
                match self.inner.take() {
                    Some(writer) => {
                        monoio::spawn(async move {
                            let mut guard = writer.lock().await;
                            guard.close().await
                            .map_err(|err| ParquetError::External(Box::new(err)))
                        }).boxed()
                    }
                    None => Box::pin(async move {
                        Ok(())
                    })
                }
            } else {
                Box::pin(async move {
                    if let Some(mut writer) = self.inner.take() {
                        writer
                            .close()
                            .await
                            .map_err(|err| ParquetError::External(Box::new(err)))?;
                    }

                    Ok(())
                })
            }
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

    async fn basic_write() {
        let tmp_dir = tempdir().unwrap();
        let fs = LocalFs {};
        let file_path = Path::from_filesystem_path(tmp_dir.path())
            .unwrap()
            .child("basic");
        let options = OpenOptions::default().create(true).write(true);

        let mut writer = AsyncWriter::new(Box::new(
            fs.open_options(&file_path, options).await.unwrap(),
        ));
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

    async fn async_writer() {
        let tmp_dir = tempdir().unwrap();
        let fs = LocalFs {};
        let file_path = Path::from_filesystem_path(tmp_dir.path())
            .unwrap()
            .child("writer");
        let options = OpenOptions::default().create(true).write(true);

        let writer = AsyncWriter::new(Box::new(
            fs.open_options(&file_path, options).await.unwrap(),
        ));

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
        basic_write().await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tokio_basic_write() {
        basic_write().await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_async_writer() {
        async_writer().await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tokio_async_writer() {
        async_writer().await;
    }
}

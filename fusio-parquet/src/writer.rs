use bytes::Bytes;
#[allow(unused)]
use fusio::{dynamic::DynFile, Write};
use futures::future::BoxFuture;
use parquet::{arrow::async_writer::AsyncFileWriter, errors::ParquetError};

pub struct AsyncWriter {
    inner: Option<Box<dyn DynFile>>,
    #[cfg(feature = "opfs")]
    pos: u64,
}

unsafe impl Send for AsyncWriter {}
impl AsyncWriter {
    pub fn new(writer: Box<dyn DynFile>) -> Self {
        Self {
            inner: Some(writer),
            #[cfg(feature = "opfs")]
            pos: 0,
        }
    }
}

impl AsyncFileWriter for AsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        cfg_if::cfg_if! {
            if #[cfg(all(feature = "opfs", target_arch = "wasm32"))] {
                match self.inner.as_mut() {
                    Some(writer) => {
                        let pos = self.pos;
                        self.pos += bs.len() as u64;
                        let (sender, receiver) =
                            futures::channel::oneshot::channel::<Result<(), ParquetError>>();
                        let opfs = unsafe {
                            std::mem::transmute::<&Box<dyn DynFile>, &Box<fusio::disk::OPFSFile>>(writer)
                        };
                        let handle = opfs.file_handle().unwrap();

                        wasm_bindgen_futures::spawn_local(async move {
                            let (result, _) = handle.write_at(bs, pos).await;
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
            if #[cfg(all(feature = "opfs", target_arch = "wasm32"))] {
                Box::pin(async move {
                    Ok(())
                })
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

#[cfg(feature = "tokio")]
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

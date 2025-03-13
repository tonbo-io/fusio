pub mod error;
pub mod fs;
mod option;
mod serdes;
use std::{io::Cursor, marker::PhantomData, sync::Arc};

use error::LogError;
use fs::hash::{HashReader, HashWriter};
pub use fusio::path::Path;
use fusio::{
    buffered::{BufReader, BufWriter},
    dynamic::DynFile,
    fs::OpenOptions,
    DynFs, DynWrite,
};
use futures_core::TryStream;
use futures_util::stream;
#[allow(unused)]
pub use option::*;
pub use serdes::*;

pub struct Logger<T> {
    path: Path,
    fs: Arc<dyn DynFs>,
    buf_writer: BufWriter<Box<dyn DynFile>>,
    _mark: PhantomData<T>,
}

impl<T> Logger<T>
where
    T: Encode,
{
    pub(crate) async fn new(option: Options) -> Result<Self, LogError> {
        let fs = option.fs_option.parse()?;
        let file = fs
            .open_options(
                &option.path,
                OpenOptions::default()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(option.truncate),
            )
            .await?;

        let buf_writer = BufWriter::new(file, option.buf_size);
        Ok(Self {
            fs,
            buf_writer,
            path: option.path,
            _mark: PhantomData,
        })
    }

    pub(crate) async fn with_fs(fs: Arc<dyn DynFs>, option: Options) -> Result<Self, LogError> {
        let file = fs
            .open_options(
                &option.path,
                OpenOptions::default()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(option.truncate),
            )
            .await?;

        let buf_writer = BufWriter::new(file, option.buf_size);
        Ok(Self {
            fs,
            buf_writer,
            path: option.path,
            _mark: PhantomData,
        })
    }
}

impl<T> Logger<T>
where
    T: Encode,
{
    pub async fn write_batch<'r>(
        &mut self,
        data: impl ExactSizeIterator<Item = &'r T>,
    ) -> Result<(), LogError>
    where
        T: 'r,
    {
        let mut writer = HashWriter::new(&mut self.buf_writer);
        (data.len() as u32).encode(&mut writer).await?;
        for e in data {
            e.encode(&mut writer)
                .await
                .map_err(|err| LogError::Encode {
                    message: err.to_string(),
                })?;
        }
        writer.eol().await?;
        Ok(())
    }

    pub async fn write(&mut self, data: &T) -> Result<(), LogError> {
        let mut writer = HashWriter::new(&mut self.buf_writer);

        1_u32.encode(&mut writer).await.unwrap();
        data.encode(&mut writer)
            .await
            .map_err(|err| LogError::Encode {
                message: err.to_string(),
            })?;
        writer.eol().await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), LogError> {
        self.buf_writer.flush().await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), LogError> {
        self.buf_writer.close().await?;
        Ok(())
    }
}

impl<T> Logger<T>
where
    T: Decode,
{
    pub(crate) async fn recover(
        option: Options,
    ) -> Result<impl TryStream<Ok = Vec<T>, Error = LogError> + Unpin, LogError> {
        let fs = option.fs_option.parse()?;
        let file = BufReader::new(
            fs.open_options(&option.path, OpenOptions::default().create(false))
                .await?,
            DEFAULT_BUF_SIZE,
        )
        .await?;

        Ok(Box::pin(stream::try_unfold(
            (file, 0),
            |(mut f, mut pos)| async move {
                let mut cursor = Cursor::new(&mut f);
                cursor.set_position(pos);
                let mut reader = HashReader::new(cursor);

                let Ok(len) = u32::decode(&mut reader).await else {
                    return Ok(None);
                };
                let mut buf = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    match T::decode(&mut reader).await {
                        Ok(record) => {
                            buf.push(record);
                        }
                        Err(err) => {
                            return Err(LogError::Decode {
                                message: err.to_string(),
                            });
                        }
                    }
                }

                pos += reader.position();
                if !reader.checksum().await? {
                    return Err(LogError::Checksum);
                }
                pos += size_of::<u32>() as u64;

                Ok(Some((buf, (f, pos))))
            },
        )))
    }
}

impl<T> Logger<T> {
    /// Remove log file
    pub async fn remove(self) -> Result<(), LogError> {
        self.fs.remove(&self.path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use futures_util::{StreamExt, TryStreamExt};
    use tempfile::TempDir;
    use tokio::pin;

    use crate::{
        fs::{AwsCredential, SeqRead, Write},
        Decode, Encode, FsOptions, Options, Path,
    };

    #[derive(Debug, Clone)]
    struct TestStruct {
        id: u64,
        name: String,
        email: Option<String>,
    }

    impl Encode for TestStruct {
        type Error = fusio::Error;

        async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
        where
            W: Write,
        {
            self.id.encode(writer).await?;
            self.name.encode(writer).await.unwrap();
            self.email.encode(writer).await.unwrap();
            Ok(())
        }

        fn size(&self) -> usize {
            self.id.size() + self.name.size() + self.email.size()
        }
    }

    impl Decode for TestStruct {
        type Error = fusio::Error;

        async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
        where
            R: SeqRead,
        {
            let id = u64::decode(reader).await?;
            let name = String::decode(reader).await.unwrap();
            let email = Option::<String>::decode(reader).await.unwrap();
            Ok(Self { id, name, email })
        }
    }

    fn test_items() -> Vec<TestStruct> {
        let mut items = vec![];
        for i in 0..50 {
            items.push(TestStruct {
                id: i,
                name: format!("Tonbo{}", i),
                email: Some(format!("fusio{}@tonboio.com", i)),
            });
        }
        items
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_write_u8() {
        let temp_dir = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp_dir.path())
            .unwrap()
            .child("u8");

        {
            let mut logger = Options::new(path.clone()).build::<u8>().await.unwrap();
            logger.write(&1).await.unwrap();
            logger.write_batch([2, 3, 4].iter()).await.unwrap();
            logger
                .write_batch([2, 3, 4, 5, 1, 255].iter())
                .await
                .unwrap();
            logger.flush().await.unwrap();
            logger.close().await.unwrap();
        }
        {
            let expected = [vec![1], vec![2, 3, 4], vec![2, 3, 4, 5, 1, 255]];
            let stream = Options::new(path)
                .recover::<u8>()
                .await
                .unwrap()
                .into_stream();
            pin!(stream);
            let mut i = 0;
            while let Some(res) = stream.next().await {
                assert!(res.is_ok());
                assert_eq!(&expected[i], &res.unwrap());
                i += 1;
            }
            assert_eq!(i, expected.len())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_write_struct() {
        let temp_dir = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp_dir.path())
            .unwrap()
            .child("struct");

        {
            let mut logger = Options::new(path.clone())
                .build::<TestStruct>()
                .await
                .unwrap();
            logger
                .write(&TestStruct {
                    id: 100,
                    name: "Name".to_string(),
                    email: None,
                })
                .await
                .unwrap();
            logger.write_batch(test_items().iter()).await.unwrap();
            logger.flush().await.unwrap();
            logger.close().await.unwrap();
        }
        {
            let expected = [
                &[TestStruct {
                    id: 100,
                    name: "Name".to_string(),
                    email: None,
                }],
                &test_items()[0..],
            ];
            let stream = Options::new(path)
                .recover::<TestStruct>()
                .await
                .unwrap()
                .into_stream();
            pin!(stream);
            let mut i = 0;
            while let Some(res) = stream.next().await {
                assert!(res.is_ok());
                for (left, right) in expected[i].iter().zip(res.unwrap()) {
                    assert_eq!(left.id, right.id);
                    assert_eq!(left.email, right.email);
                }
                i += 1;
            }
            assert_eq!(i, expected.len())
        }
    }

    #[ignore = "s3"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_write_s3() {
        let path = Path::from_url_path("log").unwrap();
        let option = Options::new(path).fs(FsOptions::S3 {
            bucket: "data".to_string(),
            credential: Some(AwsCredential {
                key_id: "key_id".to_string(),
                secret_key: "secret_key".to_string(),
                token: None,
            }),
            endpoint: None,
            region: Some("region".to_string()),
            sign_payload: None,
            checksum: None,
        });

        {
            let mut logger = option.clone().build::<TestStruct>().await.unwrap();
            logger
                .write(&TestStruct {
                    id: 100,
                    name: "Name".to_string(),
                    email: None,
                })
                .await
                .unwrap();
            logger.write_batch(test_items().iter()).await.unwrap();
            logger.flush().await.unwrap();
            logger.close().await.unwrap();
        }
        {
            let expected = [
                &[TestStruct {
                    id: 100,
                    name: "Name".to_string(),
                    email: None,
                }],
                &test_items()[0..],
            ];
            let stream = option.recover::<TestStruct>().await.unwrap().into_stream();
            pin!(stream);
            let mut i = 0;
            while let Some(res) = stream.next().await {
                assert!(res.is_ok());
                for (left, right) in expected[i].iter().zip(res.unwrap()) {
                    assert_eq!(left.id, right.id);
                    assert_eq!(left.email, right.email);
                }
                i += 1;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_recover_empty() {
        let temp_dir = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp_dir.path())
            .unwrap()
            .child("empty");

        {
            let mut logger = Options::new(path.clone())
                .build::<TestStruct>()
                .await
                .unwrap();
            logger.flush().await.unwrap();
            logger.close().await.unwrap();
        }
        {
            let mut stream = Options::new(path).recover::<TestStruct>().await.unwrap();
            let res = stream.try_next().await.unwrap();
            assert!(res.is_none());
        }
    }
}

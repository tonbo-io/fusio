#[cfg(feature = "fs")]
pub mod fs;

#[cfg(not(unix))]
use std::io::SeekFrom;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(not(unix))]
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::{fs::File, io::AsyncWriteExt, task::block_in_place};

use crate::{
    durability::{Commit, FileSync},
    error::Error,
    IoBuf, IoBufMut, Read, Write,
};

pub struct TokioFile {
    file: Option<File>,
}

impl TokioFile {
    pub(crate) fn new(file: File) -> Self {
        Self { file: Some(file) }
    }
}

impl AsRef<File> for TokioFile {
    fn as_ref(&self) -> &File {
        self.file.as_ref().unwrap()
    }
}

impl AsMut<File> for TokioFile {
    fn as_mut(&mut self) -> &mut File {
        self.file.as_mut().unwrap()
    }
}

impl Write for TokioFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let file = self.file.as_mut().expect("write file after closed");
        #[cfg(unix)]
        {
            let file = file.as_raw_fd();
            let result = block_in_place(|| {
                let buf = buf.as_slice();
                let mut file = unsafe { std::fs::File::from_raw_fd(file) };
                let res = std::io::Write::write_all(&mut file, buf).map_err(Error::Io);
                std::mem::forget(file);
                res
            });
            (result, buf)
        }
        #[cfg(not(unix))]
        {
            (
                AsyncWriteExt::write_all(file, buf.as_slice())
                    .await
                    .map_err(Error::from),
                buf,
            )
        }
    }

    async fn flush(&mut self) -> Result<(), Error> {
        AsyncWriteExt::flush(self.file.as_mut().unwrap())
            .await
            .map_err(Error::from)
    }

    async fn close(&mut self) -> Result<(), Error> {
        let file = self.file.as_mut().expect("close file after closed");
        File::shutdown(file).await?;
        Ok(())
    }
}

impl FileSync for TokioFile {
    async fn sync_data(&mut self) -> Result<(), Error> {
        self.file
            .as_ref()
            .expect("sync file after closed")
            .sync_data()
            .await
            .map_err(Error::from)
    }

    async fn sync_all(&mut self) -> Result<(), Error> {
        self.file
            .as_ref()
            .expect("sync file after closed")
            .sync_all()
            .await
            .map_err(Error::from)
    }

    async fn sync_range(&mut self, _offset: u64, _len: u64) -> Result<(), Error> {
        // no dedicated range sync primitive; fall back to data sync
        self.sync_data().await
    }
}

impl Commit for TokioFile {
    async fn commit(&mut self) -> Result<(), Error> {
        Err(Error::Unsupported {
            message: "commit not applicable for local files".to_string(),
        })
    }
}

impl Read for TokioFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let file = self.file.as_mut().expect("read file after closed");
        #[cfg(unix)]
        {
            let file = file.as_raw_fd();
            let result = block_in_place(|| {
                let buf = buf.as_slice_mut();
                let file = unsafe { std::fs::File::from_raw_fd(file) };
                let res = file.read_exact_at(buf, pos).map_err(Error::Io);
                std::mem::forget(file);
                res
            });
            (result, buf)
        }
        #[cfg(not(unix))]
        {
            // TODO: Use pread instead of seek + read_exact
            if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            match AsyncReadExt::read_exact(file, buf.as_slice_mut()).await {
                Ok(_) => (Ok(()), buf),
                Err(e) => (Err(Error::Io(e)), buf),
            }
        }
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let file = self.file.as_mut().expect("read file after closed");
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            let metadata = file.metadata().await;
            match metadata {
                Ok(metadata) => {
                    let size = metadata.len();
                    let file = file.as_raw_fd();
                    let result = block_in_place(|| {
                        let file = unsafe { std::fs::File::from_raw_fd(file) };
                        buf.resize((size - pos) as usize, 0);

                        let res = file.read_exact_at(&mut buf, pos).map_err(Error::Io);
                        std::mem::forget(file);
                        res
                    });
                    (result, buf)
                }
                Err(e) => (Err(Error::Io(e)), buf),
            }
        }
        #[cfg(not(unix))]
        {
            // TODO: Use pread instead of seek + read_exact
            if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            match AsyncReadExt::read_to_end(file, &mut buf).await {
                Ok(_) => (Ok(()), buf),
                Err(e) => (Err(Error::Io(e)), buf),
            }
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        self.file
            .as_ref()
            .expect("read file after closed")
            .metadata()
            .await
            .map(|metadata| metadata.len())
            .map_err(Error::from)
    }
}

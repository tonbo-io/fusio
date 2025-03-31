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

use crate::{Error, IoBuf, IoBufMut, Read, Write};

pub struct TokioFile {
    file: File,
}

impl TokioFile {
    pub(crate) fn new(file: File) -> Self {
        Self { file }
    }
}

impl AsRef<File> for TokioFile {
    fn as_ref(&self) -> &File {
        &self.file
    }
}

impl AsMut<File> for TokioFile {
    fn as_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

impl Write for TokioFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        #[cfg(unix)]
        {
            let file = self.file.as_raw_fd();
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
                AsyncWriteExt::write_all(&mut self.file, buf.as_slice())
                    .await
                    .map_err(Error::from),
                buf,
            )
        }
    }

    async fn flush(&mut self) -> Result<(), Error> {
        AsyncWriteExt::flush(&mut self.file)
            .await
            .map_err(Error::from)
    }

    async fn close(&mut self) -> Result<(), Error> {
        File::shutdown(&mut self.file).await?;
        Ok(())
    }
}

impl Read for TokioFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        #[cfg(unix)]
        {
            let file = self.file.as_raw_fd();
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
            if let Err(e) = AsyncSeekExt::seek(&mut self.file, SeekFrom::Start(pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            match AsyncReadExt::read_exact(&mut self.file, buf.as_slice_mut()).await {
                Ok(_) => (Ok(()), buf),
                Err(e) => (Err(Error::Io(e)), buf),
            }
        }
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            let metadata = self.file.metadata().await;
            match metadata {
                Ok(metadata) => {
                    let size = metadata.len();
                    let file = self.file.as_raw_fd();
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
            if let Err(e) = AsyncSeekExt::seek(&mut self.file, SeekFrom::Start(pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            match AsyncReadExt::read_exact(&mut self.file, &mut buf).await {
                Ok(_) => (Ok(()), buf),
                Err(e) => (Err(Error::Io(e)), buf),
            }
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        self.file
            .metadata()
            .await
            .map(|metadata| metadata.len())
            .map_err(Error::from)
    }
}

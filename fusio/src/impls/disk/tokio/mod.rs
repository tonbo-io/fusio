#[cfg(feature = "fs")]
pub mod fs;

#[cfg(not(unix))]
use std::io::SeekFrom;
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::ptr::slice_from_raw_parts;

#[cfg(not(unix))]
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::{fs::File, io::AsyncWriteExt, task::block_in_place};

use crate::{buf::IoBufMut, Error, IoBuf, Read, Write};

pub struct TokioFile {
    file: Option<File>,
    pos: u64,
}
impl TokioFile {
    pub(crate) fn new(file: File, pos: u64) -> Self {
        Self {
            file: Some(file),
            pos,
        }
    }
}

impl Write for TokioFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_mut().unwrap();
        let buf_len = buf.bytes_init();

        let pos = self.pos;
        self.pos += buf.bytes_init() as u64;

        #[cfg(unix)]
        {
            let file = file.as_raw_fd();
            let result = block_in_place(|| {
                let file = unsafe { std::fs::File::from_raw_fd(file) };
                let res = file
                    .write_all_at(
                        unsafe { &*slice_from_raw_parts(buf.as_ptr(), buf_len) },
                        pos,
                    )
                    .map_err(Error::Io);
                std::mem::forget(file);
                res
            });
            (result, buf)
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = AsyncSeekExt::seek(&mut file, SeekFrom::Start(pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            (
                AsyncWriteExt::write_all(self.file.as_mut().unwrap(), unsafe {
                    &*slice_from_raw_parts(buf.as_ptr(), buf_len)
                })
                .await
                .map_err(Error::from),
                buf,
            )
        }
    }

    async fn flush(&mut self) -> Result<(), Error> {
        debug_assert!(self.file.is_some(), "file is already closed");

        AsyncWriteExt::flush(self.file.as_mut().unwrap())
            .await
            .map_err(Error::from)
    }

    async fn close(&mut self) -> Result<(), Error> {
        debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_mut().unwrap();
        AsyncWriteExt::flush(file).await.map_err(Error::from)?;
        File::shutdown(file).await?;
        self.file.take();
        Ok(())
    }
}

impl Read for TokioFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        // println!(
        //     "read eact at range: {:?}, len: {:?}",
        //     pos..pos + buf.bytes_init() as u64,
        //     buf.bytes_init()
        // );
        debug_assert!(self.file.is_some(), "file is already closed");
        let file = self.file.as_mut().unwrap();

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
        debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_mut().unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            let size = file.metadata().await.unwrap().len();
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
        #[cfg(not(unix))]
        {
            // TODO: Use pread instead of seek + read_exact
            if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            match AsyncReadExt::read_exact(file, &mut buf).await {
                Ok(_) => (Ok(()), buf),
                Err(e) => (Err(Error::Io(e)), buf),
            }
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        debug_assert!(self.file.is_some(), "file is already closed");

        Ok(self.file.as_ref().unwrap().metadata().await?.len())
    }
}

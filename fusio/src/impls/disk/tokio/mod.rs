#[cfg(feature = "fs")]
pub mod fs;

#[cfg(unix)]
use std::io::Write as _;
use std::{io::SeekFrom, ptr::slice_from_raw_parts};

#[cfg(windows)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{fs::File, io::AsyncSeekExt, task::spawn_blocking};

use crate::{buf::IoBufMut, Error, IoBuf, Read, Write};

pub struct TokioFile {
    #[cfg(unix)]
    file: Option<std::fs::File>,
    #[cfg(windows)]
    file: Option<File>,
    pos: u64,
}
impl TokioFile {
    pub(crate) async fn new(mut file: File, pos: u64) -> Result<Self, Error> {
        if let Err(e) = AsyncSeekExt::seek(&mut file, SeekFrom::Start(pos)).await {
            return Err(Error::Io(e));
        }
        Ok(Self {
            #[cfg(unix)]
            file: Some(file.into_std().await),
            #[cfg(windows)]
            file: Some(file),
            pos,
        })
    }
}

impl Write for TokioFile {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        debug_assert!(self.file.is_some(), "file is already closed");

        let buf_len = buf.bytes_init();

        #[cfg(unix)]
        let result = {
            let mut file = self.file.take();
            let mut buf = buf.as_bytes().to_vec();
            let pos = self.pos;
            let (res, file) = spawn_blocking(move || {
                debug_assert!(file.is_some(), "file is already closed");

                use std::os::unix::fs::FileExt;
                let fd = file.as_mut().unwrap();
                (
                    // fd.write_all(unsafe { &*slice_from_raw_parts(buf.as_ptr(), buf_len) })
                    fd.write_all_at(buf.as_mut(), pos).map_err(Error::from),
                    file,
                )
            })
            .await
            .unwrap();
            self.file = file;
            res
        };
        #[cfg(windows)]
        let result = {
            let file = self.file.as_mut().unwrap();
            if let Err(e) = AsyncSeekExt::seek(file, SeekFrom::Start(self.pos)).await {
                return (Err(Error::Io(e)), buf);
            }
            AsyncWriteExt::write_all(self.file.as_mut().unwrap(), unsafe {
                &*slice_from_raw_parts(buf.as_ptr(), buf_len)
            })
            .await
            .map_err(Error::from)
        };
        self.pos += buf_len as u64;

        (result, buf)
    }

    async fn flush(&mut self) -> Result<(), Error> {
        // debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_mut().unwrap();
        #[cfg(unix)]
        {
            file.flush().map_err(Error::from)
        }
        #[cfg(windows)]
        {
            AsyncWriteExt::flush(file).await.map_err(Error::from)
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_mut().unwrap();
        #[cfg(unix)]
        {
            file.flush().map_err(Error::from)?;
            file.sync_all().map_err(Error::from)?;
        };
        #[cfg(windows)]
        {
            AsyncWriteExt::flush(file).await.map_err(Error::from)?;
            File::shutdown(file).await?;
            self.file.take();
        };
        Ok(())
    }
}

impl Read for TokioFile {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        debug_assert!(self.file.is_some(), "file is already closed");

        let size = self.size().await.unwrap();
        if size < pos + buf.bytes_init() as u64 {
            return (
                Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Read unexpected eof",
                ))),
                buf,
            );
        }
        #[cfg(unix)]
        {
            let file = self.file.take();
            use std::os::unix::fs::FileExt;
            let buf_len = buf.bytes_init();

            let (res, bytes, file) = spawn_blocking(move || {
                let mut buf = vec![0; buf_len];
                if let Some(file) = file {
                    match file.read_exact_at(buf.as_mut(), pos) {
                        Ok(_) => (Ok(()), buf, Some(file)),
                        Err(e) => (Err(Error::Io(e)), buf, Some(file)),
                    }
                } else {
                    (Ok(()), buf, None)
                }
            })
            .await
            .unwrap();
            let slice = buf.as_slice_mut();
            slice.copy_from_slice(bytes.as_slice());
            self.file = file;
            (res, buf)
        }
        #[cfg(windows)]
        {
            let file = self.file.as_mut().unwrap();
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

        let size = self.size().await.unwrap();
        if size < pos + buf.bytes_init() as u64 {
            return (
                Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Read unexpected eof",
                ))),
                buf,
            );
        }

        #[cfg(unix)]
        {
            let file = self.file.take();
            use std::os::unix::fs::FileExt;
            let (res, bytes, file) = spawn_blocking(move || {
                let mut buf = vec![0; (size - pos) as usize];
                if let Some(file) = file {
                    match file.read_exact_at(buf.as_mut(), pos) {
                        Ok(_) => (Ok(()), buf, Some(file)),
                        Err(e) => (Err(Error::Io(e)), buf, Some(file)),
                    }
                } else {
                    (Ok(()), buf, None)
                }
            })
            .await
            .unwrap();

            buf.extend(bytes);
            self.file = file;
            (res, buf)
        }
        #[cfg(windows)]
        {
            let file = self.file.as_mut().unwrap();
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
        debug_assert!(self.file.is_some(), "file is already closed");

        let file = self.file.as_ref().unwrap();
        #[cfg(unix)]
        {
            Ok(file.metadata()?.len())
        }
        #[cfg(windows)]
        {
            Ok(file.metadata().await?.len())
        }
    }
}

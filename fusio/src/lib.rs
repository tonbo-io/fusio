mod buf;
#[cfg(feature = "dyn")]
pub mod dynamic;
mod error;
#[cfg(feature = "fs")]
pub mod fs;
pub mod impls;
pub mod path;

use std::future::Future;

pub use buf::{IoBuf, IoBufMut};
#[cfg(all(feature = "dyn", feature = "fs"))]
pub use dynamic::fs::DynFs;
#[cfg(feature = "dyn")]
pub use dynamic::{DynRead, DynWrite};
pub use error::Error;
pub use impls::*;

/// # Safety
/// Do not implement it directly
#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSend: Send {}

/// # Safety
/// Do not implement it directly
#[cfg(feature = "no-send")]
pub unsafe trait MaybeSend {}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Send> MaybeSend for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSend for T {}

/// # Safety
/// Do not implement it directly
#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSync: Sync {}

/// # Safety
/// Do not implement it directly
#[cfg(feature = "no-send")]
pub unsafe trait MaybeSync {}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Sync> MaybeSync for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSync for T {}

pub trait Write: MaybeSend + MaybeSync {
    fn write_all<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend;

    fn sync_data(&self) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn sync_all(&self) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

pub trait Read: MaybeSend + MaybeSync {
    fn read<B: IoBufMut>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<u64, Error>, B)> + MaybeSend;

    fn read_exact<B: IoBufMut>(
        &mut self,
        mut buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend {
        async move {
            let len = buf.bytes_init() as u64;
            let mut read = 0;

            while read < len {
                let buf_mut = unsafe { buf.slice_mut_unchecked(read as usize..) };
                let (result, buf_mut) = self.read(buf_mut).await;
                buf = unsafe { B::recover_from_buf_mut(buf_mut) };

                match result {
                    Ok(0) => {
                        return (
                            Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "failed to fill whole buffer",
                            )
                            .into()),
                            buf,
                        )
                    }
                    Ok(n) => {
                        read += n;
                    }
                    Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return (Err(e), buf),
                }
            }
            (Ok(()), buf)
        }
    }

    fn read_to_end(
        &mut self,
        buf: Vec<u8>,
    ) -> impl Future<Output = (Result<(), Error>, Vec<u8>)> + MaybeSend;

    fn size(&self) -> impl Future<Output = Result<u64, Error>> + MaybeSend;
}

pub trait Seek: MaybeSend {
    fn seek(&mut self, pos: u64) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

impl<S: Seek> Seek for &mut S {
    fn seek(&mut self, pos: u64) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        S::seek(self, pos)
    }
}

impl<R: Read> Read for &mut R {
    fn read<B: IoBufMut>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<u64, Error>, B)> + MaybeSend {
        R::read(self, buf)
    }

    fn read_to_end(
        &mut self,
        buf: Vec<u8>,
    ) -> impl Future<Output = (Result<(), Error>, Vec<u8>)> + MaybeSend {
        R::read_to_end(self, buf)
    }

    fn size(&self) -> impl Future<Output = Result<u64, Error>> + MaybeSend {
        R::size(self)
    }
}

impl<W: Write> Write for &mut W {
    fn write_all<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend {
        W::write_all(self, buf)
    }

    fn sync_data(&self) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        W::sync_data(self)
    }

    fn sync_all(&self) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        W::sync_all(self)
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        W::close(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{Read, Write};
    use crate::{buf::IoBufMut, Error, IoBuf, Seek};

    #[allow(unused)]
    struct CountWrite<W> {
        cnt: usize,
        w: W,
    }

    impl<W> CountWrite<W> {
        #[allow(unused)]
        fn new(w: W) -> Self {
            Self { cnt: 0, w }
        }
    }

    impl<W> Write for CountWrite<W>
    where
        W: Write,
    {
        async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
            let (result, buf) = self.w.write_all(buf).await;
            (result.inspect(|_| self.cnt += buf.bytes_init()), buf)
        }

        async fn sync_data(&self) -> Result<(), Error> {
            self.w.sync_data().await
        }

        async fn sync_all(&self) -> Result<(), Error> {
            self.w.sync_all().await
        }

        async fn close(&mut self) -> Result<(), Error> {
            self.w.close().await
        }
    }

    #[allow(unused)]
    struct CountRead<R> {
        cnt: usize,
        r: R,
    }

    impl<R> CountRead<R> {
        #[allow(unused)]
        fn new(r: R) -> Self {
            Self { cnt: 0, r }
        }
    }

    impl<R> Read for CountRead<R>
    where
        R: Read,
    {
        async fn read<B: IoBufMut>(&mut self, buf: B) -> (Result<u64, Error>, B) {
            let (result, buf) = self.r.read(buf).await;
            match result {
                Ok(result) => {
                    self.cnt += buf.bytes_init();
                    (Ok(result), buf)
                }
                Err(e) => (Err(e), buf),
            }
        }

        async fn read_to_end(&mut self, buf: Vec<u8>) -> (Result<(), Error>, Vec<u8>) {
            let (result, buf) = self.r.read_to_end(buf).await;
            match result {
                Ok(()) => {
                    self.cnt += buf.bytes_init();
                    (Ok(()), buf)
                }
                Err(e) => (Err(e), buf),
            }
        }

        async fn size(&self) -> Result<u64, Error> {
            self.r.size().await
        }
    }

    impl<R: Seek> Seek for CountRead<R> {
        async fn seek(&mut self, pos: u64) -> Result<(), Error> {
            self.r.seek(pos).await
        }
    }

    #[allow(unused)]
    async fn write_and_read<W, R>(write: W, read: R)
    where
        W: Write,
        R: Read + Seek,
    {
        let mut writer = CountWrite::new(write);
        #[cfg(feature = "completion-based")]
        writer.write_all(vec![2, 0, 2, 4]).await;
        #[cfg(not(feature = "completion-based"))]
        writer.write_all(&[2, 0, 2, 4][..]).await;

        writer.sync_data().await.unwrap();

        let mut reader = CountRead::new(read);
        {
            reader.seek(0).await.unwrap();

            let mut buf = vec![];
            let (result, buf) = reader.read_to_end(buf).await;
            result.unwrap();

            assert_eq!(buf.bytes_init(), 4);
            assert_eq!(buf.as_slice(), &[2, 0, 2, 4]);
        }
        {
            reader.seek(2).await.unwrap();

            let mut buf = vec![];
            let (result, buf) = reader.read_to_end(buf).await;
            result.unwrap();

            assert_eq!(buf.bytes_init(), 2);
            assert_eq!(buf.as_slice(), &[2, 4]);
        }
    }

    #[allow(unused)]
    async fn test_local_fs<S>(fs: S) -> Result<(), Error>
    where
        S: crate::fs::Fs,
    {
        use std::collections::HashSet;

        use futures_util::StreamExt;
        use tempfile::TempDir;

        use crate::{fs::OpenOptions, path::Path, DynFs};

        let tmp_dir = TempDir::new()?;
        let work_dir_path = tmp_dir.path().join("work");
        let work_file_path = work_dir_path.join("test.file");

        fs.create_dir_all(&Path::from_absolute_path(&work_dir_path)?)
            .await?;

        assert!(work_dir_path.exists());
        assert!(fs
            .open_options(
                &Path::from_absolute_path(&work_file_path)?,
                OpenOptions::default()
            )
            .await
            .is_err());
        {
            let _ = fs
                .open_options(
                    &Path::from_absolute_path(&work_file_path)?,
                    OpenOptions::default().create(true).write(true),
                )
                .await?;
            assert!(work_file_path.exists());
        }
        {
            let mut file = fs
                .open_options(
                    &Path::from_absolute_path(&work_file_path)?,
                    OpenOptions::default().write(true),
                )
                .await?;
            file.write_all("Hello! fusio".as_bytes()).await.0?;
            let mut file = fs
                .open_options(
                    &Path::from_absolute_path(&work_file_path)?,
                    OpenOptions::default().write(true),
                )
                .await?;
            file.write_all("Hello! world".as_bytes()).await.0?;

            file.sync_all().await.unwrap();

            file.seek(0).await;

            let (result, buf) = file.read_exact(vec![0u8; 12]).await;
            result.unwrap();
            assert_eq!(buf.as_slice(), b"Hello! world");
        }

        Ok(())
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio() {
        use tempfile::tempfile;
        use tokio::fs::File;

        let read = tempfile().unwrap();
        let write = read.try_clone().unwrap();

        write_and_read(File::from_std(write), File::from_std(read)).await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_fs() {
        use crate::disk::TokioFs;

        test_local_fs(TokioFs).await.unwrap();
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_read_exact() {
        use tempfile::tempfile;
        use tokio::fs::File;

        let mut file = File::from_std(tempfile().unwrap());
        let (result, _) = file.write_all(&b"hello, world"[..]).await;
        result.unwrap();
        file.seek(0).await.unwrap();
        let (result, buf) = file.read_exact(vec![0u8; 5]).await;
        result.unwrap();
        assert_eq!(buf.as_slice(), b"hello");
        let (result, _) = file.read_exact(vec![0u8; 8]).await;
        assert!(result.is_err());
        if let Error::Io(e) = result.unwrap_err() {
            assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
        }
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio() {
        use monoio::fs::File;
        use tempfile::tempfile;

        use crate::disk::{monoio::MonoioFile, Position};

        let read = tempfile().unwrap();
        let write = read.try_clone().unwrap();

        write_and_read(
            MonoioFile::from((File::from_std(write).unwrap(), Position::default())),
            MonoioFile::from((File::from_std(read).unwrap(), Position::default())),
        )
        .await;
    }

    #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
    #[test]
    fn test_tokio_uring() {
        use tempfile::tempfile;
        use tokio_uring::fs::File;

        use crate::disk::{tokio_uring::TokioUringFile, Position};

        tokio_uring::start(async {
            let read = tempfile().unwrap();
            let write = read.try_clone().unwrap();

            write_and_read(
                TokioUringFile::from((File::from_std(write), Position::default())),
                TokioUringFile::from((File::from_std(read), Position::default())),
            )
            .await;
        });
    }
}

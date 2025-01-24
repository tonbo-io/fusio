//! Fusio is a library that provides a unified IO interface for different IO backends.
//! # Example
//! ```no_run
//! use fusio::{Error, IoBuf, IoBufMut, Read, Write};
//!
//! async fn write_without_runtime_awareness<F, B, BM>(
//!     file: &mut F,
//!     write_buf: B,
//!     read_buf: BM,
//! ) -> (Result<(), Error>, B, BM)
//! where
//!     F: Read + Write,
//!     B: IoBuf,
//!     BM: IoBufMut,
//! {
//!     let (result, write_buf) = file.write_all(write_buf).await;
//!     if result.is_err() {
//!         return (result, write_buf, read_buf);
//!     }
//!
//!     file.close().await.unwrap();
//!
//!     let (result, read_buf) = file.read_exact_at(read_buf, 0).await;
//!     if result.is_err() {
//!         return (result.map(|_| ()), write_buf, read_buf);
//!     }
//!
//!     (Ok(()), write_buf, read_buf)
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     #[cfg(feature = "tokio")]
//!     {
//!         use fusio::{disk::LocalFs, DynFs};
//!         use tokio::fs::File;
//!
//!         let fs = LocalFs {};
//!         let mut file = fs.open(&"foo.txt".into()).await.unwrap();
//!         let write_buf = "hello, world".as_bytes();
//!         let mut read_buf = [0; 12];
//!         let (result, _, read_buf) =
//!             write_without_runtime_awareness(&mut file, write_buf, &mut read_buf[..]).await;
//!         result.unwrap();
//!         assert_eq!(&read_buf, b"hello, world");
//!     }
//! }
//! ```

pub mod buf;
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

#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSend: Send {
    //! Considering lots of runtimes does not require [`std::marker::Send`] for
    //! [`std::future::Future`] and [`futures_core::stream::Stream`], we provide a trait to
    //! represent the future or stream that may not require [`std::marker::Send`]. Users could
    //! switch the feature `no-send` at compile-time to disable the [`std::marker::Send`] bound
    //! for [`std::future::Future`] and [`futures_core::stream::Stream`].
    //!
    //! # Safety
    //! Do not implement it directly.
}

/// # Safety
/// Do not implement it directly
#[cfg(feature = "no-send")]
pub unsafe trait MaybeSend {}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Send> MaybeSend for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSend for T {}

#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSync: Sync {
    //! Same as [`MaybeSend`], but for [`std::marker::Sync`].
    //!
    //! # Safety
    //! Do not implement it directly.
}

#[cfg(feature = "no-send")]
pub unsafe trait MaybeSync {
    //! Same as [`MaybeSend`], but for [`std::marker::Sync`].
    //!
    //! # Safety
    //! Do not implement it directly.
}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Sync> MaybeSync for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSync for T {}

pub trait Write: MaybeSend {
    //! The core trait for writing data,
    //! it is similar to [`std::io::Write`], but it takes the ownership of the buffer,
    //! because completion-based IO requires the buffer to be pinned and should be safe to
    //! cancellation.
    //!
    //! [`Write`] represents "sequential write all and overwrite" semantics,
    //! which means each buffer will be written to the file sequentially and overwrite the previous
    //! file when closed.
    //!
    //! Contents are not be garanteed to be written to the file until the [`Write::close`] method is
    //! called, [`Write::flush`] may be used to flush the data to the file in some
    //! implementations, but not all implementations will do so.
    //!
    //! Whether the operation is successful or not, the buffer will be returned,
    //! fusio promises that the returned buffer will be the same as the input buffer.
    //!
    //! # Dyn Compatibility
    //! This trait is not dyn compatible.
    //! If you want to use [`Write`] trait in a dynamic way, you could use [`DynWrite`] trait.

    fn write_all<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend;

    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

pub trait Read: MaybeSend + MaybeSync {
    //! The core trait for reading data,
    //! it is similar to [`std::io::Read`],
    //! but it takes the ownership of the buffer,
    //! because completion-based IO requires the buffer to be pinned and should be safe to
    //! cancellation.
    //!
    //! [`Read`] represents "random exactly read" semantics,
    //! which means the read operation will start at the specified position, and the buffer will be
    //! exactly filled with the data read.
    //!
    //! The buffer will be returned with the result, whether the operation is successful or not,
    //! fusio promises that the returned buffer will be the same as the input buffer.
    //!
    //! If you want sequential reading, try [`SeqRead`].
    //!
    //! # Dyn Compatibility
    //! This trait is not dyn compatible.
    //! If you want to use [`Read`] trait in a dynamic way, you could use [`DynRead`] trait.

    fn read_exact_at<B: IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend;

    fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Error>, Vec<u8>)> + MaybeSend;

    fn size(&self) -> impl Future<Output = Result<u64, Error>> + MaybeSend;
}

impl<R: Read> Read for &mut R {
    fn read_exact_at<B: IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend {
        R::read_exact_at(self, buf, pos)
    }

    fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Error>, Vec<u8>)> + MaybeSend {
        R::read_to_end_at(self, buf, pos)
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

    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        W::flush(self)
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        W::close(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{Read, Write};
    use crate::{buf::IoBufMut, Error, IoBuf};

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

        async fn flush(&mut self) -> Result<(), Error> {
            self.w.flush().await
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
        async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
            let (result, buf) = self.r.read_exact_at(buf, pos).await;
            match result {
                Ok(()) => {
                    self.cnt += buf.bytes_init();
                    (Ok(()), buf)
                }
                Err(e) => (Err(e), buf),
            }
        }

        async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
            let (result, buf) = self.r.read_to_end_at(buf, pos).await;
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

    #[allow(unused)]
    async fn write_and_read<W, R>(write: W, read: R)
    where
        W: Write,
        R: Read,
    {
        let mut writer = CountWrite::new(write);
        #[cfg(feature = "completion-based")]
        writer.write_all(vec![2, 0, 2, 4]).await;
        #[cfg(not(feature = "completion-based"))]
        writer.write_all(&[2, 0, 2, 4][..]).await;

        writer.close().await.unwrap();

        let mut reader = CountRead::new(read);
        {
            let mut buf = vec![];
            let (result, buf) = reader.read_to_end_at(buf, 0).await;
            result.unwrap();

            assert_eq!(buf.bytes_init(), 4);
            assert_eq!(buf.as_slice(), &[2, 0, 2, 4]);
        }
        {
            let mut buf = vec![];
            let (result, buf) = reader.read_to_end_at(buf, 2).await;
            result.unwrap();

            assert_eq!(buf.bytes_init(), 2);
            assert_eq!(buf.as_slice(), &[2, 4]);
        }
    }

    #[allow(unused)]
    #[cfg(not(target_arch = "wasm32"))]
    async fn test_local_fs_read_write<S>(fs: S) -> Result<(), Error>
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
            file.flush().await.unwrap();
            file.close().await.unwrap();

            let mut file = fs
                .open_options(
                    &Path::from_absolute_path(&work_file_path)?,
                    OpenOptions::default().read(true),
                )
                .await?;

            let (result, buf) = file.read_exact_at(vec![0u8; 12], 0).await;
            result.unwrap();
            assert_eq!(buf.as_slice(), b"Hello! world");
        }

        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(unused)]
    async fn test_local_fs_copy_link<F: crate::fs::Fs>(src_fs: F) -> Result<(), Error> {
        use std::collections::HashSet;

        use futures_util::StreamExt;
        use tempfile::TempDir;

        use crate::{fs::OpenOptions, path::Path, DynFs};

        let tmp_dir = TempDir::new()?;

        let work_dir_path = tmp_dir.path().join("work_dir");
        let src_file_path = work_dir_path.join("src_test.file");
        let dst_file_path = work_dir_path.join("dst_test.file");

        src_fs
            .create_dir_all(&Path::from_absolute_path(&work_dir_path)?)
            .await?;

        // create files
        let _ = src_fs
            .open_options(
                &Path::from_absolute_path(&src_file_path)?,
                OpenOptions::default().create(true),
            )
            .await?;
        let _ = src_fs
            .open_options(
                &Path::from_absolute_path(&dst_file_path)?,
                OpenOptions::default().create(true),
            )
            .await?;
        // copy
        {
            let mut src_file = src_fs
                .open_options(
                    &Path::from_absolute_path(&src_file_path)?,
                    OpenOptions::default().write(true),
                )
                .await?;
            src_file.write_all("Hello! fusio".as_bytes()).await.0?;
            src_file.close().await?;

            src_fs
                .copy(
                    &Path::from_absolute_path(&src_file_path)?,
                    &Path::from_absolute_path(&dst_file_path)?,
                )
                .await?;

            let mut src_file = src_fs
                .open_options(
                    &Path::from_absolute_path(&src_file_path)?,
                    OpenOptions::default().write(true).read(true),
                )
                .await?;
            src_file.write_all("Hello! world".as_bytes()).await.0?;
            src_file.flush().await?;
            src_file.close().await?;

            let mut src_file = src_fs
                .open_options(
                    &Path::from_absolute_path(&src_file_path)?,
                    OpenOptions::default().write(true).read(true),
                )
                .await?;

            let (result, buf) = src_file.read_exact_at(vec![0u8; 12], 0).await;
            result.unwrap();
            assert_eq!(buf.as_slice(), b"Hello! world");

            let mut dst_file = src_fs
                .open_options(
                    &Path::from_absolute_path(&dst_file_path)?,
                    OpenOptions::default().read(true),
                )
                .await?;

            let (result, buf) = dst_file.read_exact_at(vec![0u8; 12], 0).await;
            result.unwrap();
            assert_eq!(buf.as_slice(), b"Hello! fusio");
        }

        src_fs
            .remove(&Path::from_absolute_path(&dst_file_path)?)
            .await?;
        // link
        {
            let mut src_file = src_fs
                .open_options(
                    &Path::from_absolute_path(&src_file_path)?,
                    OpenOptions::default().write(true),
                )
                .await?;
            src_file.write_all("Hello! fusio".as_bytes()).await.0?;
            src_file.close().await?;

            src_fs
                .link(
                    &Path::from_absolute_path(&src_file_path)?,
                    &Path::from_absolute_path(&dst_file_path)?,
                )
                .await?;

            let mut src_file = src_fs
                .open_options(
                    &Path::from_absolute_path(&src_file_path)?,
                    OpenOptions::default().write(true).read(true),
                )
                .await?;
            src_file.write_all("Hello! world".as_bytes()).await.0?;
            src_file.flush().await?;
            src_file.close().await?;

            let mut src_file = src_fs
                .open_options(
                    &Path::from_absolute_path(&src_file_path)?,
                    OpenOptions::default().write(true).read(true),
                )
                .await?;
            let (result, buf) = src_file.read_exact_at(vec![0u8; 12], 0).await;
            result.unwrap();
            assert_eq!(buf.as_slice(), b"Hello! world");

            let mut dst_file = src_fs
                .open_options(
                    &Path::from_absolute_path(&dst_file_path)?,
                    OpenOptions::default().read(true),
                )
                .await?;

            let (result, buf) = dst_file.read_exact_at(vec![0u8; 12], 0).await;
            result.unwrap();
            assert_eq!(buf.as_slice(), b"Hello! world");
        }

        Ok(())
    }

    #[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn test_tokio() {
        use tempfile::tempfile;
        use tokio::fs::File;

        use crate::disk::tokio::TokioFile;

        let read = tempfile().unwrap();
        let write = read.try_clone().unwrap();
        let read_file = TokioFile::new(File::from_std(read));
        let write_file = TokioFile::new(File::from_std(write));
        write_and_read(write_file, read_file).await;
    }

    #[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn test_tokio_fs() {
        use crate::disk::TokioFs;

        test_local_fs_read_write(TokioFs).await.unwrap();
        test_local_fs_copy_link(TokioFs).await.unwrap();
    }

    #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
    #[test]
    fn test_tokio_uring_fs() {
        use crate::disk::tokio_uring::fs::TokioUringFs;

        tokio_uring::start(async {
            test_local_fs_read_write(TokioUringFs).await.unwrap();
            test_local_fs_copy_link(TokioUringFs).await.unwrap();
        })
    }

    #[cfg(all(feature = "monoio", not(target_arch = "wasm32")))]
    #[monoio::test]
    async fn test_monoio_fs() {
        use crate::disk::monoio::fs::MonoIoFs;

        test_local_fs_read_write(MonoIoFs).await.unwrap();
        test_local_fs_copy_link(MonoIoFs).await.unwrap();
    }

    #[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn test_read_exact() {
        use tempfile::tempfile;
        use tokio::fs::File;

        use crate::disk::tokio::TokioFile;

        let mut file = TokioFile::new(File::from_std(tempfile().unwrap()));
        let (result, _) = file.write_all(&b"hello, world"[..]).await;
        result.unwrap();
        let (result, buf) = file.read_exact_at(vec![0u8; 5], 0).await;
        result.unwrap();
        assert_eq!(buf.as_slice(), b"hello");
        let (result, _) = file.read_exact_at(vec![0u8; 8], 5).await;
        assert!(result.is_err());
        if let Error::Io(e) = result.unwrap_err() {
            assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
        }
    }

    #[cfg(all(feature = "monoio", not(target_arch = "wasm32")))]
    #[monoio::test]
    async fn test_monoio() {
        use monoio::fs::File;
        use tempfile::tempfile;

        use crate::disk::monoio::MonoioFile;

        let read = tempfile().unwrap();
        let write = read.try_clone().unwrap();

        write_and_read(
            MonoioFile::from(File::from_std(write).unwrap()),
            MonoioFile::from(File::from_std(read).unwrap()),
        )
        .await;
    }

    #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
    #[test]
    fn test_tokio_uring() {
        use tempfile::tempfile;
        use tokio_uring::fs::File;

        use crate::disk::tokio_uring::TokioUringFile;

        tokio_uring::start(async {
            let read = tempfile().unwrap();
            let write = read.try_clone().unwrap();

            write_and_read(
                TokioUringFile::from(File::from_std(write)),
                TokioUringFile::from(File::from_std(read)),
            )
            .await;
        });
    }
}

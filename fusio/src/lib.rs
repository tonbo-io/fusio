mod buf;
#[cfg(feature = "dyn")]
pub mod dynamic;
mod error;
#[cfg(feature = "fs")]
pub mod fs;
pub mod local;
pub mod path;
pub mod remotes;

use std::future::Future;

pub use buf::{IoBuf, IoBufMut};
#[cfg(feature = "dyn")]
pub use dynamic::{DynFs, DynRead, DynWrite};
pub use error::Error;
use path::Path;

#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSend: Send {}

#[cfg(feature = "no-send")]
pub unsafe trait MaybeSend {}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Send> MaybeSend for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSend for T {}

#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSync: Sync {}

#[cfg(feature = "no-send")]
pub unsafe trait MaybeSync {}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Sync> MaybeSync for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSync for T {}

pub trait Write: MaybeSend + MaybeSync {
    fn write<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<usize, Error>, B)> + MaybeSend;

    fn sync_data(&self) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn sync_all(&self) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

pub struct FileMeta {
    pub path: Path,
    pub size: u64,
}

pub trait Read: MaybeSend + MaybeSync {
    fn read(
        &mut self,
        len: Option<u64>,
    ) -> impl Future<Output = Result<impl IoBuf, Error>> + MaybeSend;

    fn metadata(&self) -> impl Future<Output = Result<FileMeta, Error>> + MaybeSend;
}

pub trait Seek: MaybeSend {
    fn seek(&mut self, pos: u64) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

#[cfg(test)]
mod tests {
    use super::{Read, Write};
    #[cfg(feature = "dyn")]
    use crate::dynamic::{DynRead, DynWrite};
    use crate::{dynamic::DynSeek, Error, IoBuf, Seek};

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
        async fn write<B: IoBuf>(&mut self, buf: B) -> (Result<usize, Error>, B) {
            let (result, buf) = self.w.write(buf).await;
            (result.inspect(|i| self.cnt += *i), buf)
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
        async fn read(&mut self, len: Option<u64>) -> Result<impl IoBuf, Error> {
            self.r
                .read(len)
                .await
                .inspect(|buf| self.cnt += buf.bytes_init())
        }

        async fn metadata(&self) -> Result<crate::FileMeta, Error> {
            self.r.metadata().await
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
        #[cfg(feature = "dyn")]
        let mut writer = Box::new(CountWrite::new(write)) as Box<dyn DynWrite>;
        #[cfg(not(feature = "dyn"))]
        let mut writer = CountWrite::new(write);

        #[cfg(feature = "dyn")]
        writer
            .write(bytes::Bytes::from(&[2, 0, 2, 4][..]))
            .await
            .0
            .unwrap();
        #[cfg(not(feature = "dyn"))]
        writer.write(&[2, 0, 2, 4][..]).await.0.unwrap();

        writer.sync_data().await.unwrap();

        trait ReadSeek: DynRead + DynSeek {}

        impl<R: Read + Seek> ReadSeek for R {}

        #[cfg(feature = "dyn")]
        let mut reader = Box::new(CountRead::new(read)) as Box<dyn ReadSeek>;
        #[cfg(not(feature = "dyn"))]
        let mut reader = CountRead::new(read);

        reader.seek(0).await.unwrap();

        let buf = reader.read(Some(4)).await.unwrap();

        assert_eq!(buf.bytes_init(), 4);
        assert_eq!(buf.as_slice(), &[2, 0, 2, 4]);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio() {
        use tempfile::tempfile;
        use tokio::fs::File;

        use crate::local::tokio::PathFile;

        let read = tempfile().unwrap();
        let write = read.try_clone().unwrap();

        write_and_read(
            PathFile::new("".into(), File::from_std(write)),
            PathFile::new("".into(), File::from_std(read)),
        )
        .await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio() {
        use monoio::fs::File;
        use tempfile::tempfile;

        use crate::local::monoio::MonoioFile;

        let read = tempfile().unwrap();
        let write = read.try_clone().unwrap();

        write_and_read(
            MonoioFile::new("".into(), File::from_std(write).unwrap()),
            MonoioFile::new("".into(), File::from_std(read).unwrap()),
        )
        .await;
    }

    #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
    #[test]
    fn test_tokio_uring() {
        use tempfile::tempfile;
        use tokio_uring::fs::File;

        tokio_uring::start(async {
            let read = tempfile().unwrap();
            let write = read.try_clone().unwrap();

            write_and_read(File::from_std(write), File::from_std(read)).await;
        });
    }
}

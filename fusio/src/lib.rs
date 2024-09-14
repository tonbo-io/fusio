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
pub use dynamic::{DynRead, DynWrite, DynFs};
pub use error::Error;

#[cfg(not(feature = "no-send"))]
pub trait Write: Send {
    fn write<B: IoBuf>(&mut self, buf: B)
        -> impl Future<Output = (Result<usize, Error>, B)> + Send;

    fn sync_data(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    fn sync_all(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + Send;

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
}

#[cfg(feature = "no-send")]
pub trait Write {
    fn write<B: IoBuf>(&mut self, buf: B) -> impl Future<Output = (Result<usize, Error>, B)>;

    fn sync_data(&mut self) -> impl Future<Output = Result<(), Error>>;

    fn sync_all(&mut self) -> impl Future<Output = Result<(), Error>>;

    fn flush(&mut self) -> impl Future<Output = Result<(), Error>>;

    fn close(&mut self) -> impl Future<Output = Result<(), Error>>;
}

#[cfg(not(feature = "no-send"))]
pub trait Read: Send {
    fn read(
        &mut self,
        pos: u64,
        len: Option<u64>,
    ) -> impl Future<Output = Result<impl IoBuf, Error>> + Send;
}

#[cfg(feature = "no-send")]
pub trait Read {
    fn read(
        &mut self,
        pos: u64,
        len: Option<u64>,
    ) -> impl Future<Output = Result<impl IoBuf, Error>>;
}

#[cfg(test)]
mod tests {
    use super::{Read, Write};
    #[cfg(feature = "dyn")]
    use crate::dynamic::{DynRead, DynWrite};
    use crate::{Error, IoBuf};

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

        async fn sync_data(&mut self) -> Result<(), Error> {
            self.w.sync_data().await
        }

        async fn sync_all(&mut self) -> Result<(), Error> {
            self.w.sync_all().await
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
        async fn read(&mut self, pos: u64, len: Option<u64>) -> Result<impl IoBuf, Error> {
            self.r
                .read(pos, len)
                .await
                .inspect(|buf| self.cnt += buf.bytes_init())
        }
    }

    #[allow(unused)]
    async fn write_and_read<W, R>(write: W, read: R)
    where
        W: Write,
        R: Read,
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

        #[cfg(feature = "dyn")]
        let mut reader = Box::new(CountRead::new(read)) as Box<dyn DynRead>;
        #[cfg(not(feature = "dyn"))]
        let mut reader = CountRead::new(read);
        let buf = reader.read(0, Some(4)).await.unwrap();

        assert_eq!(buf.bytes_init(), 4);
        assert_eq!(buf.as_slice(), &[2, 0, 2, 4]);
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

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio() {
        use monoio::fs::File;
        use tempfile::tempfile;

        use crate::local::MonoioFile;

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

        tokio_uring::start(async {
            let read = tempfile().unwrap();
            let write = read.try_clone().unwrap();

            write_and_read(File::from_std(write), File::from_std(read)).await;
        });
    }
}

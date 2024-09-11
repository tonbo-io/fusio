mod buf;
mod error;
mod locals;
#[cfg(feature = "dyn")]
pub mod object;
pub mod remotes;

use std::future::Future;

pub use buf::{IoBuf, IoBufMut};
pub use error::Error;

#[cfg(not(feature = "no-send"))]
pub trait Write: Send + Sync {
    fn write<B: IoBuf>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = (Result<usize, Error>, B)> + Send;

    fn sync_data(&self) -> impl Future<Output = Result<(), Error>> + Send;

    fn sync_all(&self) -> impl Future<Output = Result<(), Error>> + Send;

    fn close(self) -> impl Future<Output = Result<(), Error>> + Send;
}

#[cfg(feature = "no-send")]
pub trait Write {
    fn write<B: IoBuf>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = (Result<usize, Error>, B)>;

    fn sync_data(&self) -> impl Future<Output = Result<(), Error>>;

    fn sync_all(&self) -> impl Future<Output = Result<(), Error>>;

    fn close(self) -> impl Future<Output = Result<(), Error>>;
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
    use crate::object::{DynRead, DynWrite};
    use crate::{Error, IoBuf};

    struct CountWrite<W> {
        cnt: usize,
        w: W,
    }

    impl<W> CountWrite<W> {
        fn new(w: W) -> Self {
            Self { cnt: 0, w }
        }
    }

    impl<W> Write for CountWrite<W>
    where
        W: Write,
    {
        async fn write<B: IoBuf>(&mut self, buf: B, pos: u64) -> (Result<usize, Error>, B) {
            let (result, buf) = self.w.write(buf, pos).await;
            (result.inspect(|i| self.cnt += *i), buf)
        }

        async fn sync_data(&self) -> Result<(), Error> {
            self.w.sync_data().await
        }

        async fn sync_all(&self) -> Result<(), Error> {
            self.w.sync_all().await
        }

        async fn close(self) -> Result<(), Error> {
            self.w.close().await
        }
    }

    struct CountRead<R> {
        cnt: usize,
        r: R,
    }

    impl<R> CountRead<R> {
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
            .write(bytes::Bytes::from(&[2, 0, 2, 4][..]), 0)
            .await
            .0
            .unwrap();
        #[cfg(not(feature = "dyn"))]
        writer.write(&[2, 0, 2, 4][..], 0).await.0.unwrap();

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

        let read = tempfile().unwrap();
        let write = read.try_clone().unwrap();

        write_and_read(
            File::from_std(write).unwrap(),
            File::from_std(read).unwrap(),
        )
        .await;
    }

    #[cfg(all(feature = "tokio-uring", target_os = "linux"))]
    #[test]
    fn test_tokio_uring() {
        use tokio_uring::fs::File;

        tokio_uring::start(async {
            let read = tempfile().unwrap();
            let write = read.try_clone().unwrap();

            write_and_read(File::from_std(write), File::from_std(read)).await;
        });
    }
}

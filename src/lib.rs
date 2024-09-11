mod buf;
mod error;
mod locals;
pub mod remotes;
// #[cfg(feature = "parquet")]
// #[cfg(not(feature = "no-send"))]
// pub mod parquet;

use std::{future::Future, io, pin::Pin};

pub use buf::{IoBuf, IoBufMut};
use bytes::Bytes;
pub use error::Error;

pub type WriteResult<T, B> = Result<T, (io::Error, B)>;

#[cfg(not(feature = "no-send"))]
pub type BoxFuture<'a, O> = Pin<Box<dyn Future<Output = O> + Send + 'a>>;

#[cfg(feature = "no-send")]
pub type BoxFuture<'a, O> = Pin<Box<dyn Future<Output = O> + 'a>>;

#[cfg(not(feature = "no-send"))]
pub trait Write: Send {
    fn write(&mut self, buf: Bytes, pos: u64) -> BoxFuture<(Result<usize, Error>, Bytes)>;

    fn sync_data(&self) -> BoxFuture<Result<(), Error>>;

    fn sync_all(&self) -> BoxFuture<Result<(), Error>>;

    fn close<'s>(self) -> BoxFuture<'s, Result<(), Error>> where Self: 's;
}

#[cfg(feature = "no-send")]
pub trait Write {
    fn write(&mut self, buf: Bytes, pos: u64) -> BoxFuture<(Result<usize, Error>, Bytes)>;

    fn sync_data(&self) -> BoxFuture<Result<(), Error>>;

    fn sync_all(&self) -> BoxFuture<Result<(), Error>>;

    fn close<'s>(mut self) -> BoxFuture<'s, Result<(), Error>> where Self: 's;
}

#[cfg(not(feature = "no-send"))]
pub trait Read: Send {
    fn read(&mut self, pos: u64, len: Option<u64>) -> BoxFuture<Result<Bytes, Error>>;
}

#[cfg(feature = "no-send")]
pub trait Read {
    fn read(&mut self, pos: u64, len: Option<u64>) -> BoxFuture<Result<Bytes, Error>>;
}

#[cfg(test)]
mod tests {
    #![allow(dead_code)]

    use monoio::buf::IoBuf;

    use super::*;

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
        fn write(&mut self, buf: Bytes, pos: u64) -> BoxFuture<(Result<usize, Error>, Bytes)> {
            Box::pin(async move {
                let (result, buf) = self.w.write(buf, pos).await;
                (result.inspect(|i| self.cnt += *i), buf)
            })
        }

        fn sync_data(&self) -> BoxFuture<Result<(), Error>> {
            self.w.sync_data()
        }

        fn sync_all(&self) -> BoxFuture<Result<(), Error>> {
            self.w.sync_all()
        }

        fn close<'s>(self) -> BoxFuture<'s, Result<(), Error>> where Self: 's {
            self.w.close()
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
        fn read(&mut self, pos: u64, len: Option<u64>) -> BoxFuture<Result<Bytes, Error>> {
            Box::pin(async move {
                self.r
                    .read(pos, len)
                    .await
                    .inspect(|buf| self.cnt += buf.bytes_init())
            })
        }
    }

    async fn write_and_read<W, R>(write: W, read: R)
    where
        W: Write,
        R: Read,
    {
        let mut writer = CountWrite::new(write);
        writer
            .write(Bytes::from(vec![2, 0, 2, 4]), 0)
            .await
            .0
            .unwrap();

        writer.sync_data().await.unwrap();

        let mut reader = CountRead::new(read);
        let buf = reader.read(0, Some(4)).await.unwrap();

        assert_eq!(buf.bytes_init(), 4);
        assert_eq!(buf.as_ref(), &[2, 0, 2, 4]);
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

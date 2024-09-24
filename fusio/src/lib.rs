mod buf;
#[cfg(feature = "dyn")]
pub mod dynamic;
mod error;
#[cfg(feature = "fs")]
pub mod fs;
pub mod local;
pub mod path;
pub mod remotes;

use std::{future::Future, io::Cursor};

pub use buf::IoBuf;
use buf::IoBufMut;
#[cfg(all(feature = "dyn", feature = "fs"))]
pub use dynamic::fs::DynFs;
#[cfg(feature = "dyn")]
pub use dynamic::{DynRead, DynWrite};
pub use error::Error;

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
    fn write_all<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend;

    fn sync_data(&self) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn sync_all(&self) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

pub trait Read: MaybeSend + MaybeSync {
    fn read_exact<B: IoBufMut>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = Result<B, Error>> + MaybeSend;

    fn size(&self) -> impl Future<Output = Result<u64, Error>> + MaybeSend;
}

pub trait Seek: MaybeSend {
    fn seek(&mut self, pos: u64) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

impl<T> Read for Cursor<T>
where
    T: AsRef<[u8]> + Unpin + Send + Sync,
{
    async fn read_exact<B: IoBufMut>(&mut self, mut buf: B) -> Result<B, Error> {
        std::io::Read::read_exact(self, buf.as_slice_mut())?;

        return Ok(buf);
    }

    async fn size(&self) -> Result<u64, Error> {
        Ok(self.get_ref().as_ref().len() as u64)
    }
}

impl<T> Seek for Cursor<T>
where
    T: AsRef<[u8]> + MaybeSend,
{
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        std::io::Seek::seek(self, std::io::SeekFrom::Start(pos))
            .map_err(Error::Io)
            .map(|_| ())
    }
}

impl Write for Cursor<&mut Vec<u8>> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let result = std::io::Write::write_all(self, buf.as_slice()).map_err(Error::Io);

        (result, buf)
    }

    async fn sync_data(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<S: Seek> Seek for &mut S {
    fn seek(&mut self, pos: u64) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        S::seek(self, pos)
    }
}

impl<R: Read> Read for &mut R {
    fn read_exact<B: IoBufMut>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = Result<B, Error>> + MaybeSend {
        R::read_exact(self, buf)
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
        async fn read_exact<B: IoBufMut>(&mut self, buf: B) -> Result<B, Error> {
            self.r
                .read_exact(buf)
                .await
                .inspect(|buf| self.cnt += buf.bytes_init())
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
        reader.seek(0).await.unwrap();

        #[cfg(feature = "completion-based")]
        let buf = vec![0; 4];
        #[cfg(feature = "completion-based")]
        let buf = reader.read_exact(buf).await.unwrap();

        #[cfg(not(feature = "completion-based"))]
        let mut buf = [0; 4];
        #[cfg(not(feature = "completion-based"))]
        let buf = reader.read_exact(&mut buf[..]).await.unwrap();

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

        use crate::local::monoio::MonoioFile;

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

use std::{future::Future, pin::Pin};

use bytes::Bytes;

use crate::{Error, FileMeta, IoBuf, MaybeSend, MaybeSync, Read, Seek, Write};

pub trait MaybeSendFuture: Future + MaybeSend {}

impl<F: Future + MaybeSend> MaybeSendFuture for F {}

pub trait DynWrite: MaybeSend + MaybeSync {
    fn write(
        &mut self,
        buf: Bytes,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<usize, Error>, Bytes)> + '_>>;

    fn sync_data(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn sync_all(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;
}

impl<W: Write> DynWrite for W {
    fn write(
        &mut self,
        buf: Bytes,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<usize, Error>, Bytes)> + '_>> {
        Box::pin(W::write(self, buf))
    }

    fn sync_data(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(W::sync_data(self))
    }

    fn sync_all(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(W::sync_all(self))
    }

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(W::close(self))
    }
}

pub trait DynRead: MaybeSend + MaybeSync {
    fn read(
        &mut self,
        len: Option<u64>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Bytes, Error>> + '_>>;

    fn metadata(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<FileMeta, Error>> + '_>>;
}

pub trait DynSeek: MaybeSend {
    fn seek(&mut self, pos: u64) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;
}

impl<R> DynRead for R
where
    R: Read,
{
    fn read(
        &mut self,
        len: Option<u64>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Bytes, Error>> + '_>> {
        Box::pin(async move {
            let buf = R::read(self, len).await?;
            Ok(buf.as_bytes())
        })
    }

    fn metadata(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<FileMeta, Error>> + '_>> {
        Box::pin(R::metadata(self))
    }
}

impl<S> DynSeek for S
where
    S: Seek,
{
    fn seek(&mut self, pos: u64) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(S::seek(self, pos))
    }
}

#[cfg(feature = "fs")]
pub use fs::*;

#[cfg(feature = "fs")]
pub mod fs {
    use std::pin::Pin;

    use futures_core::Stream;

    use super::{DynSeek, MaybeSendFuture};
    use crate::{
        fs::{Fs, OpenOptions},
        path::Path,
        DynRead, DynWrite, Error, FileMeta,
    };

    pub trait DynFile: DynRead + DynSeek + DynWrite + 'static {}

    impl<F> DynFile for F where F: DynRead + DynSeek + DynWrite + 'static {}

    pub trait DynFs {
        fn open<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Box<dyn DynFile>, Error>> + 's>> {
            self.open_options(path, OpenOptions::default())
        }

        fn open_options<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
            options: OpenOptions,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Box<dyn DynFile>, Error>> + 's>>;

        fn create_dir<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + 's>>;

        fn list<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
        ) -> Pin<
            Box<
                dyn MaybeSendFuture<
                        Output = Result<
                            Pin<Box<dyn Stream<Item = Result<FileMeta, Error>> + 's>>,
                            Error,
                        >,
                    > + 's,
            >,
        >;

        fn remove<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + 's>>;
    }

    impl<F: Fs> DynFs for F {
        fn open_options<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
            options: OpenOptions,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Box<dyn DynFile>, Error>> + 's>> {
            Box::pin(async move {
                let file = F::open_options(self, path, options).await?;
                Ok(Box::new(file) as Box<dyn DynFile>)
            })
        }

        fn create_dir<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + 's>> {
            Box::pin(F::create_dir(path))
        }

        fn list<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
        ) -> Pin<
            Box<
                dyn MaybeSendFuture<
                        Output = Result<
                            Pin<Box<dyn Stream<Item = Result<FileMeta, Error>> + 's>>,
                            Error,
                        >,
                    > + 's,
            >,
        > {
            Box::pin(async move {
                let stream = F::list(self, path).await?;
                Ok(Box::pin(stream) as Pin<Box<dyn Stream<Item = Result<FileMeta, Error>>>>)
            })
        }

        fn remove<'s, 'path: 's>(
            &'s self,
            path: &'path Path,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + 's>> {
            Box::pin(F::remove(self, path))
        }
    }
}

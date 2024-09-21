#[cfg(feature = "fs")]
pub mod fs;

use std::{future::Future, pin::Pin};

use bytes::Bytes;

use crate::{Error, FileMeta, IoBuf, MaybeSend, MaybeSync, Read, Seek, Write};
#[cfg(feature = "fs")]
pub use fs::{DynFile, DynFs};

pub struct BoxedFuture<'future, Output> {
    inner: Pin<Box<dyn MaybeSendFuture<Output = Output> + 'future>>,
}

impl<'future, Output> BoxedFuture<'future, Output> {
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = Output> + MaybeSend + 'future,
    {
        Self {
            inner: Box::pin(future),
        }
    }
}

impl<'future, Output> Future for BoxedFuture<'future, Output> {
    type Output = Output;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

pub trait MaybeSendFuture: Future + MaybeSend {}

impl<F> MaybeSendFuture for F where F: Future + MaybeSend {}

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

pub trait DynSeek: MaybeSend {
    fn seek(&mut self, pos: u64) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;
}

impl<S> DynSeek for S
where
    S: Seek,
{
    fn seek(&mut self, pos: u64) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(S::seek(self, pos))
    }
}

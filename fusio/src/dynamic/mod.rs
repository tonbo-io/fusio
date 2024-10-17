#[cfg(feature = "fs")]
pub mod fs;

use std::{future::Future, pin::Pin};

#[cfg(feature = "fs")]
pub use fs::{DynFile, DynFs};

use crate::{
    buf::{Slice, SliceMut},
    Error, MaybeSend, MaybeSync, Read, Seek, Write,
};

pub trait MaybeSendFuture: Future + MaybeSend {}

impl<F> MaybeSendFuture for F where F: Future + MaybeSend {}

pub trait DynWrite: MaybeSend + MaybeSync {
    fn write_all(
        &mut self,
        buf: Slice,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Slice)> + '_>>;

    fn sync_data(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn sync_all(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;
}

impl<W: Write> DynWrite for W {
    fn write_all(
        &mut self,
        buf: Slice,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Slice)> + '_>> {
        Box::pin(W::write_all(self, buf))
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
        buf: SliceMut,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<u64, Error>, SliceMut)> + '_>>;

    fn read_exact(
        &mut self,
        buf: SliceMut,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, SliceMut)> + '_>>;

    fn read_to_end(
        &mut self,
        buf: Vec<u8>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Vec<u8>)> + '_>>;

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, Error>> + '_>>;
}

impl<R> DynRead for R
where
    R: Read,
{
    fn read(
        &mut self,
        buf: SliceMut,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<u64, Error>, SliceMut)> + '_>> {
        Box::pin(async move { R::read(self, buf).await })
    }

    fn read_exact(
        &mut self,
        buf: SliceMut,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, SliceMut)> + '_>> {
        Box::pin(async move { R::read_exact(self, buf).await })
    }

    fn read_to_end(
        &mut self,
        buf: Vec<u8>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Vec<u8>)> + '_>> {
        Box::pin(async move { R::read_to_end(self, buf).await })
    }

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, Error>> + '_>> {
        Box::pin(R::size(self))
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

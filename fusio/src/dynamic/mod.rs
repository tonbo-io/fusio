//! Dyn compatible(object safety) version of [`Read`], [`Write`] and others.

#[cfg(feature = "fs")]
pub mod fs;

use std::{future::Future, pin::Pin};

#[cfg(feature = "fs")]
pub use fs::{DynFile, DynFs};

use crate::{
    buf::{Slice, SliceMut},
    Error, IoBuf, IoBufMut, MaybeSend, MaybeSync, Read, Write,
};

pub trait MaybeSendFuture: Future + MaybeSend {}

impl<F> MaybeSendFuture for F where F: Future + MaybeSend {}

pub trait DynWrite: MaybeSend {
    //! Dyn compatible(object safety) version of [`Write`].
    //! All implementations of [`Write`] has already implemented this trait.
    //! Also, all implementations of [`DynWrite`] has already implemented [`Write`].
    //! User should not use this trait directly.

    fn write_all(
        &mut self,
        buf: Slice,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Slice)> + '_>>;

    fn flush(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;
}

impl<W: Write> DynWrite for W {
    fn write_all(
        &mut self,
        buf: Slice,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Slice)> + '_>> {
        Box::pin(W::write_all(self, buf))
    }

    fn flush(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(W::flush(self))
    }

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(W::close(self))
    }
}

impl<'write> Write for Box<dyn DynWrite + 'write> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (result, buf) =
            DynWrite::write_all(self.as_mut(), unsafe { buf.slice_unchecked(..) }).await;
        (result, unsafe { B::recover_from_slice(buf) })
    }

    async fn flush(&mut self) -> Result<(), Error> {
        DynWrite::flush(self.as_mut()).await
    }

    async fn close(&mut self) -> Result<(), Error> {
        DynWrite::close(self.as_mut()).await
    }
}

pub trait DynRead: MaybeSend + MaybeSync {
    //! Dyn compatible(object safety) version of [`Read`].
    //! Same as [`DynWrite`].

    fn read_exact_at(
        &mut self,
        buf: SliceMut,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, SliceMut)> + '_>>;

    fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Vec<u8>)> + '_>>;

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, Error>> + '_>>;
}

impl<R> DynRead for R
where
    R: Read,
{
    fn read_exact_at(
        &mut self,
        buf: SliceMut,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, SliceMut)> + '_>> {
        Box::pin(async move { R::read_exact_at(self, buf, pos).await })
    }

    fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Vec<u8>)> + '_>> {
        Box::pin(async move { R::read_to_end_at(self, buf, pos).await })
    }

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, Error>> + '_>> {
        Box::pin(R::size(self))
    }
}

impl<'read> Read for Box<dyn DynRead + 'read> {
    async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let (result, buf) =
            DynRead::read_exact_at(self.as_mut(), unsafe { buf.slice_mut_unchecked(..) }, pos)
                .await;
        (result, unsafe { B::recover_from_slice_mut(buf) })
    }

    async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        DynRead::read_to_end_at(self.as_mut(), buf, pos).await
    }

    async fn size(&self) -> Result<u64, Error> {
        DynRead::size(self.as_ref()).await
    }
}

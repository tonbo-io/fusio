//! Dyn compatible(object safety) version of [`Read`], [`Write`] and others.

#[cfg(feature = "fs")]
pub mod fs;

use std::{future::Future, pin::Pin};

#[cfg(feature = "fs")]
pub use fs::{DynFile, DynFs};

use crate::{
    buf::{Slice, SliceMut},
    Error, MaybeSend, MaybeSync, Read, Write,
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

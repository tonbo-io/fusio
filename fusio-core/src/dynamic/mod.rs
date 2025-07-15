use alloc::{boxed::Box, vec::Vec};
use core::pin::Pin;

use crate::{
    buf::slice::{Buf, BufMut},
    error::Error,
    IoBuf, IoBufMut, MaybeSend, MaybeSendFuture, MaybeSync, Read, Write,
};

mod seal {
    pub trait Sealed {}

    impl<T> Sealed for T {}
}

/// Dyn compatible (object safe) version of [`Write`].
///
/// All implementations of [`Write`] automatically implement this trait.
/// Also, all implementations of [`DynWrite`] automatically implement [`Write`].
/// Users should not use this trait directly.
///
/// # Safety
///
/// Do not implement this trait directly. All implementations of [`Write`] automatically
/// implement this trait.
pub unsafe trait DynWrite: MaybeSend + seal::Sealed {
    fn write_all(
        &mut self,
        buf: Buf,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Buf)> + '_>>;

    fn flush(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;
}

unsafe impl<W: Write> DynWrite for W {
    fn write_all(
        &mut self,
        buf: Buf,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Buf)> + '_>> {
        Box::pin(async move {
            let (result, slice) = W::write_all(self, buf).await;
            (result, slice)
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(async move { W::flush(self).await })
    }

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(async move { W::close(self).await })
    }
}

impl Write for Box<dyn DynWrite + '_> {
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

/// Dyn compatible (object safe) version of [`Read`].
///
/// Similar to [`DynWrite`], all implementations of [`Read`] automatically implement this trait.
/// Users should not use this trait directly.
///
/// # Safety
///
/// Do not implement this trait directly. All implementations of [`Read`] automatically
/// implement this trait.
pub unsafe trait DynRead: MaybeSend + MaybeSync + seal::Sealed {
    fn read_exact_at(
        &mut self,
        buf: BufMut,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, BufMut)> + '_>>;

    fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Vec<u8>)> + '_>>;

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, Error>> + '_>>;
}

unsafe impl<R> DynRead for R
where
    R: Read,
{
    fn read_exact_at(
        &mut self,
        buf: BufMut,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, BufMut)> + '_>> {
        Box::pin(async move {
            let (result, buf) = R::read_exact_at(self, buf, pos).await;
            (result, buf)
        })
    }

    fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Vec<u8>)> + '_>> {
        Box::pin(async move {
            let (result, buf) = R::read_to_end_at(self, buf, pos).await;
            (result, buf)
        })
    }

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, Error>> + '_>> {
        Box::pin(async move { R::size(self).await })
    }
}

impl Read for Box<dyn DynRead + '_> {
    async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let (result, buf) =
            DynRead::read_exact_at(self.as_mut(), unsafe { buf.slice_mut_unchecked(..) }, pos)
                .await;
        (result, unsafe { B::recover_from_slice_mut(buf) })
    }

    async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let (result, buf) = DynRead::read_to_end_at(self.as_mut(), buf, pos).await;
        (result, buf)
    }

    async fn size(&self) -> Result<u64, Error> {
        DynRead::size(self.as_ref()).await
    }
}

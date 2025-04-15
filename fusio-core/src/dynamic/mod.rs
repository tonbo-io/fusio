use alloc::{boxed::Box, vec::Vec};
use core::pin::Pin;

use crate::{
    buf::slice::{Buf, BufMut},
    error::{BoxedError, Error},
    IoBuf, IoBufMut, MaybeSend, MaybeSendFuture, MaybeSync, Read, Write,
};

pub trait DynWrite: MaybeSend {
    //! Dyn compatible(object safety) version of [`Write`].
    //! All implementations of [`Write`] has already implemented this trait.
    //! Also, all implementations of [`DynWrite`] has already implemented [`Write`].
    //! User should not use this trait directly.

    fn write_all(
        &mut self,
        buf: Buf,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), BoxedError>, Buf)> + '_>>;

    fn flush(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), BoxedError>> + '_>>;

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), BoxedError>> + '_>>;
}

impl<W: Write> DynWrite for W {
    fn write_all(
        &mut self,
        buf: Buf,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), BoxedError>, Buf)> + '_>> {
        Box::pin(async move {
            let (result, slice) = W::write_all(self, buf).await;
            (result.map_err(Into::into), slice)
        })
    }

    fn flush(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), BoxedError>> + '_>> {
        Box::pin(async move { W::flush(self).await.map_err(Into::into) })
    }

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), BoxedError>> + '_>> {
        Box::pin(async move { W::close(self).await.map_err(Into::into) })
    }
}

impl<'write> Write for Box<dyn DynWrite + 'write> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (result, buf) =
            DynWrite::write_all(self.as_mut(), unsafe { buf.slice_unchecked(..) }).await;
        (result.map_err(Error::Other), unsafe {
            B::recover_from_slice(buf)
        })
    }

    async fn flush(&mut self) -> Result<(), Error> {
        DynWrite::flush(self.as_mut()).await.map_err(Error::Other)
    }

    async fn close(&mut self) -> Result<(), Error> {
        DynWrite::close(self.as_mut()).await.map_err(Error::Other)
    }
}

pub trait DynRead: MaybeSend + MaybeSync {
    //! Dyn compatible(object safety) version of [`Read`].
    //! Same as [`DynWrite`].

    fn read_exact_at(
        &mut self,
        buf: BufMut,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), BoxedError>, BufMut)> + '_>>;

    fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), BoxedError>, Vec<u8>)> + '_>>;

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, BoxedError>> + '_>>;
}

impl<R> DynRead for R
where
    R: Read,
{
    fn read_exact_at(
        &mut self,
        buf: BufMut,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), BoxedError>, BufMut)> + '_>> {
        Box::pin(async move {
            let (result, buf) = R::read_exact_at(self, buf, pos).await;
            (result.map_err(Into::into), buf)
        })
    }

    fn read_to_end_at(
        &mut self,
        buf: Vec<u8>,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), BoxedError>, Vec<u8>)> + '_>> {
        Box::pin(async move {
            let (result, buf) = R::read_to_end_at(self, buf, pos).await;
            (result.map_err(Into::into), buf)
        })
    }

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, BoxedError>> + '_>> {
        Box::pin(async move { R::size(self).await.map_err(Into::into) })
    }
}

impl<'read> Read for Box<dyn DynRead + 'read> {
    async fn read_exact_at<B: IoBufMut>(&mut self, buf: B, pos: u64) -> (Result<(), Error>, B) {
        let (result, buf) =
            DynRead::read_exact_at(self.as_mut(), unsafe { buf.slice_mut_unchecked(..) }, pos)
                .await;
        (result.map_err(Error::Other), unsafe {
            B::recover_from_slice_mut(buf)
        })
    }

    async fn read_to_end_at(&mut self, buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let (result, buf) = DynRead::read_to_end_at(self.as_mut(), buf, pos).await;
        (result.map_err(Error::Other), buf)
    }

    async fn size(&self) -> Result<u64, Error> {
        DynRead::size(self.as_ref()).await.map_err(Error::Other)
    }
}

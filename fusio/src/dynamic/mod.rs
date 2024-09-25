#[cfg(feature = "fs")]
pub mod fs;

use std::{future::Future, pin::Pin};

#[cfg(feature = "fs")]
pub use fs::{DynFile, DynFs};

use crate::{
    buf::{Buf, BufMut, IoBufMut},
    Error, MaybeSend, MaybeSync, Read, Seek, Write,
};

pub trait MaybeSendFuture: Future + MaybeSend {}

impl<F> MaybeSendFuture for F where F: Future + MaybeSend {}

pub trait DynWrite: MaybeSend + MaybeSync {
    fn write_all(
        &mut self,
        buf: Buf,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Buf)> + '_>>;

    fn sync_data(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn sync_all(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn close(&mut self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;
}

impl<W: Write> DynWrite for W {
    fn write_all(
        &mut self,
        buf: Buf,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<(), Error>, Buf)> + '_>> {
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
    fn read_exact(
        &mut self,
        buf: BufMut,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<BufMut, Error>> + '_>>;

    fn read_to_end(
        &mut self,
        mut buf: Vec<u8>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<u8>, Error>> + '_>> {
        Box::pin(async move {
            buf.resize(self.size().await? as usize, 0);
            let buf = self.read_exact(unsafe { buf.to_buf_mut_nocopy() }).await?;
            Ok(unsafe { Vec::recover_from_buf_mut(buf) })
        })
    }

    fn size(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<u64, Error>> + '_>>;
}

impl<R> DynRead for R
where
    R: Read,
{
    fn read_exact(
        &mut self,
        buf: BufMut,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<BufMut, Error>> + '_>> {
        Box::pin(async move {
            let buf = R::read_exact(self, buf).await?;
            Ok(buf)
        })
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

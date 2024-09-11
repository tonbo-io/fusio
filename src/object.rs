use std::{future::Future, pin::Pin};

use bytes::Bytes;

use crate::{Error, IoBuf, Read, Write};

/// # Safety
/// You should not implement this trait manually.
#[cfg(not(feature = "no-send"))]
pub unsafe trait MaybeSend: Send {}

/// # Safety
/// You should not implement this trait manually.
#[cfg(feature = "no-send")]
pub unsafe trait MaybeSend {}

#[cfg(not(feature = "no-send"))]
unsafe impl<T: Send> MaybeSend for T {}
#[cfg(feature = "no-send")]
unsafe impl<T> MaybeSend for T {}

pub trait MaybeSendFuture: Future + MaybeSend {}

impl<F: Future + MaybeSend> MaybeSendFuture for F {}

pub trait DynWrite {
    fn write(
        &mut self,
        buf: Bytes,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<usize, Error>, Bytes)> + '_>>;

    fn sync_data(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn sync_all(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>>;

    fn close<'s>(self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + 's>>
    where
        Self: 's;
}

impl<W: Write> DynWrite for W {
    fn write(
        &mut self,
        buf: Bytes,
        pos: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = (Result<usize, Error>, Bytes)> + '_>> {
        Box::pin(W::write(self, buf, pos))
    }

    fn sync_data(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(W::sync_data(self))
    }

    fn sync_all(&self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + '_>> {
        Box::pin(W::sync_all(self))
    }

    fn close<'s>(self) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), Error>> + 's>>
    where
        Self: 's,
    {
        Box::pin(W::close(self))
    }
}

pub trait DynRead {
    fn read(
        &mut self,
        pos: u64,
        len: Option<u64>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Bytes, Error>> + '_>>;
}

impl<R> DynRead for R
where
    R: Read,
{
    fn read(
        &mut self,
        pos: u64,
        len: Option<u64>,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Bytes, Error>> + '_>> {
        Box::pin(async move {
            let buf = R::read(self, pos, len).await?;
            Ok(buf.as_bytes())
        })
    }
}

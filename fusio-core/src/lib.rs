#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod buf;
#[cfg(feature = "alloc")]
mod dynamic;
mod maybe;

use core::future::Future;

pub use buf::{IoBuf, IoBufMut};
#[cfg(feature = "alloc")]
pub use dynamic::{
    error::{BoxedError, Error},
    DynRead, DynWrite,
};
pub use maybe::{MaybeOwned, MaybeSend, MaybeSendFuture, MaybeSync};

pub trait Write: MaybeSend {
    //! The core trait for writing data,
    //! it is similar to [`std::io::Write`], but it takes the ownership of the buffer,
    //! because completion-based IO requires the buffer to be pinned and should be safe to
    //! cancellation.
    //!
    //! [`Write`] represents "sequential write all and overwrite" semantics,
    //! which means each buffer will be written to the file sequentially and overwrite the previous
    //! file when closed.
    //!
    //! Contents are not be garanteed to be written to the file until the [`Write::close`] method is
    //! called, [`Write::flush`] may be used to flush the data to the file in some
    //! implementations, but not all implementations will do so.
    //!
    //! Whether the operation is successful or not, the buffer will be returned,
    //! fusio promises that the returned buffer will be the same as the input buffer.
    //!
    //! # Dyn Compatibility
    //! This trait is not dyn compatible.
    //! If you want to use [`Write`] trait in a dynamic way, you could use [`DynWrite`] trait.

    type Error: core::error::Error + Send + Sync + 'static;

    fn write_all<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Self::Error>, B)> + MaybeSend;

    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend;
}

pub trait Read: MaybeSend + MaybeSync {
    //! The core trait for reading data,
    //! it is similar to [`std::io::Read`],
    //! but it takes the ownership of the buffer,
    //! because completion-based IO requires the buffer to be pinned and should be safe to
    //! cancellation.
    //!
    //! [`Read`] represents "random exactly read" semantics,
    //! which means the read operation will start at the specified position, and the buffer will be
    //! exactly filled with the data read.
    //!
    //! The buffer will be returned with the result, whether the operation is successful or not,
    //! fusio promises that the returned buffer will be the same as the input buffer.
    //!
    //! If you want sequential reading, try [`SeqRead`].
    //!
    //! # Dyn Compatibility
    //! This trait is not dyn compatible.
    //! If you want to use [`Read`] trait in a dynamic way, you could use [`DynRead`] trait.

    type Error: core::error::Error + Send + Sync + 'static;

    fn read_exact_at<B: IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Self::Error>, B)> + MaybeSend;

    #[cfg(feature = "alloc")]
    fn read_to_end_at(
        &mut self,
        buf: alloc::vec::Vec<u8>,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Self::Error>, alloc::vec::Vec<u8>)> + MaybeSend;

    fn size(&self) -> impl Future<Output = Result<u64, Self::Error>> + MaybeSend;
}

impl<R: Read> Read for &mut R {
    type Error = R::Error;

    fn read_exact_at<B: IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Self::Error>, B)> + MaybeSend {
        R::read_exact_at(self, buf, pos)
    }

    #[cfg(feature = "alloc")]
    fn read_to_end_at(
        &mut self,
        buf: alloc::vec::Vec<u8>,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Self::Error>, alloc::vec::Vec<u8>)> + MaybeSend {
        R::read_to_end_at(self, buf, pos)
    }

    fn size(&self) -> impl Future<Output = Result<u64, Self::Error>> + MaybeSend {
        R::size(self)
    }
}

impl<W: Write> Write for &mut W {
    type Error = W::Error;

    fn write_all<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Self::Error>, B)> + MaybeSend {
        W::write_all(self, buf)
    }

    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        W::flush(self)
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + MaybeSend {
        W::close(self)
    }
}

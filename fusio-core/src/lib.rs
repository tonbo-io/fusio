#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod buf;
#[cfg(feature = "alloc")]
#[cfg_attr(docsrs, doc(cfg(feature = "alloc")))]
mod dynamic;
pub mod error;
mod maybe;

use core::future::Future;

pub use buf::{IoBuf, IoBufMut};
#[cfg(feature = "alloc")]
#[cfg_attr(docsrs, doc(cfg(feature = "alloc")))]
pub use dynamic::{DynRead, DynWrite};
use error::Error;
pub use maybe::{MaybeOwned, MaybeSend, MaybeSendFuture, MaybeSync};

/// The core trait for writing data.
///
/// It is similar to [`std::io::Write`], but it takes ownership of the buffer,
/// because completion-based IO requires the buffer to be pinned and should be safe to
/// cancellation.
///
/// [`Write`] represents "sequential write all and overwrite" semantics,
/// which means each buffer will be written to the file sequentially and overwrite the previous
/// file when closed.
///
/// Contents are not guaranteed to be written to the file until the [`Write::close`] method is
/// called. [`Write::flush`] may be used to flush the data to the file in some
/// implementations, but not all implementations will do so.
///
/// Whether the operation is successful or not, the buffer will be returned.
/// Fusio promises that the returned buffer will be the same as the input buffer.
///
/// # Examples
///
/// ```no_run
/// use fusio_core::{IoBuf, Write};
///
/// async fn write_data<W: Write>(
///     mut writer: W,
///     data: Vec<u8>,
/// ) -> Result<(), fusio_core::error::Error> {
///     let (result, _buf) = writer.write_all(data).await;
///     result?;
///     writer.flush().await?;
///     writer.close().await?;
///     Ok(())
/// }
/// ```
///
/// # Dyn Compatibility
/// This trait is not dyn compatible.
/// If you want to use [`Write`] trait in a dynamic way, you could use [`DynWrite`] trait.
pub trait Write: MaybeSend {
    fn write_all<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend;

    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend;
}

/// The core trait for reading data.
///
/// It is similar to [`std::io::Read`], but it takes ownership of the buffer,
/// because completion-based IO requires the buffer to be pinned and should be safe to
/// cancellation.
///
/// [`Read`] represents "random exactly read" semantics,
/// which means the read operation will start at the specified position, and the buffer will be
/// exactly filled with the data read.
///
/// The buffer will be returned with the result, whether the operation is successful or not.
/// Fusio promises that the returned buffer will be the same as the input buffer.
///
/// If you want sequential reading, try `SeqRead` (not yet implemented).
///
/// # Examples
///
/// ```no_run
/// use fusio_core::{IoBufMut, Read};
///
/// async fn read_at_position<R: Read>(
///     mut reader: R,
///     pos: u64,
///     len: usize,
/// ) -> Result<Vec<u8>, fusio_core::error::Error> {
///     let mut buf = vec![0u8; len];
///     let (result, buf) = reader.read_exact_at(buf, pos).await;
///     result?;
///     Ok(buf)
/// }
/// ```
///
/// # Dyn Compatibility
/// This trait is not dyn compatible.
/// If you want to use [`Read`] trait in a dynamic way, you could use [`DynRead`] trait.
pub trait Read: MaybeSend + MaybeSync {
    fn read_exact_at<B: IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend;

    #[cfg(feature = "alloc")]
    #[cfg_attr(docsrs, doc(cfg(feature = "alloc")))]
    fn read_to_end_at(
        &mut self,
        buf: alloc::vec::Vec<u8>,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Error>, alloc::vec::Vec<u8>)> + MaybeSend;

    fn size(&self) -> impl Future<Output = Result<u64, Error>> + MaybeSend;
}

impl<R: Read> Read for &mut R {
    fn read_exact_at<B: IoBufMut>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend {
        R::read_exact_at(self, buf, pos)
    }

    #[cfg(feature = "alloc")]
    fn read_to_end_at(
        &mut self,
        buf: alloc::vec::Vec<u8>,
        pos: u64,
    ) -> impl Future<Output = (Result<(), Error>, alloc::vec::Vec<u8>)> + MaybeSend {
        R::read_to_end_at(self, buf, pos)
    }

    fn size(&self) -> impl Future<Output = Result<u64, Error>> + MaybeSend {
        R::size(self)
    }
}

impl<W: Write> Write for &mut W {
    fn write_all<B: IoBuf>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend {
        W::write_all(self, buf)
    }

    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        W::flush(self)
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        W::close(self)
    }
}

#[cfg(feature = "std")]
impl Read for &mut Vec<u8> {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let pos = pos as usize;
        let len = buf.bytes_init();
        let end = pos + len;
        if end > self.len() {
            return (
                Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "").into()),
                buf,
            );
        }
        buf.as_slice_mut().copy_from_slice(&self[pos..end]);
        (Ok(()), buf)
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        let pos = pos as usize;
        buf.extend_from_slice(&self[pos..]);
        (Ok(()), buf)
    }

    async fn size(&self) -> Result<u64, Error> {
        Ok(self.len() as u64)
    }
}

#[cfg(feature = "std")]
impl Write for std::io::Cursor<&mut Vec<u8>> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        (
            std::io::Write::write_all(self, buf.as_slice()).map_err(Error::Io),
            buf,
        )
    }

    async fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

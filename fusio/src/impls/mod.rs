//! Implementations of the traits in the `fusio` crate.

pub mod buffered;
pub mod disk;
pub mod remotes;

use std::{future::Future, io::Cursor};

use crate::{IoBufMut, MaybeSend, Read};

pub trait SeqRead: MaybeSend {
    fn read_exact<B: IoBufMut>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), fusio_core::Error>, B)> + MaybeSend;
}

impl<R: SeqRead> SeqRead for &mut R {
    fn read_exact<B: IoBufMut>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), fusio_core::Error>, B)> + MaybeSend {
        R::read_exact(self, buf)
    }
}

impl<R: Read> SeqRead for Cursor<R> {
    async fn read_exact<B: IoBufMut>(&mut self, buf: B) -> (Result<(), fusio_core::Error>, B) {
        let pos = self.position();
        let result = self.get_mut().read_exact_at(buf, pos).await;
        self.set_position(pos + result.1.bytes_init() as u64);
        result
    }
}

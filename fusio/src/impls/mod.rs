pub mod disk;
pub mod remotes;

use std::{
    future::Future,
    io::{self, Cursor},
};

use crate::{Error, IoBuf, IoBufMut, MaybeSend, Read, Write};

impl Read for &mut Vec<u8> {
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        let pos = pos as usize;
        let len = buf.bytes_init();
        let end = pos + len;
        if end > self.len() {
            return (
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "").into()),
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

impl Write for Cursor<&mut Vec<u8>> {
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

pub trait SeqRead: MaybeSend {
    fn read_exact<B: IoBufMut>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend;
}

impl<R: SeqRead> SeqRead for &mut R {
    fn read_exact<B: IoBufMut>(
        &mut self,
        buf: B,
    ) -> impl Future<Output = (Result<(), Error>, B)> + MaybeSend {
        R::read_exact(self, buf)
    }
}

impl<R: Read> SeqRead for Cursor<R> {
    async fn read_exact<B: IoBufMut>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let pos = self.position();
        let result = self.get_mut().read_exact_at(buf, pos).await;
        self.set_position(pos + result.1.bytes_init() as u64);
        result
    }
}

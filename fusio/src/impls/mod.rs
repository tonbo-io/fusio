pub mod disk;
pub mod remotes;

use std::io::Cursor;

use crate::{Error, IoBuf, IoBufMut, Read, Write};

impl<T> Read for Cursor<T>
where
    T: AsRef<[u8]> + Unpin + Send + Sync,
{
    async fn read_exact_at<B: IoBufMut>(&mut self, mut buf: B, pos: u64) -> (Result<(), Error>, B) {
        self.set_position(pos);
        (
            std::io::Read::read_exact(self, buf.as_slice_mut()).map_err(Error::Io),
            buf,
        )
    }

    async fn read_to_end_at(&mut self, mut buf: Vec<u8>, pos: u64) -> (Result<(), Error>, Vec<u8>) {
        self.set_position(pos);
        match std::io::Read::read_to_end(self, &mut buf) {
            Ok(n) => {
                buf.resize(n, 0);
                (Ok(()), buf)
            }
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn size(&mut self) -> Result<u64, Error> {
        Ok(self.get_ref().as_ref().len() as u64)
    }
}

impl Write for Cursor<&mut Vec<u8>> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        (
            std::io::Write::write_all(self, buf.as_slice()).map_err(Error::Io),
            buf,
        )
    }

    async fn complete(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

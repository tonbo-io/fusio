pub mod disk;
pub mod remotes;

use std::io::Cursor;

use crate::{Error, IoBuf, IoBufMut, MaybeSend, Read, Seek, Write};

impl<T> Read for Cursor<T>
where
    T: AsRef<[u8]> + Unpin + Send + Sync,
{
    async fn read<B: IoBufMut>(&mut self, mut buf: B) -> (Result<u64, Error>, B) {
        match std::io::Read::read(self, buf.as_slice_mut()) {
            Ok(n) => (Ok(n as u64), buf),
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn read_to_end(&mut self, mut buf: Vec<u8>) -> (Result<(), Error>, Vec<u8>) {
        match std::io::Read::read_to_end(self, &mut buf) {
            Ok(n) => {
                buf.resize(n, 0);
                (Ok(()), buf)
            }
            Err(e) => (Err(Error::Io(e)), buf),
        }
    }

    async fn size(&self) -> Result<u64, Error> {
        Ok(self.get_ref().as_ref().len() as u64)
    }
}

impl<T> Seek for Cursor<T>
where
    T: AsRef<[u8]> + MaybeSend,
{
    async fn seek(&mut self, pos: u64) -> Result<(), Error> {
        std::io::Seek::seek(self, std::io::SeekFrom::Start(pos))
            .map_err(Error::Io)
            .map(|_| ())
    }
}

impl Write for Cursor<&mut Vec<u8>> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let result = std::io::Write::write_all(self, buf.as_slice()).map_err(Error::Io);

        (result, buf)
    }

    async fn sync_data(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn sync_all(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

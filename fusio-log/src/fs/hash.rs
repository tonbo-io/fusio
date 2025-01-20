use std::{future::Future, hash::Hasher};

use fusio::{Error, IoBuf, IoBufMut, MaybeSend, SeqRead, Write};

use crate::{
    error::LogError,
    serdes::{Decode, Encode},
};

pub(crate) struct HashWriter<W: Write> {
    hasher: crc32fast::Hasher,
    writer: W,
}

impl<W: Write + Unpin> HashWriter<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
            writer,
        }
    }

    pub(crate) async fn eol(mut self) -> Result<(), fusio::Error> {
        let i = self.hasher.finalize();
        i.encode(&mut self.writer).await
    }
}

impl<W: Write> Write for HashWriter<W> {
    async fn write_all<B: IoBuf>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (result, buf) = self.writer.write_all(buf).await;
        self.hasher.write(buf.as_slice());

        (result, buf)
    }

    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + MaybeSend {
        self.writer.flush()
    }

    fn close(&mut self) -> impl Future<Output = Result<(), fusio::Error>> + MaybeSend {
        self.writer.close()
    }
}

pub(crate) struct HashReader<R: SeqRead> {
    hasher: crc32fast::Hasher,
    reader: R,
    pos: u64,
}

impl<R: SeqRead> HashReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
            reader,
            pos: 0,
        }
    }

    pub(crate) async fn checksum(mut self) -> Result<bool, LogError> {
        let checksum = u32::decode(&mut self.reader).await?;

        self.pos += size_of::<u32>() as u64;
        Ok(self.hasher.finalize() == checksum)
    }

    pub(crate) fn position(&self) -> u64 {
        self.pos
    }
}

impl<R: SeqRead> SeqRead for HashReader<R> {
    async fn read_exact<B: IoBufMut>(&mut self, buf: B) -> (Result<(), Error>, B) {
        let (result, buf) = self.reader.read_exact(buf).await;
        self.pos += buf.bytes_init() as u64;
        if result.is_ok() {
            self.hasher.write(buf.as_slice());
        }
        (result, buf)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncSeekExt;

    use crate::{
        fs::hash::{HashReader, HashWriter},
        serdes::{Decode, Encode},
    };

    #[tokio::test]
    async fn test_encode_decode() {
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        let mut writer = HashWriter::new(&mut cursor);
        4_u64.encode(&mut writer).await.unwrap();
        3_u32.encode(&mut writer).await.unwrap();
        2_u16.encode(&mut writer).await.unwrap();
        1_u8.encode(&mut writer).await.unwrap();
        writer.eol().await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let mut reader = HashReader::new(&mut cursor);
        assert_eq!(u64::decode(&mut reader).await.unwrap(), 4);
        assert_eq!(u32::decode(&mut reader).await.unwrap(), 3);
        assert_eq!(u16::decode(&mut reader).await.unwrap(), 2);
        assert_eq!(u8::decode(&mut reader).await.unwrap(), 1);
        assert!(reader.checksum().await.unwrap());
    }
}

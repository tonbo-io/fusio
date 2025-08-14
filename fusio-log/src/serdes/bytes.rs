use bytes::Bytes;
use fusio::{Error, IoBuf, SeqRead, Write};

use crate::serdes::{Decode, Encode};

impl Encode for &[u8] {
    async fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        (self.len() as u32).encode(writer).await?;
        #[cfg(feature = "monoio")]
        let (result, _) = writer.write_all(self.to_vec()).await;
        #[cfg(not(feature = "monoio"))]
        let (result, _) = writer.write_all(*self).await;
        result?;

        Ok(())
    }

    fn size(&self) -> usize {
        self.len() + 4
    }
}

impl Encode for Bytes {
    async fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        (self.len() as u32).encode(writer).await?;
        #[cfg(feature = "monoio")]
        let (result, _) = writer.write_all(self.as_bytes()).await;
        #[cfg(not(feature = "monoio"))]
        let (result, _) = writer.write_all(self.as_slice()).await;
        result?;

        Ok(())
    }

    fn size(&self) -> usize {
        self.len() + 4
    }
}

impl Decode for Bytes {
    async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, Error> {
        let len = u32::decode(reader).await?;
        let (result, buf) = reader.read_exact(vec![0u8; len as usize]).await;
        result?;

        Ok(buf.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;
    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    async fn encode_decode_bytes() {
        let source = Bytes::from_static(b"hello! Tonbo");

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        let before = cursor.position();
        source.encode(&mut cursor).await.unwrap();
        let after = cursor.position();

        assert_eq!(source.size(), (after - before) as usize);

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = Bytes::decode(&mut cursor).await.unwrap();

        assert_eq!(source, decoded);
    }

    async fn encode_u8_slice_decode_bytes() {
        let source = b"hello! Tonbo".as_slice();

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        let before = cursor.position();
        source.encode(&mut cursor).await.unwrap();
        let after = cursor.position();

        assert_eq!(source.size(), (after - before) as usize);

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = Bytes::decode(&mut cursor).await.unwrap();

        assert_eq!(source, decoded);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_encode_decode_bytes() {
        encode_decode_bytes().await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_encode_decode() {
        encode_decode_bytes().await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_encode_decode_u8_slice() {
        encode_u8_slice_decode_bytes().await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_encode_decode_u8_slice() {
        encode_u8_slice_decode_bytes().await;
    }
}

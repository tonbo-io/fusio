use fusio::{Error, SeqRead, Write};

use super::{Decode, Encode};

impl<V> Encode for Option<V>
where
    V: Encode + Sync,
{
    async fn encode<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: Write,
    {
        match self {
            None => 0u8.encode(writer).await?,
            Some(v) => {
                1u8.encode(writer).await?;
                v.encode(writer).await?;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        match self {
            None => 1,
            Some(v) => 1 + v.size(),
        }
    }
}

impl<V> Decode for Option<V>
where
    V: Decode,
{
    async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, Error> {
        match u8::decode(reader).await? {
            0 => Ok(None),
            1 => Ok(Some(V::decode(reader).await?)),
            _ => panic!("invalid option tag"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_encode_decode() {
        let source_0 = Some(1u64);
        let source_1 = None;
        let source_2 = Some("Hello! Tonbo".to_string());

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor).await.unwrap();
        source_1.encode(&mut cursor).await.unwrap();
        source_2.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded_0 = Option::<u64>::decode(&mut cursor).await.unwrap();
        let decoded_1 = Option::<u64>::decode(&mut cursor).await.unwrap();
        let decoded_2 = Option::<String>::decode(&mut cursor).await.unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
        assert_eq!(source_2, decoded_2);
    }
}

use fusio::{SeqRead, Write};

use super::{Decode, Encode};

impl<T> Decode for Vec<T>
where
    T: Decode + Send + Sync,
    fusio::Error: From<<T as Decode>::Error>,
{
    type Error = fusio::Error;

    async fn decode<R>(reader: &mut R) -> Result<Self, Self::Error>
    where
        R: SeqRead,
    {
        let len = u32::decode(reader).await? as usize;
        let mut data = Vec::with_capacity(len);
        for _ in 0..len {
            data.push(T::decode(reader).await?);
        }
        Ok(data)
    }
}

impl<T> Encode for Vec<T>
where
    T: Encode + Send + Sync,
    fusio::Error: From<<T as Encode>::Error>,
{
    type Error = fusio::Error;

    async fn encode<W>(&self, writer: &mut W) -> Result<(), Self::Error>
    where
        W: Write,
    {
        (self.len() as u32).encode(writer).await?;
        for item in self.iter() {
            item.encode(writer).await?;
        }

        Ok(())
    }

    fn size(&self) -> usize {
        self.iter()
            .fold(size_of::<u32>(), |acc, item| acc + item.size())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::AsyncSeekExt;

    use crate::serdes::{Decode, Encode};

    #[tokio::test]
    async fn test_u8_encode_decode() {
        let source = b"hello! Tonbo".to_vec();

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = Vec::<u8>::decode(&mut cursor).await.unwrap();

        assert_eq!(source, decoded);
    }

    #[tokio::test]
    async fn test_num_encode_decode() {
        {
            let source = vec![1_u32, 1237654, 456, 123456789];

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            source.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Vec::<u32>::decode(&mut cursor).await.unwrap();

            assert_eq!(source, decoded);
        }
        {
            let source = vec![1_i64, 1237654, 456, 123456789];

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            source.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Vec::<i64>::decode(&mut cursor).await.unwrap();

            assert_eq!(source, decoded);
        }
    }

    #[tokio::test]
    async fn test_bool_encode_decode() {
        let source = vec![true, false, false, true];

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = Vec::<bool>::decode(&mut cursor).await.unwrap();

        assert_eq!(source, decoded);
    }

    #[tokio::test]
    async fn test_string_encode_decode() {
        {
            let source = vec!["hello", "", "tonbo", "fusio", "!! @tonbo.io"];

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            source.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Vec::<String>::decode(&mut cursor).await.unwrap();

            assert_eq!(source, decoded);
        }

        {
            let source = vec![
                "hello".to_string(),
                "".to_string(),
                "tonbo".to_string(),
                "fusio".to_string(),
                "!! @tonbo.io".to_string(),
            ];

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            source.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Vec::<String>::decode(&mut cursor).await.unwrap();

            assert_eq!(source, decoded);
        }
    }

    #[tokio::test]
    async fn test_encode_decode_empty() {
        {
            let source = Vec::<u16>::new();

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            source.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Vec::<u16>::decode(&mut cursor).await.unwrap();

            assert_eq!(source, decoded);
        }

        {
            let source = Vec::<i32>::new();

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            source.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Vec::<i32>::decode(&mut cursor).await.unwrap();

            assert_eq!(source, decoded);
        }

        {
            let source = Vec::<String>::new();

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            source.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
            let decoded = Vec::<String>::decode(&mut cursor).await.unwrap();

            assert_eq!(source, decoded);
        }
    }
}

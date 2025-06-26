mod arc;
mod boolean;
#[cfg(feature = "bytes")]
mod bytes;
mod list;
mod num;
pub(crate) mod option;
mod string;

use std::future::Future;

use crate::fs::{MaybeSend, SeqRead, Write};

pub trait Encode {
    fn encode<W>(
        &self,
        writer: &mut W,
    ) -> impl Future<Output = Result<(), fusio::Error>> + MaybeSend
    where
        W: Write;

    fn size(&self) -> usize;
}

impl<T: Encode + Sync> Encode for &T {
    async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
    where
        W: Write,
    {
        Encode::encode(*self, writer).await
    }

    fn size(&self) -> usize {
        Encode::size(*self)
    }
}

pub trait Decode: Sized {
    fn decode<R>(reader: &mut R) -> impl Future<Output = Result<Self, fusio::Error>> + MaybeSend
    where
        R: SeqRead;
}

#[cfg(test)]
mod tests {
    use std::io;

    use tokio::io::AsyncSeekExt;

    use super::*;

    #[tokio::test]
    async fn test_encode_decode() {
        // Implement a simple struct that implements Encode and Decode
        struct TestStruct(u32);

        impl Encode for TestStruct {
            async fn encode<W>(&self, writer: &mut W) -> Result<(), fusio::Error>
            where
                W: Write,
            {
                self.0.encode(writer).await?;

                Ok(())
            }

            fn size(&self) -> usize {
                std::mem::size_of::<u32>()
            }
        }

        impl Decode for TestStruct {
            async fn decode<R>(reader: &mut R) -> Result<Self, fusio::Error>
            where
                R: SeqRead,
            {
                Ok(TestStruct(u32::decode(reader).await?))
            }
        }

        // Test encoding and decoding
        let original = TestStruct(42);
        let mut buf = Vec::new();
        let mut cursor = io::Cursor::new(&mut buf);
        original.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let decoded = TestStruct::decode(&mut cursor).await.unwrap();

        assert_eq!(original.0, decoded.0);
    }
}

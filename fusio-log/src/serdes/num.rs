use std::mem::size_of;

use fusio::{SeqRead, Write};

use super::{Decode, Encode};

#[macro_export]
macro_rules! implement_encode_decode {
    ($struct_name:ident) => {
        impl Encode for $struct_name {
            type Error = fusio::Error;

            async fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
                #[cfg(feature = "monoio")]
                let (result, _) = writer.write_all(self.to_le_bytes().to_vec()).await;
                #[cfg(not(feature = "monoio"))]
                let (result, _) = writer.write_all(&self.to_le_bytes()[..]).await;
                result?;

                Ok(())
            }

            fn size(&self) -> usize {
                size_of::<Self>()
            }
        }

        impl Decode for $struct_name {
            type Error = fusio::Error;

            async fn decode<R: SeqRead>(reader: &mut R) -> Result<Self, Self::Error> {
                #[cfg(feature = "monoio")]
                let data = {
                    let (result, buf) = reader.read_exact(vec![0u8; size_of::<Self>()]).await;
                    result?;
                    Self::from_le_bytes(buf.try_into().unwrap())
                };
                #[cfg(not(feature = "monoio"))]
                let data = {
                    let mut bytes = [0u8; size_of::<Self>()];
                    let (result, _) = reader.read_exact(&mut bytes[..]).await;
                    result?;
                    Self::from_le_bytes(bytes)
                };

                Ok(data)
            }
        }
    };
}

implement_encode_decode!(i8);
implement_encode_decode!(i16);
implement_encode_decode!(i32);
implement_encode_decode!(i64);
implement_encode_decode!(u8);
implement_encode_decode!(u16);
implement_encode_decode!(u32);
implement_encode_decode!(u64);

implement_encode_decode!(f32);
implement_encode_decode!(f64);

#[cfg(test)]
mod tests {
    use core::{f32, f64};
    use std::io::{Cursor, Seek};

    use crate::serdes::{Decode, Encode};

    async fn encode_decode() {
        let source_0 = 8u8;
        let source_1 = 16u16;
        let source_2 = 32u32;
        let source_3 = 64u64;
        let source_4 = 8i8;
        let source_5 = 16i16;
        let source_6 = 32i32;
        let source_7 = 64i64;

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor).await.unwrap();
        source_1.encode(&mut cursor).await.unwrap();
        source_2.encode(&mut cursor).await.unwrap();
        source_3.encode(&mut cursor).await.unwrap();
        source_4.encode(&mut cursor).await.unwrap();
        source_5.encode(&mut cursor).await.unwrap();
        source_6.encode(&mut cursor).await.unwrap();
        source_7.encode(&mut cursor).await.unwrap();

        cursor.seek(std::io::SeekFrom::Start(0)).unwrap();
        let decoded_0 = u8::decode(&mut cursor).await.unwrap();
        let decoded_1 = u16::decode(&mut cursor).await.unwrap();
        let decoded_2 = u32::decode(&mut cursor).await.unwrap();
        let decoded_3 = u64::decode(&mut cursor).await.unwrap();
        let decoded_4 = i8::decode(&mut cursor).await.unwrap();
        let decoded_5 = i16::decode(&mut cursor).await.unwrap();
        let decoded_6 = i32::decode(&mut cursor).await.unwrap();
        let decoded_7 = i64::decode(&mut cursor).await.unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
        assert_eq!(source_2, decoded_2);
        assert_eq!(source_3, decoded_3);
        assert_eq!(source_4, decoded_4);
        assert_eq!(source_5, decoded_5);
        assert_eq!(source_6, decoded_6);
        assert_eq!(source_7, decoded_7);
    }

    async fn encode_decode_float() {
        {
            let source_0 = 1.1_f32;
            let source_1 = 1.1_f64;
            let source_2 = 32.0_f32;
            let source_3 = 64.6_f64;
            let source_4 = -1.1_f32;
            let source_5 = -1.1_f64;
            let source_6 = -32.0_f32;
            let source_7 = -64.6_f64;

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            source_0.encode(&mut cursor).await.unwrap();
            source_1.encode(&mut cursor).await.unwrap();
            source_2.encode(&mut cursor).await.unwrap();
            source_3.encode(&mut cursor).await.unwrap();
            source_4.encode(&mut cursor).await.unwrap();
            source_5.encode(&mut cursor).await.unwrap();
            source_6.encode(&mut cursor).await.unwrap();
            source_7.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).unwrap();
            let decoded_0 = f32::decode(&mut cursor).await.unwrap();
            let decoded_1 = f64::decode(&mut cursor).await.unwrap();
            let decoded_2 = f32::decode(&mut cursor).await.unwrap();
            let decoded_3 = f64::decode(&mut cursor).await.unwrap();
            let decoded_4 = f32::decode(&mut cursor).await.unwrap();
            let decoded_5 = f64::decode(&mut cursor).await.unwrap();
            let decoded_6 = f32::decode(&mut cursor).await.unwrap();
            let decoded_7 = f64::decode(&mut cursor).await.unwrap();

            assert_eq!(source_0, decoded_0);
            assert_eq!(source_1, decoded_1);
            assert_eq!(source_2, decoded_2);
            assert_eq!(source_3, decoded_3);
            assert_eq!(source_4, decoded_4);
            assert_eq!(source_5, decoded_5);
            assert_eq!(source_6, decoded_6);
            assert_eq!(source_7, decoded_7);
        }
        {
            let zero = 0.0_f32;
            let neg_zero = -0.0_f32;
            let neg_inf = f32::NEG_INFINITY;
            let inf = f32::INFINITY;
            let nan = f32::NAN;
            let neg_nan = -f32::NAN;

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            zero.encode(&mut cursor).await.unwrap();
            neg_zero.encode(&mut cursor).await.unwrap();
            neg_inf.encode(&mut cursor).await.unwrap();
            inf.encode(&mut cursor).await.unwrap();
            nan.encode(&mut cursor).await.unwrap();
            neg_nan.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).unwrap();
            let decoded_zero = f32::decode(&mut cursor).await.unwrap();
            let decoded_neg_zero = f32::decode(&mut cursor).await.unwrap();
            let decoded_neg_inf = f32::decode(&mut cursor).await.unwrap();
            let decoded_inf = f32::decode(&mut cursor).await.unwrap();
            let decoded_nan = f32::decode(&mut cursor).await.unwrap();
            let decoded_neg_nan = f32::decode(&mut cursor).await.unwrap();

            assert_eq!(zero, decoded_zero);
            assert!(decoded_zero.is_sign_positive());

            assert_eq!(neg_zero, decoded_neg_zero);
            assert!(decoded_neg_zero.is_sign_negative());

            assert_eq!(neg_inf, decoded_neg_inf);
            assert!(decoded_inf.is_infinite());
            assert!(decoded_inf.is_sign_positive());

            assert_eq!(inf, decoded_inf);
            assert!(decoded_inf.is_infinite());
            assert!(decoded_inf.is_sign_positive());

            assert_eq!(nan.is_nan(), decoded_nan.is_nan());
            assert!(decoded_nan.is_sign_positive());

            assert_eq!(neg_nan.is_nan(), decoded_neg_nan.is_nan());
            assert!(decoded_neg_nan.is_sign_negative());
        }
        {
            let zero = 0.0_f64;
            let neg_zero = -0.0_f64;
            let neg_inf = f64::NEG_INFINITY;
            let inf = f64::INFINITY;
            let nan = f64::NAN;
            let neg_nan = -f64::NAN;

            let mut bytes = Vec::new();
            let mut cursor = Cursor::new(&mut bytes);

            zero.encode(&mut cursor).await.unwrap();
            neg_zero.encode(&mut cursor).await.unwrap();
            neg_inf.encode(&mut cursor).await.unwrap();
            inf.encode(&mut cursor).await.unwrap();
            nan.encode(&mut cursor).await.unwrap();
            neg_nan.encode(&mut cursor).await.unwrap();

            cursor.seek(std::io::SeekFrom::Start(0)).unwrap();
            let decoded_zero = f64::decode(&mut cursor).await.unwrap();
            let decoded_neg_zero = f64::decode(&mut cursor).await.unwrap();
            let decoded_neg_inf = f64::decode(&mut cursor).await.unwrap();
            let decoded_inf = f64::decode(&mut cursor).await.unwrap();
            let decoded_nan = f64::decode(&mut cursor).await.unwrap();
            let decoded_neg_nan = f64::decode(&mut cursor).await.unwrap();

            assert_eq!(zero, decoded_zero);
            assert!(decoded_zero.is_sign_positive());

            assert_eq!(neg_zero, decoded_neg_zero);
            assert!(decoded_neg_zero.is_sign_negative());

            assert_eq!(neg_inf, decoded_neg_inf);
            assert!(decoded_inf.is_infinite());
            assert!(decoded_inf.is_sign_positive());

            assert_eq!(inf, decoded_inf);
            assert!(decoded_inf.is_infinite());
            assert!(decoded_inf.is_sign_positive());

            assert_eq!(nan.is_nan(), decoded_nan.is_nan());
            assert!(decoded_nan.is_sign_positive());

            assert_eq!(neg_nan.is_nan(), decoded_neg_nan.is_nan());
            assert!(decoded_neg_nan.is_sign_negative());
        }
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_encode_decode() {
        encode_decode().await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_encode_decode() {
        encode_decode().await;
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn test_tokio_encode_decode_float() {
        encode_decode_float().await;
    }

    #[cfg(feature = "monoio")]
    #[monoio::test]
    async fn test_monoio_encode_decode_float() {
        encode_decode_float().await;
    }
}

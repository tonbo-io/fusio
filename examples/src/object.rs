use fusio::dynamic::DynFile;
use fusio::{Error, IoBuf, IoBufMut, Read, Write};

#[allow(unused)]
async fn object_safe_file_trait<B, BM>(
    mut file: Box<dyn DynFile>,
    write_buf: B,
    read_buf: BM,
) -> (Result<(), Error>, B, BM)
where
    B: IoBuf,
    BM: IoBufMut,
{
    let (result, write_buf) = file.write_all(write_buf).await;
    if result.is_err() {
        return (result, write_buf, read_buf);
    }

    let (result, read_buf) = file.read(read_buf).await;
    if result.is_err() {
        return (result.map(|_| ()), write_buf, read_buf);
    }

    (Ok(()), write_buf, read_buf)
}

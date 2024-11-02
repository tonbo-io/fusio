mod fs;
mod multi_runtime;
mod object;
mod opendal;
mod s3;

use fusio::{Error, IoBuf, IoBufMut, Read, Write};

#[allow(unused)]
async fn write_without_runtime_awareness<F, B, BM>(
    file: &mut F,
    write_buf: B,
    read_buf: BM,
) -> (Result<(), Error>, B, BM)
where
    F: Read + Write,
    B: IoBuf,
    BM: IoBufMut,
{
    let (result, write_buf) = file.write_all(write_buf).await;
    if result.is_err() {
        return (result, write_buf, read_buf);
    }

    file.close().await.unwrap();

    let (result, read_buf) = file.read_exact_at(read_buf, 0).await;
    if result.is_err() {
        return (result.map(|_| ()), write_buf, read_buf);
    }

    (Ok(()), write_buf, read_buf)
}

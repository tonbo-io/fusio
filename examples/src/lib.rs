mod fs;
mod multi_runtime;
mod object;
mod s3;

use fusio::{Error, IoBuf, IoBufMut, Read, Seek, Write};

#[allow(unused)]
async fn write_without_runtime_awareness<F, B, BM>(
    file: &mut F,
    write_buf: B,
    read_buf: BM,
) -> (Result<(), Error>, B, BM)
where
    F: Read + Write + Seek,
    B: IoBuf,
    BM: IoBufMut,
{
    let (result, write_buf) = file.write_all(write_buf).await;
    if result.is_err() {
        return (result, write_buf, read_buf);
    }

    file.sync_all().await.unwrap();
    file.seek(0).await.unwrap();

    let (result, read_buf) = file.read(read_buf).await;
    if result.is_err() {
        return (result.map(|_| ()), write_buf, read_buf);
    }

    (Ok(()), write_buf, read_buf)
}

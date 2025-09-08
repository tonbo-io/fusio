use fusio::{dynamic::DynFile, Error, IoBuf, IoBufMut, Read, Write};

#[allow(unused)]
#[cfg(feature = "tokio")]
async fn use_tokio_file() {
    use std::sync::Arc;

    use fusio::{disk::LocalFs, fs::OpenOptions, DynFs};

    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});
    let mut file: Box<dyn DynFile> = fs
        .open_options(
            &"foo.txt".into(),
            OpenOptions::default().create(true).truncate(true),
        )
        .await
        .unwrap();
    let write_buf = "hello, world".as_bytes();
    let mut read_buf = [0; 12];
    let (result, _, read_buf) =
        object_safe_file_trait(&mut file, write_buf, &mut read_buf[..]).await;
    result.unwrap();
    assert_eq!(&read_buf, b"hello, world");
}

#[allow(unused)]
async fn object_safe_file_trait<B, BM>(
    mut file: &mut Box<dyn DynFile>,
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

    let (result, read_buf) = file.read_exact_at(read_buf, 0).await;
    if result.is_err() {
        return (result.map(|_| ()), write_buf, read_buf);
    }

    (Ok(()), write_buf, read_buf)
}

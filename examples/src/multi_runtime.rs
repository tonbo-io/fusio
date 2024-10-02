use crate::write_without_runtime_awareness;

#[allow(unused)]
#[cfg(feature = "tokio")]
async fn use_tokio_file() {
    use tokio::fs::File;

    let mut file = File::open("foo.txt").await.unwrap();
    let write_buf = "hello, world".as_bytes();
    let mut read_buf = [0; 12];
    let (result, _, read_buf) =
        write_without_runtime_awareness(&mut file, write_buf, &mut read_buf[..]).await;
    result.unwrap();
    assert_eq!(&read_buf, b"hello, world");
}

#[allow(unused)]
#[cfg(feature = "monoio")]
async fn use_monoio_file() {
    use fusio::disk::MonoioFile;
    use monoio::fs::File;

    let mut file: MonoioFile = File::open("foo.txt").await.unwrap().into();
    let write_buf = "hello, world".as_bytes();
    let read_buf = vec![0; 12];
    // completion-based runtime has to pass owned buffer to the function
    let (result, _, read_buf) =
        write_without_runtime_awareness(&mut file, write_buf, read_buf).await;
    result.unwrap();
    assert_eq!(&read_buf, b"hello, world");
}

use crate::write_without_runtime_awareness;

#[allow(unused)]
#[cfg(feature = "tokio")]
async fn use_tokio_file() {
    use fusio::{
        disk::LocalFs,
        fs::{Fs, OpenOptions},
    };

    let fs = LocalFs {};
    let mut file = fs
        .open_options(
            &"foo.txt".into(),
            OpenOptions::default().create(true).truncate(true),
        )
        .await
        .unwrap();
    let write_buf = "hello, world".as_bytes();
    let mut read_buf = vec![0; 12];
    let (result, _, read_buf) =
        write_without_runtime_awareness(&mut file, write_buf, read_buf).await;
    result.unwrap();
    assert_eq!(&read_buf[..], b"hello, world");
}

#[allow(unused)]
#[cfg(feature = "monoio")]
async fn use_monoio_file() {
    use fusio::{
        disk::MonoIoFs,
        fs::{Fs, OpenOptions},
    };

    let fs = MonoIoFs;
    let mut file = fs
        .open_options(
            &"foo.txt".into(),
            OpenOptions::default().create(true).truncate(true),
        )
        .await
        .unwrap();
    let write_buf = "hello, world".as_bytes();
    let read_buf = vec![0; 12];
    // completion-based runtime has to pass owned buffer to the function
    let (result, _, read_buf) =
        write_without_runtime_awareness(&mut file, write_buf, read_buf).await;
    result.unwrap();
    assert_eq!(&read_buf, b"hello, world");
}

use std::sync::Arc;

use fusio::{disk::LocalFs, dynamic::DynFile, DynFs};

#[allow(unused)]
async fn use_fs() {
    use fusio::fs::OpenOptions;
    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});

    let mut file: Box<dyn DynFile> = fs
        .open_options(
            &"foo.txt".into(),
            OpenOptions::default().create(true).truncate(true),
        )
        .await
        .unwrap();

    let write_buf = "hello, world".as_bytes();
    let mut read_buf = vec![0; 12];

    let (result, _, read_buf) =
        crate::write_without_runtime_awareness(&mut file, write_buf, read_buf).await;
    result.unwrap();

    assert_eq!(&read_buf[..], b"hello, world");
}

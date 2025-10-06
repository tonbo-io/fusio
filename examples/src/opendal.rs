use std::sync::Arc;

use fusio::{dynamic::DynFile, DynFs};
use fusio_opendal::OpendalFs;
use opendal::{services::Memory, Operator};

#[allow(unused)]
async fn use_opendalfs() {
    let op = Operator::new(Memory::default()).unwrap().finish();
    use fusio::fs::OpenOptions;
    let fs: Arc<dyn DynFs> = Arc::new(OpendalFs::from(op));

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

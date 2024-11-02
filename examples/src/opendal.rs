use fusio::{dynamic::DynFile, DynFs};
use fusio_opendal::OpendalFs;
use opendal::services::Memory;
use opendal::Operator;
use std::sync::Arc;

#[allow(unused)]
async fn use_opendalfs() {
    let op = Operator::new(Memory::default()).unwrap().finish();
    let fs: Arc<dyn DynFs> = Arc::new(OpendalFs::from(op));

    let mut file: Box<dyn DynFile> = Box::new(fs.open(&"foo.txt".into()).await.unwrap());

    let write_buf = "hello, world".as_bytes();
    let mut read_buf = [0; 12];

    let (result, _, read_buf) =
        crate::write_without_runtime_awareness(&mut file, write_buf, &mut read_buf[..]).await;
    result.unwrap();

    assert_eq!(&read_buf, b"hello, world");
}

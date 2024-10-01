use std::sync::Arc;

use fusio::disk::LocalFs;
use fusio::dynamic::DynFile;
use fusio::DynFs;

#[allow(unused)]
async fn use_fs() {
    let fs: Arc<dyn DynFs> = Arc::new(LocalFs {});

    let mut file: Box<dyn DynFile> = Box::new(fs.open(&"foo.txt".into()).await.unwrap());

    let write_buf = "hello, world".as_bytes();
    let mut read_buf = [0; 12];

    let (result, _, read_buf) =
        crate::write_without_runtime_awareness(&mut file, write_buf, &mut read_buf[..]).await;
    result.unwrap();

    assert_eq!(&read_buf, b"hello, world");
}

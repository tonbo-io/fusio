use std::env;
use std::sync::Arc;

use fusio::remotes::aws::fs::AmazonS3Builder;
use fusio::remotes::aws::AwsCredential;
use fusio::DynFs;

use crate::write_without_runtime_awareness;

#[allow(unused)]
async fn use_fs() {
    let key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();

    let s3 = AmazonS3Builder::new("fusio-test".into())
        .credential(AwsCredential {
            key_id,
            secret_key,
            token: None,
        })
        .region("ap-southeast-1".into())
        .sign_payload(true)
        .build();

    let fs: Arc<dyn DynFs> = Arc::new(s3);
    let _ = write_without_runtime_awareness(
        &mut fs.open(&"foo.txt".into()).await.unwrap(),
        "hello, world".as_bytes(),
        &mut [0; 12][..],
    )
    .await;
}

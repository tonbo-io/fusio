//! WASM environment tests for fusio-manifest.
//!
//! Run with: `wasm-pack test --chrome --headless fusio-manifest --no-default-features --features
//! wasm`

#![cfg(all(target_arch = "wasm32", feature = "wasm"))]

use std::sync::Arc;

use fusio::{executor::web::WebExecutor, impls::mem::fs::InMemoryFs};
use fusio_manifest::{
    context::ManifestContext, manifest::Manifest, retention::DefaultRetention, BackoffPolicy,
    CheckpointStoreImpl, HeadStoreImpl, LeaseStoreImpl, SegmentStoreImpl,
};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn create_test_manifest() -> Manifest<
    String,
    String,
    HeadStoreImpl<InMemoryFs>,
    SegmentStoreImpl<InMemoryFs>,
    CheckpointStoreImpl<InMemoryFs>,
    LeaseStoreImpl<InMemoryFs, WebExecutor>,
    WebExecutor,
> {
    let fs = InMemoryFs::new();
    let head = HeadStoreImpl::new(fs.clone(), "HEAD.json");
    let segment = SegmentStoreImpl::new(fs.clone(), "segments");
    let checkpoint = CheckpointStoreImpl::new(fs.clone(), "");
    let timer = WebExecutor::default();
    let lease = LeaseStoreImpl::new(fs, "", BackoffPolicy::default(), timer);

    let opts: ManifestContext<DefaultRetention, WebExecutor> =
        ManifestContext::new(WebExecutor::default());

    Manifest::new_with_context(head, segment, checkpoint, lease, Arc::new(opts))
}

#[wasm_bindgen_test]
async fn manifest_basic_write_read() {
    let manifest = create_test_manifest();

    // Write a key-value pair
    let mut writer = manifest.session_write().await.unwrap();
    writer.put("key1".into(), "value1".into());
    writer.commit().await.unwrap();

    // Read it back
    let value = manifest.get_latest(&"key1".to_string()).await.unwrap();
    assert_eq!(value, Some("value1".to_string()));
}

#[wasm_bindgen_test]
async fn manifest_multiple_writes() {
    let manifest = create_test_manifest();

    // First transaction
    let mut writer1 = manifest.session_write().await.unwrap();
    writer1.put("a".into(), "1".into());
    writer1.put("b".into(), "2".into());
    writer1.commit().await.unwrap();

    // Second transaction
    let mut writer2 = manifest.session_write().await.unwrap();
    writer2.put("c".into(), "3".into());
    writer2.commit().await.unwrap();

    // Verify all values
    assert_eq!(
        manifest.get_latest(&"a".to_string()).await.unwrap(),
        Some("1".to_string())
    );
    assert_eq!(
        manifest.get_latest(&"b".to_string()).await.unwrap(),
        Some("2".to_string())
    );
    assert_eq!(
        manifest.get_latest(&"c".to_string()).await.unwrap(),
        Some("3".to_string())
    );
}

#[wasm_bindgen_test]
async fn manifest_snapshot_isolation() {
    let manifest = create_test_manifest();

    // Initial write
    let mut writer = manifest.session_write().await.unwrap();
    writer.put("key".into(), "v1".into());
    writer.commit().await.unwrap();

    // Take a snapshot
    let snap = manifest.snapshot().await.unwrap();

    // Write a new value
    let mut writer2 = manifest.session_write().await.unwrap();
    writer2.put("key".into(), "v2".into());
    writer2.commit().await.unwrap();

    // Read from old snapshot - should see old value
    let reader = manifest.session_at(snap).await.unwrap();
    let old_value = reader.get(&"key".to_string()).await.unwrap();
    assert_eq!(old_value, Some("v1".to_string()));
    reader.end().await.unwrap();

    // Read latest - should see new value
    let latest = manifest.get_latest(&"key".to_string()).await.unwrap();
    assert_eq!(latest, Some("v2".to_string()));
}

#[wasm_bindgen_test]
async fn manifest_delete_tombstone() {
    let manifest = create_test_manifest();

    // Write initial value
    let mut writer = manifest.session_write().await.unwrap();
    writer.put("key".into(), "value".into());
    writer.commit().await.unwrap();

    // Delete the key
    let mut deleter = manifest.session_write().await.unwrap();
    deleter.delete("key".into());
    deleter.commit().await.unwrap();

    // Key should be gone
    let value = manifest.get_latest(&"key".to_string()).await.unwrap();
    assert_eq!(value, None);
}

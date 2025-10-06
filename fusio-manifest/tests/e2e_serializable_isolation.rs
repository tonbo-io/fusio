//! End-to-end tests for fusio-manifest that test serializable isolation.
//!
//! These tests validate the complete fusio-manifest implementation against S3 or S3-compatible
//! storage backends. They verify:
//! - Transaction commit and isolation
//! - Concurrent writer conflicts
//! - Write skew prevention (serializable isolation)
//! - Multi-writer chain operations
//! - Reader snapshot isolation
//! - High contention scenarios
//!
//! ## Running these tests
//!
//! ### Option 1: Against AWS S3 (with SSO)
//!
//! 1. Configure AWS SSO (if not already done): ```bash aws configure sso ```
//!
//! 2. Login to AWS SSO: ```bash aws sso login ```
//!
//! 3. Set environment variables and run tests: ```bash export AWS_REGION=ap-southeast-1  # or your
//!    preferred region export FUSIO_MANIFEST_BUCKET=your-bucket-name cargo test --test
//!    e2e_serializable_isolation -- --test-threads=1 ```
//!
//! ### Option 2: Against LocalStack
//!
//! 1. Start LocalStack
//!
//! 2. Set environment variables:
//!    ```bash
//!    export AWS_ACCESS_KEY_ID=test
//!    export AWS_SECRET_ACCESS_KEY=test
//!    export AWS_REGION=us-east-1
//!    export FUSIO_MANIFEST_BUCKET=fusio-test
//!    export AWS_ENDPOINT_URL=http://localhost:4566
//!    cargo test --test e2e_serializable_isolation -- --test-threads=1
//!    ```
//!
//! ## Required Environment Variables
//!
//! - `AWS_REGION`: AWS region (e.g., ap-southeast-1)
//! - `FUSIO_MANIFEST_BUCKET`: S3 bucket name
//! - `AWS_ACCESS_KEY_ID`: AWS access key (auto-set by SSO)
//! - `AWS_SECRET_ACCESS_KEY`: AWS secret key (auto-set by SSO)
//! - `AWS_SESSION_TOKEN`: Session token for SSO (auto-set by SSO, optional for static credentials)
//! - `AWS_ENDPOINT_URL`: Custom endpoint URL (optional, for LocalStack/MinIO)

use std::{env, sync::Arc};

use fusio::executor::tokio::TokioExecutor;
use fusio_manifest::{context::ManifestContext, s3, s3::S3Manifest, types::Result};

/// Helper to create an S3 manifest configured with the environmental variables.
/// If AWS_ENDPOINT_URL is present, we use it (LocalStack/MinIO). Otherwise, use real AWS S3.
fn create_test_manifest(test_name: &str) -> Result<S3Manifest<String, String, TokioExecutor>> {
    let bucket = env::var("FUSIO_MANIFEST_BUCKET").expect("FUSIO_MANIFEST_BUCKET not set");
    let key_id = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not set");
    let secret = env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not set");
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let endpoint = env::var("AWS_ENDPOINT_URL").ok();
    let token = env::var("AWS_SESSION_TOKEN").ok();

    let prefix = format!(
        "tests/{}/{}",
        test_name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );

    let mut builder = s3::Builder::new(&bucket)
        .prefix(prefix)
        .region(region.clone())
        .sign_payload(true)
        .credential(fusio::impls::remotes::aws::credential::AwsCredential {
            key_id,
            secret_key: secret,
            token,
        });

    if let Some(ep) = endpoint {
        builder = builder.endpoint(ep);
    }

    let config = builder.build();

    let context = Arc::new(ManifestContext::new(TokioExecutor::default()));
    let manifest: S3Manifest<String, String, TokioExecutor> = config.with_context(context).into();

    Ok(manifest)
}

/// Helper function to create an S3 manifest, returning the manifest, dropping the result.
fn create_test_manifest_without_result(
    test_name: &str,
) -> S3Manifest<String, String, TokioExecutor> {
    // Create manifest
    let result = create_test_manifest(test_name);
    assert!(result.is_ok());
    result.unwrap()
}

/// Helper function to write a key-value pair and commit the transaction.
async fn write_and_commit_data(
    manifest: &S3Manifest<String, String, TokioExecutor>,
    key: String,
    value: String,
) {
    let mut writer = manifest.session_write().await.unwrap();
    writer.put(key, value);

    writer.commit().await.unwrap();
}

// ============================================================================
// Basic Sanity Test
// ============================================================================

#[tokio::test]
async fn test_basic_write_read() -> Result<()> {
    // 1. Create manifest
    let manifest = create_test_manifest_without_result("test_basic_write_read");

    // 2. Write some key-value pairs
    let mut writer = manifest.session_write().await?;
    writer.put("key1".into(), "value1".into());
    writer.put("key2".into(), "value2".into());
    writer.put("key3".into(), "value3".into());

    // 3. Commit transaction
    writer.commit().await?;

    // 4. Read back and verify that values match
    let reader = manifest.session_read().await?;
    assert_eq!(
        reader.get(&"key1".to_string()).await?,
        Some("value1".to_string())
    );
    assert_eq!(
        reader.get(&"key2".to_string()).await?,
        Some("value2".to_string())
    );
    assert_eq!(
        reader.get(&"key3".to_string()).await?,
        Some("value3".to_string())
    );
    reader.end().await?;

    println!("✅ All assertions passed!");
    Ok(())
}

// ============================================================================
// Concurrency Tests
// ============================================================================

// Scenario 1.1 - Concurrent Writer Conflicts - Same Key Conflict
#[tokio::test]
async fn test_concurrent_writers_same_key_conflict() -> Result<()> {
    // 1. Create manifest
    let manifest = create_test_manifest_without_result("test_concurrent_writers_same_key_conflict");

    // 2. Write initial value to a key
    write_and_commit_data(&manifest, "key".into(), "value1".into()).await;

    // 3. Open two concurrent writers on the same HEAD
    let mut writer_a = manifest.session_write().await?;
    writer_a.put("key".into(), "value2".into());

    let mut writer_b = manifest.session_write().await?;
    writer_b.put("key".into(), "value3".into());

    // 4. The first commit wins (writer_b)
    writer_b.commit().await?;

    // 5. The second commit fails with CAS conflict (writer_a)
    let result = writer_a.commit().await;
    match result {
        Err(e) => {
            assert_eq!(e.to_string(), "precondition failed");
            println!("✅ Got expected conflict error: {:?}", e);
        }
        Ok(_) => panic!("Expected writer_a commit to fail with conflict"),
    }

    // 6. Verify winner's value persisted, loser's did not
    let reader = manifest.session_read().await?;
    assert_eq!(
        reader.get(&"key".to_string()).await?,
        Some("value3".to_string())
    );
    assert_ne!(
        reader.get(&"key".to_string()).await?,
        Some("value2".to_string())
    );
    reader.end().await?;

    println!("✅ All assertions passed!");
    Ok(())
}

// Scenario 1.2 - Write Skew Test
#[tokio::test]
async fn test_write_skew_prevention() -> Result<()> {
    let manifest = create_test_manifest_without_result("test_write_skew_prevention");

    // 1. Setup initial state: x = 100, y = 100
    let mut writer = manifest.session_write().await?;
    writer.put("x".into(), "100".into());
    writer.put("y".into(), "100".into());
    writer.commit().await?;

    // 2. Two writers read from the same snapshot, write to different keys
    let mut writer_a = manifest.session_write().await?;
    writer_a.put("x".into(), "0".into());

    let mut writer_b = manifest.session_write().await?;
    writer_b.put("y".into(), "0".into());

    // 3. Commit first writer
    writer_a.commit().await?;

    // 4. Second writer should fail despite writing to a different key (This proves serializable
    //    isolation via HEAD CAS)
    let result = writer_b.commit().await;
    assert!(result.is_err());

    // 5. Verify that x = 0, y = 100
    let reader = manifest.session_read().await?;
    assert_eq!(reader.get(&"x".to_string()).await?, Some("0".to_string()));
    assert_eq!(reader.get(&"y".to_string()).await?, Some("100".to_string()));
    reader.end().await?;

    println!("✅ All assertions passed!");
    Ok(())
}

// Scenario 1.3 - Multi-Writer Chain
#[tokio::test]
async fn test_multi_writer_chain() -> Result<()> {
    let manifest = create_test_manifest_without_result("test_multi_writer_chain");

    // 1. Initialize counter to 0
    let mut writer = manifest.session_write().await?;
    writer.put("count".into(), "0".into());
    writer.commit().await?;

    // 2. Execute 5 sequential writers, each one increments the counter
    for _i in 1..=5 {
        let reader = manifest.session_read().await?;
        let current = reader
            .get(&"count".to_string())
            .await?
            .unwrap()
            .parse::<i32>()
            .unwrap();
        reader.end().await?;

        let mut writer = manifest.session_write().await?;
        writer.put("count".into(), (current + 1).to_string());
        writer.commit().await?;
    }

    // 3. Verify the final value is 5
    let reader = manifest.session_read().await?;
    assert_eq!(
        reader.get(&"count".to_string()).await?,
        Some("5".to_string())
    );
    reader.end().await?;

    println!("✅ All 5 sequential writes succeeded!");
    Ok(())
}

// Scenario 1.4 - Reader During Concurrent Writes
#[tokio::test]
async fn test_reader_during_concurrent_writes() -> Result<()> {
    let manifest = create_test_manifest_without_result("test_reader_during_concurrent_writes");

    // 1. Initial state: a=1, b=2
    write_and_commit_data(&manifest, "a".into(), "1".into()).await;
    write_and_commit_data(&manifest, "b".into(), "2".into()).await;

    // 2. Open writer (doesn't commit yet)
    let mut writer = manifest.session_write().await?;
    writer.put("a".into(), "10".into());
    writer.put("b".into(), "20".into());

    // 3. Open reader BEFORE writer commits
    let reader = manifest.session_read().await?;

    // 4. Now commit the writer
    writer.commit().await?;

    // 5. Reader should still see OLD values (snapshot isolation)
    assert_eq!(reader.get(&"a".to_string()).await?, Some("1".to_string()));
    assert_eq!(reader.get(&"b".to_string()).await?, Some("2".to_string()));
    reader.end().await?;

    // 6. New reader sees NEW values
    let reader2 = manifest.session_read().await?;
    assert_eq!(reader2.get(&"a".to_string()).await?, Some("10".to_string()));
    assert_eq!(reader2.get(&"b".to_string()).await?, Some("20".to_string()));
    reader2.end().await?;

    println!("✅ Snapshot isolation verified!");
    Ok(())
}

// Scenario 1.5 - High Contention (10 Concurrent Writers)
#[tokio::test]
async fn test_high_contention_writers() -> Result<()> {
    let manifest = create_test_manifest_without_result("test_high_contention");

    // 1. Initial value
    write_and_commit_data(&manifest, "key".into(), "initial".into()).await;

    // 2. Open 10 concurrent writers from the same snapshot
    let mut writers = vec![];
    for i in 0..10 {
        let mut writer = manifest.session_write().await?;
        writer.put("key".into(), format!("writer-{}", i));
        writers.push(writer);
    }

    // 3. Try to commit all - only 1 should succeed, 9 should fail
    let mut success_count = 0;
    let mut failure_count = 0;

    for writer in writers {
        match writer.commit().await {
            Ok(_) => success_count += 1,
            Err(_) => failure_count += 1,
        }
    }

    // 4. Verify: exactly 1 success, 9 failures
    assert_eq!(success_count, 1, "Expected exactly 1 writer to succeed");
    assert_eq!(failure_count, 9, "Expected exactly 9 writers to fail");

    // 5. Winner's value should be persisted
    let reader = manifest.session_read().await?;
    let value = reader.get(&"key".to_string()).await?.unwrap();
    assert!(
        value.starts_with("writer-"),
        "Winner's value should be writer-N"
    );
    reader.end().await?;

    println!("✅ High contention test passed: 1 winner, 9 conflicts!");
    Ok(())
}

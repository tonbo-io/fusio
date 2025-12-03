use std::time::{Duration, SystemTime};

use fusio::{
    executor::Timer,
    fs::{CasCondition, Fs, FsCas, OpenOptions},
    path::Path,
    Error as FsError, Read,
};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::{
    backoff::{classify_error, BackoffPolicy, RetryClass},
    head::HeadTag,
    types::{Error, Result},
};

pub mod keeper;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseId(pub String);

#[derive(Debug, Clone)]
pub struct LeaseHandle {
    pub id: LeaseId,
    pub snapshot_txn_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveLease {
    pub id: LeaseId,
    pub snapshot_txn_id: u64,
    pub expires_at: Duration,
}

pub trait LeaseStore: MaybeSend + MaybeSync + Clone {
    fn create(
        &self,
        snapshot_txn_id: u64,
        head_tag: Option<HeadTag>,
        ttl: Duration,
    ) -> impl MaybeSendFuture<Output = Result<LeaseHandle>> + '_;

    fn heartbeat(
        &self,
        lease: &LeaseHandle,
        ttl: Duration,
    ) -> impl MaybeSendFuture<Output = Result<(), Error>> + '_;

    fn release(&self, lease: LeaseHandle) -> impl MaybeSendFuture<Output = Result<(), Error>> + '_;

    fn list_active(
        &self,
        now: Duration,
    ) -> impl MaybeSendFuture<Output = Result<Vec<ActiveLease>, Error>> + '_;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseDoc {
    id: String,
    snapshot_txn_id: u64,
    expires_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    head_tag: Option<String>,
}

/// Lease store backed by a filesystem implementation.
#[derive(Debug, Clone)]
pub struct LeaseStoreImpl<FS, T>
where
    T: Timer + Clone,
{
    fs: FS,
    prefix: String,
    backoff: BackoffPolicy,
    timer: T,
}

impl<FS, T> LeaseStoreImpl<FS, T>
where
    T: Timer + Clone,
{
    pub fn new(fs: FS, prefix: impl Into<String>, backoff: BackoffPolicy, timer: T) -> Self {
        let prefix = prefix.into().trim_end_matches('/').to_string();
        Self {
            fs,
            prefix,
            backoff,
            timer,
        }
    }

    fn key_for(&self, id: &str) -> String {
        if self.prefix.is_empty() {
            format!("leases/{}.json", id)
        } else {
            format!("{}/leases/{}.json", self.prefix, id)
        }
    }

    fn wall_clock_now_ms(&self) -> u64 {
        self.timer
            .now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }
}

impl<FS, T> LeaseStore for LeaseStoreImpl<FS, T>
where
    FS: Fs + FsCas + Clone + MaybeSend + MaybeSync + 'static,
    T: Timer + Clone + 'static,
{
    fn create(
        &self,
        snapshot_txn_id: u64,
        head_tag: Option<HeadTag>,
        ttl: Duration,
    ) -> impl MaybeSendFuture<Output = Result<LeaseHandle, Error>> + '_ {
        async move {
            let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
            let mut attempt: u32 = 0;
            let mut backoff_iter = self.backoff.build_backoff();

            loop {
                let now = self.wall_clock_now_ms();
                let expires_at_ms = now.saturating_add(ttl_ms);
                let id = if attempt == 0 {
                    format!("lease-{}", now)
                } else {
                    format!("lease-{}-{}", now, attempt)
                };
                let key = self.key_for(&id);
                let doc = LeaseDoc {
                    id: id.clone(),
                    snapshot_txn_id,
                    expires_at_ms,
                    head_tag: head_tag.as_ref().map(|t| t.0.clone()),
                };
                let body = serde_json::to_vec(&doc)
                    .map_err(|e| Error::Corrupt(format!("lease encode: {e}")))?;
                let path = Path::parse(&key).map_err(Error::other)?;
                match self
                    .fs
                    .put_conditional(
                        &path,
                        &body,
                        Some("application/json"),
                        None,
                        CasCondition::IfNotExists,
                    )
                    .await
                {
                    Ok(_) => {
                        return Ok(LeaseHandle {
                            id: LeaseId(id),
                            snapshot_txn_id,
                        });
                    }
                    Err(e) => {
                        let err: Error = e.into();
                        match classify_error(&err) {
                            RetryClass::RetryTransient => {
                                if let Some(delay) = backoff_iter.next() {
                                    attempt += 1;
                                    self.timer.sleep(delay).await;
                                    continue;
                                } else {
                                    return Err(err);
                                }
                            }
                            _ => return Err(err),
                        }
                    }
                }
            }
        }
    }

    fn heartbeat(
        &self,
        lease: &LeaseHandle,
        ttl: Duration,
    ) -> impl MaybeSendFuture<Output = Result<(), Error>> + '_ {
        let key = self.key_for(&lease.id.0);
        async move {
            let path = Path::parse(&key).map_err(Error::other)?;
            let (bytes, tag) = match self.fs.load_with_tag(&path).await? {
                Some(v) => v,
                None => return Err(Error::Corrupt("lease missing".into())),
            };
            let mut doc: LeaseDoc = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Corrupt(format!("lease decode: {e}")))?;
            let now = self.wall_clock_now_ms();
            let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
            doc.expires_at_ms = now.saturating_add(ttl_ms);
            let body = serde_json::to_vec(&doc)
                .map_err(|e| Error::Corrupt(format!("lease encode: {e}")))?;
            self.fs
                .put_conditional(
                    &path,
                    &body,
                    Some("application/json"),
                    None,
                    CasCondition::IfMatch(tag),
                )
                .await
                .map_err(|e| match e {
                    FsError::PreconditionFailed => Error::PreconditionFailed,
                    other => other.into(),
                })?;
            Ok(())
        }
    }

    fn release(&self, lease: LeaseHandle) -> impl MaybeSendFuture<Output = Result<(), Error>> + '_ {
        async move {
            if let Ok(path) = Path::parse(self.key_for(&lease.id.0)) {
                let _ = self.fs.remove(&path).await;
            }
            Ok(())
        }
    }

    fn list_active(
        &self,
        now: Duration,
    ) -> impl MaybeSendFuture<Output = Result<Vec<ActiveLease>, Error>> + '_ {
        async move {
            use futures_util::StreamExt;
            let mut out = Vec::new();
            let dir = if self.prefix.is_empty() {
                "leases".to_string()
            } else {
                format!("{}/leases", self.prefix)
            };
            let prefix_path = Path::from(dir);
            let stream = self.fs.list(&prefix_path).await?;
            futures_util::pin_mut!(stream);
            let now_ms = now.as_millis().min(u128::from(u64::MAX)) as u64;
            while let Some(item) = stream.next().await {
                let meta = item?;
                let mut f = self
                    .fs
                    .open_options(&meta.path, OpenOptions::default())
                    .await?;
                let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                res?;
                if let Ok(doc) = serde_json::from_slice::<LeaseDoc>(&bytes) {
                    if doc.expires_at_ms > now_ms {
                        out.push(ActiveLease {
                            id: LeaseId(doc.id),
                            snapshot_txn_id: doc.snapshot_txn_id,
                            expires_at: Duration::from_millis(doc.expires_at_ms),
                        });
                    }
                }
            }
            Ok(out)
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::{
        io::{Error as IoError, ErrorKind},
        pin::Pin,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
    };

    use fusio::{
        error::Error as FsError,
        fs::{FileMeta, FileSystemTag, OpenOptions},
        impls::mem::fs::InMemoryFs,
    };
    use fusio_core::MaybeSendFuture;
    use futures_executor::block_on;
    use futures_util::stream::Stream;
    use rstest::rstest;

    use super::*;
    use crate::test_utils::{in_memory_stores, InMemoryStores};

    #[derive(Clone)]
    struct FailingFs {
        inner: InMemoryFs,
        fail_for: Arc<AtomicU32>,
    }

    impl FailingFs {
        fn new(fail_count: u32) -> Self {
            Self {
                inner: InMemoryFs::new(),
                fail_for: Arc::new(AtomicU32::new(fail_count)),
            }
        }

        fn should_fail(&self) -> bool {
            let current = self.fail_for.load(Ordering::SeqCst);
            if current > 0 {
                self.fail_for.fetch_sub(1, Ordering::SeqCst);
                true
            } else {
                false
            }
        }
    }

    impl Fs for FailingFs {
        type File = <InMemoryFs as Fs>::File;

        fn file_system(&self) -> FileSystemTag {
            self.inner.file_system()
        }

        async fn open_options(
            &self,
            path: &Path,
            options: OpenOptions,
        ) -> Result<Self::File, FsError> {
            self.inner.open_options(path, options).await
        }

        async fn create_dir_all(_path: &Path) -> Result<(), FsError> {
            InMemoryFs::create_dir_all(_path).await
        }

        async fn list(
            &self,
            path: &Path,
        ) -> Result<impl Stream<Item = Result<FileMeta, FsError>> + fusio_core::MaybeSend, FsError>
        {
            self.inner.list(path).await
        }

        async fn remove(&self, path: &Path) -> Result<(), FsError> {
            self.inner.remove(path).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> Result<(), FsError> {
            self.inner.copy(from, to).await
        }

        async fn link(&self, from: &Path, to: &Path) -> Result<(), FsError> {
            self.inner.link(from, to).await
        }
    }

    impl FsCas for FailingFs {
        fn load_with_tag(
            &self,
            path: &Path,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(Vec<u8>, String)>, FsError>> + '_>>
        {
            self.inner.load_with_tag(path)
        }

        fn put_conditional(
            &self,
            path: &Path,
            payload: &[u8],
            content_type: Option<&str>,
            metadata: Option<Vec<(String, String)>>,
            condition: CasCondition,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<String, FsError>> + '_>> {
            if self.should_fail() {
                Box::pin(async move {
                    Err(FsError::Io(IoError::new(
                        ErrorKind::TimedOut,
                        "simulated S3 throttling",
                    )))
                })
            } else {
                self.inner
                    .put_conditional(path, payload, content_type, metadata, condition)
            }
        }
    }

    #[rstest]
    fn mem_lease_ttl_and_min_watermark(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let store = in_memory_stores.lease;
            // Create two leases at different snapshot txn_ids
            let ttl = Duration::from_secs(60);
            let l1 = store.create(100, None, ttl).await.unwrap();
            let l2 = store.create(50, None, ttl).await.unwrap();

            // Now = 0 should show both as active if we pass a very small now (simulate immediate
            // check)
            let active = store.list_active(Duration::from_millis(0)).await.unwrap();
            assert_eq!(active.len(), 2);
            let min = active.iter().map(|l| l.snapshot_txn_id).min().unwrap();
            assert_eq!(min, 50);

            // Heartbeat l1 and ensure it extends expiry without affecting txn id
            store.heartbeat(&l1, ttl).await.unwrap();

            // Simulate far-future 'now' so all leases appear expired
            let far_future = Duration::from_millis(u64::MAX / 2);
            let active2 = store.list_active(far_future).await.unwrap();
            assert!(active2.is_empty());

            // Release removes from active set
            store.release(l2).await.unwrap();
        })
    }

    #[test]
    fn lease_first_attempt_success_no_retry_suffix() {
        block_on(async move {
            let fs = FailingFs::new(0);
            let timer = fusio::executor::BlockingExecutor::default();
            let policy = BackoffPolicy::default();
            let store = LeaseStoreImpl::new(fs, "", policy, timer);
            let ttl = Duration::from_secs(60);

            let lease = store.create(100, None, ttl).await.unwrap();

            assert!(
                lease.id.0.starts_with("lease-"),
                "lease ID should start with 'lease-', got: {}",
                lease.id.0
            );
            assert!(
                !lease.id.0.contains("--"),
                "lease ID should not contain double hyphens, got: {}",
                lease.id.0
            );
            let after_prefix = &lease.id.0["lease-".len()..];
            assert!(
                !after_prefix.contains('-'),
                "lease ID should be 'lease-{{timestamp}}' with no retry suffix, got: {}",
                lease.id.0
            );
            assert!(
                after_prefix.chars().all(|c| c.is_ascii_digit()),
                "timestamp should be numeric, got: {}",
                after_prefix
            );
            assert_eq!(lease.snapshot_txn_id, 100);
        })
    }

    #[test]
    fn lease_retry_on_transient_errors() {
        block_on(async move {
            let fs = FailingFs::new(2);
            let timer = fusio::executor::BlockingExecutor::default();
            let policy = BackoffPolicy {
                base_ms: 1,
                max_ms: 10,
                multiplier_times_100: 200,
                jitter_frac_times_100: 0,
                max_retries: 5,
                max_elapsed_ms: 1000,
                max_backoff_sleep_ms: 1000,
            };
            let store = LeaseStoreImpl::new(fs, "", policy, timer);
            let ttl = Duration::from_secs(60);

            let lease = store.create(100, None, ttl).await.unwrap();

            assert!(
                lease.id.0.starts_with("lease-"),
                "lease ID should start with 'lease-', got: {}",
                lease.id.0
            );
            assert!(
                lease.id.0.ends_with("-2"),
                "lease ID should end with '-2' after 2 retries, got: {}",
                lease.id.0
            );
            assert_eq!(lease.snapshot_txn_id, 100);
        })
    }

    #[test]
    fn lease_retry_exhaustion() {
        block_on(async move {
            let fs = FailingFs::new(100);
            let timer = fusio::executor::BlockingExecutor::default();
            let policy = BackoffPolicy {
                base_ms: 1,
                max_ms: 10,
                multiplier_times_100: 200,
                jitter_frac_times_100: 0,
                max_retries: 2,
                max_elapsed_ms: 1000,
                max_backoff_sleep_ms: 1000,
            };
            let store = LeaseStoreImpl::new(fs, "", policy, timer);
            let ttl = Duration::from_secs(60);

            let result = store.create(100, None, ttl).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(matches!(err, Error::Io(_)));
        })
    }
}

use core::pin::Pin;
use std::time::Duration;

use fusio::{
    fs::{CasCondition, Fs, FsCas},
    impls::remotes::aws::fs::AmazonS3,
    path::Path,
    Error as FsError, Read,
};
use fusio_core::MaybeSendFuture;
use serde::{Deserialize, Serialize};

use crate::{
    backoff::{BackoffPolicy, ExponentialBackoff, TimerHandle},
    head::HeadTag,
    lease::{ActiveLease, LeaseHandle, LeaseId, LeaseStore},
    types::Error,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseDoc {
    id: String,
    snapshot_txn_id: u64,
    expires_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    head_tag: Option<String>,
}

/// Lease store backed by an Amazon S3 filesystem handle.
#[derive(Clone)]
pub struct FsLeaseStore {
    fs: AmazonS3,
    prefix: String,
    backoff: BackoffPolicy,
    timer: TimerHandle,
}

impl FsLeaseStore {
    pub fn new(
        fs: AmazonS3,
        prefix: impl Into<String>,
        backoff: BackoffPolicy,
        timer: TimerHandle,
    ) -> Self {
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

    fn unix_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl LeaseStore for FsLeaseStore {
    fn create(
        &self,
        snapshot_txn_id: u64,
        head_tag: Option<HeadTag>,
        ttl: Duration,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<LeaseHandle, crate::types::Error>>>> {
        let fs = self.fs.clone();
        let prefix = self.prefix.clone();
        let timer = self.timer.clone();
        let backoff_pol = self.backoff;
        Box::pin(async move {
            let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
            let mut attempt: u32 = 0;
            let mut backoff = ExponentialBackoff::new(backoff_pol);
            loop {
                let now = Self::unix_ms();
                let expires_at_ms = now.saturating_add(ttl_ms);
                let id = if attempt == 0 {
                    format!("lease-{}", now)
                } else {
                    format!("lease-{}-{}", now, attempt)
                };
                let key = if prefix.is_empty() {
                    format!("leases/{}.json", id)
                } else {
                    format!("{}/leases/{}.json", prefix, id)
                };
                let doc = LeaseDoc {
                    id: id.clone(),
                    snapshot_txn_id,
                    expires_at_ms,
                    head_tag: head_tag.as_ref().map(|t| t.0.clone()),
                };
                let body = serde_json::to_vec(&doc)
                    .map_err(|e| Error::Corrupt(format!("lease encode: {e}")))?;
                let path = Path::parse(&key).map_err(|e| Error::Other(Box::new(e)))?;
                match fs
                    .put_conditional(
                        &path,
                        body,
                        Some("application/json"),
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
                    Err(FsError::PreconditionFailed) => {
                        attempt = attempt.saturating_add(1);
                        if backoff.exhausted() {
                            return Err(Error::PreconditionFailed);
                        }
                        let delay = backoff.next_delay();
                        timer.sleep(delay).await;
                    }
                    Err(other) => return Err(Error::Other(Box::new(other))),
                }
            }
        })
    }

    fn heartbeat(
        &self,
        lease: &LeaseHandle,
        ttl: Duration,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), crate::types::Error>>>> {
        let fs = self.fs.clone();
        let key = self.key_for(&lease.id.0);
        Box::pin(async move {
            let path = Path::parse(&key).map_err(|e| Error::Other(Box::new(e)))?;
            let (bytes, tag) = match fs.load_with_tag(&path).await {
                Ok(Some(v)) => v,
                Ok(None) => return Err(Error::Corrupt("lease missing".into())),
                Err(err) => return Err(Error::Other(Box::new(err))),
            };
            let mut doc: LeaseDoc = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Corrupt(format!("lease decode: {e}")))?;
            let now = Self::unix_ms();
            let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
            doc.expires_at_ms = now.saturating_add(ttl_ms);
            let body = serde_json::to_vec(&doc)
                .map_err(|e| Error::Corrupt(format!("lease encode: {e}")))?;
            fs.put_conditional(
                &path,
                body,
                Some("application/json"),
                CasCondition::IfMatch(tag),
            )
            .await
            .map_err(|e| match e {
                FsError::PreconditionFailed => Error::PreconditionFailed,
                other => Error::Other(Box::new(other)),
            })?;
            Ok(())
        })
    }

    fn release(
        &self,
        lease: LeaseHandle,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), crate::types::Error>>>> {
        let fs = self.fs.clone();
        let key = self.key_for(&lease.id.0);
        Box::pin(async move {
            if let Ok(path) = Path::parse(&key) {
                let _ = fs.remove(&path).await;
            }
            Ok(())
        })
    }

    fn list_active(
        &self,
        now_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<ActiveLease>, crate::types::Error>>>> {
        let fs = self.fs.clone();
        let dir = if self.prefix.is_empty() {
            "leases".to_string()
        } else {
            format!("{}/leases", self.prefix)
        };
        Box::pin(async move {
            use futures_util::StreamExt;
            let mut out = Vec::new();
            let prefix_path = Path::from(dir.clone());
            let stream = fs
                .list(&prefix_path)
                .await
                .map_err(|e| Error::Other(Box::new(e)))?;
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| Error::Other(Box::new(e)))?;
                let mut f = fs
                    .open(&meta.path)
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                res.map_err(|e| Error::Other(Box::new(e)))?;
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
        })
    }
}

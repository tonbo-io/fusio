use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use fusio::{
    fs::{CasCondition, Fs, FsCas, OpenOptions},
    path::Path,
    Error as FsError, Read,
};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::{
    backoff::{BackoffPolicy, ExponentialBackoff},
    head::HeadTag,
    types::Result,
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
    ) -> impl MaybeSendFuture<Output = Result<(), crate::types::Error>> + '_;

    fn release(
        &self,
        lease: LeaseHandle,
    ) -> impl MaybeSendFuture<Output = Result<(), crate::types::Error>> + '_;

    fn list_active(
        &self,
        now: Duration,
    ) -> impl MaybeSendFuture<Output = Result<Vec<ActiveLease>, crate::types::Error>> + '_;
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
#[derive(Clone)]
pub struct FsLeaseStore<FS> {
    fs: FS,
    prefix: String,
    backoff: BackoffPolicy,
    timer: Arc<dyn fusio::executor::Timer + Send + Sync>,
}

impl<FS> FsLeaseStore<FS> {
    pub fn new(
        fs: FS,
        prefix: impl Into<String>,
        backoff: BackoffPolicy,
        timer: Arc<dyn fusio::executor::Timer + Send + Sync>,
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

    fn wall_clock_now_ms() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl<FS> LeaseStore for FsLeaseStore<FS>
where
    FS: Fs + FsCas + Clone + Send + Sync + 'static,
{
    fn create(
        &self,
        snapshot_txn_id: u64,
        head_tag: Option<HeadTag>,
        ttl: Duration,
    ) -> impl MaybeSendFuture<Output = Result<LeaseHandle, crate::types::Error>> + '_ {
        async move {
            let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
            let mut attempt: u32 = 0;
            let mut backoff = ExponentialBackoff::new(self.backoff, self.timer.clone());
            loop {
                let now = Self::wall_clock_now_ms();
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
                    .map_err(|e| crate::types::Error::Corrupt(format!("lease encode: {e}")))?;
                let path =
                    Path::parse(&key).map_err(|e| crate::types::Error::Other(Box::new(e)))?;
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
                    Err(FsError::PreconditionFailed) => {
                        attempt = attempt.saturating_add(1);
                        if backoff.exhausted() {
                            return Err(crate::types::Error::PreconditionFailed);
                        }
                        let delay = backoff.next_delay();
                        self.timer.sleep(delay).await;
                    }
                    Err(other) => return Err(crate::types::Error::Other(Box::new(other))),
                }
            }
        }
    }

    fn heartbeat(
        &self,
        lease: &LeaseHandle,
        ttl: Duration,
    ) -> impl MaybeSendFuture<Output = Result<(), crate::types::Error>> + '_ {
        let key = self.key_for(&lease.id.0);
        async move {
            let path = Path::parse(&key).map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            let (bytes, tag) = match self.fs.load_with_tag(&path).await {
                Ok(Some(v)) => v,
                Ok(None) => return Err(crate::types::Error::Corrupt("lease missing".into())),
                Err(err) => return Err(crate::types::Error::Other(Box::new(err))),
            };
            let mut doc: LeaseDoc = serde_json::from_slice(&bytes)
                .map_err(|e| crate::types::Error::Corrupt(format!("lease decode: {e}")))?;
            let now = Self::wall_clock_now_ms();
            let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
            doc.expires_at_ms = now.saturating_add(ttl_ms);
            let body = serde_json::to_vec(&doc)
                .map_err(|e| crate::types::Error::Corrupt(format!("lease encode: {e}")))?;
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
                    FsError::PreconditionFailed => crate::types::Error::PreconditionFailed,
                    other => crate::types::Error::Other(Box::new(other)),
                })?;
            Ok(())
        }
    }

    fn release(
        &self,
        lease: LeaseHandle,
    ) -> impl MaybeSendFuture<Output = Result<(), crate::types::Error>> + '_ {
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
    ) -> impl MaybeSendFuture<Output = Result<Vec<ActiveLease>, crate::types::Error>> + '_ {
        async move {
            use futures_util::StreamExt;
            let mut out = Vec::new();
            let dir = if self.prefix.is_empty() {
                "leases".to_string()
            } else {
                format!("{}/leases", self.prefix)
            };
            let prefix_path = Path::from(dir);
            let stream = self
                .fs
                .list(&prefix_path)
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            futures_util::pin_mut!(stream);
            let now_ms = now.as_millis().min(u128::from(u64::MAX)) as u64;
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                let mut f = self
                    .fs
                    .open_options(&meta.path, OpenOptions::default())
                    .await
                    .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
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

#[cfg(test)]
mod tests {
    use futures_executor::block_on;

    use super::*;
    use crate::testing::new_inmemory_stores;

    #[test]
    fn mem_lease_ttl_and_min_watermark() {
        block_on(async move {
            let (_, _, _, store) = new_inmemory_stores();
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
}

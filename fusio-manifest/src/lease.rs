use core::pin::Pin;
#[cfg(feature = "mem")]
use std::sync::atomic::{AtomicU64, Ordering};

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::{head::HeadTag, types::Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseId(pub String);

#[derive(Debug, Clone)]
pub struct LeaseHandle {
    pub id: LeaseId,
    pub snapshot_lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveLease {
    pub id: LeaseId,
    pub snapshot_lsn: u64,
    pub expires_at_ms: u64,
}

pub trait LeaseStore: MaybeSend + MaybeSync + Clone {
    fn create(
        &self,
        snapshot_lsn: u64,
        _head_tag: Option<HeadTag>,
        ttl_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<LeaseHandle>>>>;

    fn heartbeat(
        &self,
        lease: &LeaseHandle,
        ttl_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>>;

    fn release(&self, lease: LeaseHandle) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>>;

    fn list_active(
        &self,
        now_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<ActiveLease>>>>>;
}

#[cfg(feature = "mem")]
#[derive(Debug, Clone, Default)]
pub struct MemLeaseStore {
    ctr: std::sync::Arc<AtomicU64>,
    inner: std::sync::Arc<std::sync::Mutex<Vec<ActiveLease>>>,
}

#[cfg(feature = "mem")]
impl MemLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(feature = "mem")]
impl LeaseStore for MemLeaseStore {
    fn create(
        &self,
        snapshot_lsn: u64,
        _head_tag: Option<HeadTag>,
        ttl_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<LeaseHandle>>>> {
        let id = self.ctr.fetch_add(1, Ordering::Relaxed) + 1;
        let lease = LeaseHandle {
            id: LeaseId(format!("lease-{:020}", id)),
            snapshot_lsn,
        };
        let now_ms = unix_ms();
        let active = ActiveLease {
            id: lease.id.clone(),
            snapshot_lsn,
            expires_at_ms: now_ms + ttl_ms,
        };
        let inner = self.inner.clone();
        Box::pin(async move {
            inner.lock().unwrap().push(active);
            Ok(lease)
        })
    }

    fn heartbeat(
        &self,
        lease: &LeaseHandle,
        ttl_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>> {
        let id = lease.id.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let now = unix_ms();
            let mut g = inner.lock().unwrap();
            if let Some(l) = g.iter_mut().find(|l| l.id == id) {
                l.expires_at_ms = now + ttl_ms;
            }
            Ok(())
        })
    }

    fn release(&self, lease: LeaseHandle) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>> {
        let id = lease.id;
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut g = inner.lock().unwrap();
            g.retain(|l| l.id != id);
            Ok(())
        })
    }

    fn list_active(
        &self,
        now_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<ActiveLease>>>>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let g = inner.lock().unwrap();
            let v = g
                .iter()
                .filter(|l| l.expires_at_ms > now_ms)
                .cloned()
                .collect();
            Ok(v)
        })
    }
}

#[cfg(feature = "mem")]
fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

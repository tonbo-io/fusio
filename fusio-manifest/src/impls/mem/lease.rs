#![cfg(test)]

use core::pin::Pin;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use fusio_core::MaybeSendFuture;

use crate::{
    head::HeadTag,
    lease::{ActiveLease, LeaseHandle, LeaseId, LeaseStore},
    types::Result,
};

#[derive(Debug, Clone, Default)]
pub struct MemLeaseStore {
    ctr: std::sync::Arc<AtomicU64>,
    inner: std::sync::Arc<std::sync::Mutex<Vec<ActiveLease>>>,
}

impl MemLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LeaseStore for MemLeaseStore {
    fn create(
        &self,
        snapshot_txn_id: u64,
        _head_tag: Option<HeadTag>,
        ttl: Duration,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<LeaseHandle>>>> {
        let id = self.ctr.fetch_add(1, Ordering::Relaxed) + 1;
        let lease = LeaseHandle {
            id: LeaseId(format!("lease-{:020}", id)),
            snapshot_txn_id,
        };
        let now_ms = unix_ms();
        let active = ActiveLease {
            id: lease.id.clone(),
            snapshot_txn_id,
            expires_at: Duration::from_millis(now_ms).saturating_add(ttl),
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
        ttl: Duration,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>> {
        let id = lease.id.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            let now = unix_ms();
            let mut g = inner.lock().unwrap();
            if let Some(l) = g.iter_mut().find(|l| l.id == id) {
                l.expires_at = Duration::from_millis(now).saturating_add(ttl);
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
            let now = Duration::from_millis(now_ms);
            let g = inner.lock().unwrap();
            let v = g.iter().filter(|l| l.expires_at > now).cloned().collect();
            Ok(v)
        })
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

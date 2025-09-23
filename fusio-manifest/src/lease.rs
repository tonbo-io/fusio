use core::pin::Pin;
use std::time::Duration;

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::{head::HeadTag, types::Result};

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
        _head_tag: Option<HeadTag>,
        ttl: Duration,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<LeaseHandle>>>>;

    fn heartbeat(
        &self,
        lease: &LeaseHandle,
        ttl: Duration,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>>;

    fn release(&self, lease: LeaseHandle) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>>;

    fn list_active(
        &self,
        now_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<ActiveLease>>>>>;
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;

    use super::*;
    use crate::impls::mem::lease::MemLeaseStore;

    #[test]
    fn mem_lease_ttl_and_min_watermark() {
        block_on(async move {
            let store = MemLeaseStore::new();
            // Create two leases at different snapshot txn_ids
            let ttl = Duration::from_secs(60);
            let l1 = store.create(100, None, ttl).await.unwrap();
            let l2 = store.create(50, None, ttl).await.unwrap();

            // Now = 0 should show both as active if we pass a very small now (simulate immediate
            // check)
            let active = store.list_active(0).await.unwrap();
            assert_eq!(active.len(), 2);
            let min = active.iter().map(|l| l.snapshot_txn_id).min().unwrap();
            assert_eq!(min, 50);

            // Heartbeat l1 and ensure it extends expiry without affecting txn id
            store.heartbeat(&l1, ttl).await.unwrap();

            // Simulate far-future 'now' so all leases appear expired
            let far_future = u64::MAX / 2;
            let active2 = store.list_active(far_future).await.unwrap();
            assert!(active2.is_empty());

            // Release removes from active set
            store.release(l2).await.unwrap();
        })
    }
}

use std::{collections::HashMap, hash::Hash, time::Duration};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    backoff::{classify_error, ExponentialBackoff, RetryClass},
    checkpoint::CheckpointStore,
    head::{HeadJson, HeadStore, PutCondition},
    lease::{LeaseHandle, LeaseStore},
    manifest::{Op, Record, Segment},
    segment::SegmentIo,
    snapshot::{ScanRange, Snapshot},
    store::StoreHandle,
    types::{Error, Result},
};

/// Unified Session: can be read-only or writable; may pin GC via a lease.
pub struct Session<K, V, HS, SS, CS, LS>
where
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    store: StoreHandle<HS, SS, CS, LS>,
    lease: Option<LeaseHandle>,
    snapshot: Snapshot,
    writable: bool,
    pinned: bool,
    ttl: Duration,
    staged: Vec<(K, Op, Option<V>)>,
}

impl<K, V, HS, SS, CS, LS> Session<K, V, HS, SS, CS, LS>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: StoreHandle<HS, SS, CS, LS>,
        lease: Option<LeaseHandle>,
        snapshot: Snapshot,
        writable: bool,
        pinned: bool,
        ttl: Duration,
    ) -> Self {
        Self {
            store,
            lease,
            snapshot,
            writable,
            pinned,
            ttl,
            staged: Vec::new(),
        }
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub async fn heartbeat(&self) -> Result<()> {
        if self.pinned {
            if let Some(lease) = self.lease.as_ref() {
                return self.store.leases.heartbeat(lease, self.ttl).await;
            }
        }
        Ok(())
    }

    pub async fn end(mut self) -> Result<()> {
        if let Some(lease) = self.lease.take() {
            self.store.leases.release(lease).await
        } else {
            Ok(())
        }
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let snap = &self.snapshot;
        if let Some(last_seq) = snap.last_segment_seq {
            let start_seq = snap
                .checkpoint_seq
                .map(|s| s.saturating_add(1))
                .unwrap_or(0);
            let mut all: Vec<crate::types::SegmentId> = Vec::new();
            let mut cursor = start_seq;
            loop {
                let mut ids = self.store.segment.list_from(cursor, 512).await?;
                if ids.is_empty() {
                    break;
                }
                ids.retain(|id| id.seq >= start_seq && id.seq <= last_seq);
                if ids.is_empty() {
                    break;
                }
                cursor = ids.iter().map(|s| s.seq).max().unwrap_or(cursor) + 1;
                all.extend(ids);
                if cursor > last_seq {
                    break;
                }
            }
            all.sort_by_key(|s| s.seq);
            for id in all.into_iter().rev() {
                let bytes = self.store.segment.get(&id).await?;
                let seg: Segment<K, V> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Corrupt(format!("kv segment decode: {e}")))?;
                if seg.txn_id > snap.txn_id.0 {
                    continue;
                }
                for r in seg.records.into_iter().rev() {
                    if &r.key == key {
                        return Ok(match r.op {
                            Op::Put => r.value,
                            Op::Del => None,
                        });
                    }
                }
            }
            if let Some(ckpt_id) = snap.checkpoint_id.clone() {
                let (_meta, bytes) = self.store.checkpoint.get_checkpoint(&ckpt_id).await?;
                #[derive(Deserialize)]
                struct CkptPayload<K, V> {
                    entries: Vec<(K, V)>,
                }
                let payload: CkptPayload<K, V> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Corrupt(format!("ckpt decode: {e}")))?;
                for (k, v) in payload.entries.into_iter().rev() {
                    if &k == key {
                        return Ok(Some(v));
                    }
                }
            }
            return Ok(None);
        }
        Ok(None)
    }

    pub async fn scan(&self) -> Result<Vec<(K, V)>> {
        let mut map: HashMap<K, V> = HashMap::new();
        self.fold_until(&mut map, &self.snapshot).await?;
        Ok(map.into_iter().collect())
    }

    pub async fn scan_range(&self, range: ScanRange<K>) -> Result<Vec<(K, V)>> {
        let mut map: HashMap<K, V> = HashMap::new();
        self.fold_until(&mut map, &self.snapshot).await?;
        let mut out: Vec<(K, V)> = map.into_iter().collect();
        if let Some(start) = range.start.as_ref() {
            out.retain(|(k, _)| k >= start);
        }
        if let Some(end) = range.end.as_ref() {
            out.retain(|(k, _)| k < end);
        }
        Ok(out)
    }

    async fn fold_until(&self, map: &mut HashMap<K, V>, snap: &Snapshot) -> Result<()> {
        let last_seq = match snap.last_segment_seq {
            Some(s) => s,
            None => return Ok(()),
        };
        let mut cursor = snap
            .checkpoint_seq
            .map(|s| s.saturating_add(1))
            .unwrap_or(0);
        loop {
            let ids = self.store.segment.list_from(cursor, 256).await?;
            if ids.is_empty() {
                break;
            }
            for id in ids {
                if id.seq > last_seq {
                    return Ok(());
                }
                let bytes = self.store.segment.get(&id).await?;
                let seg: Segment<K, V> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Corrupt(format!("kv segment decode: {e}")))?;
                if seg.txn_id > snap.txn_id.0 {
                    return Ok(());
                }
                for r in seg.records.into_iter() {
                    match r.op {
                        Op::Put => {
                            if let Some(v) = r.value {
                                map.insert(r.key, v);
                            }
                        }
                        Op::Del => {
                            map.remove(&r.key);
                        }
                    }
                }
                cursor = id.seq + 1;
            }
        }
        Ok(())
    }

    pub fn put(&mut self, k: K, v: V) -> Result<()> {
        if !self.writable {
            return Err(Error::Unimplemented("put on read-only session"));
        }
        self.staged.push((k, Op::Put, Some(v)));
        Ok(())
    }

    pub fn delete(&mut self, k: K) -> Result<()> {
        if !self.writable {
            return Err(Error::Unimplemented("delete on read-only session"));
        }
        self.staged.push((k, Op::Del, None));
        Ok(())
    }

    fn clone_via_json<T: Serialize + DeserializeOwned>(t: &T) -> Result<T> {
        let bytes = serde_json::to_vec(t)
            .map_err(|e| Error::Corrupt(format!("local clone encode: {e}")))?;
        let val = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Corrupt(format!("local clone decode: {e}")))?;
        Ok(val)
    }

    /// Read-your-writes for a single key. Does not affect global visibility.
    pub async fn get_local(&self, key: &K) -> Result<Option<V>> {
        if self.writable {
            for (k, op, v) in self.staged.iter().rev() {
                if k == key {
                    return Ok(match op {
                        Op::Del => None,
                        Op::Put => match v {
                            Some(val) => Some(Self::clone_via_json(val)?),
                            None => None,
                        },
                    });
                }
            }
        }
        self.get(key).await
    }

    /// Read-your-writes for a range or full scan.
    pub async fn scan_local(&self, range: Option<ScanRange<K>>) -> Result<Vec<(K, V)>> {
        let mut base_entries: Vec<(K, V)> = match range.as_ref() {
            None => self.scan().await?,
            Some(r) => {
                self.scan_range(ScanRange {
                    start: r.start.as_ref().map(Self::clone_via_json).transpose()?,
                    end: r.end.as_ref().map(Self::clone_via_json).transpose()?,
                })
                .await?
            }
        };
        let mut map: HashMap<K, V> = base_entries.drain(..).collect();

        if self.writable {
            for (k, op, v) in &self.staged {
                match op {
                    Op::Del => {
                        map.remove(k);
                    }
                    Op::Put => {
                        if let Some(val) = v.as_ref() {
                            map.insert(Self::clone_via_json(k)?, Self::clone_via_json(val)?);
                        }
                    }
                }
            }
        }

        let mut out: Vec<(K, V)> = map.into_iter().collect();
        if let Some(r) = range.as_ref() {
            if let Some(start) = r.start.as_ref() {
                out.retain(|(k, _)| k >= start);
            }
            if let Some(end) = r.end.as_ref() {
                out.retain(|(k, _)| k < end);
            }
        }
        Ok(out)
    }

    /// Commit staged changes. Only valid for writable sessions.
    pub async fn commit(mut self) -> Result<()> {
        if !self.writable {
            return Err(Error::Unimplemented("commit on read-only session"));
        }
        // Determine new transaction id and segment seq based on our snapshot
        let base_txn = self.snapshot.txn_id.0;
        let next_txn = base_txn.saturating_add(1);
        let next_seq = self
            .snapshot
            .last_segment_seq
            .unwrap_or(0)
            .saturating_add(1);
        let expected_tag = self.snapshot.head_tag.clone();

        // Encode the staged operations once; retry loop reuses the payload.
        let staged = core::mem::take(&mut self.staged);
        let mut records = Vec::with_capacity(staged.len());
        for (k, op, v) in staged.into_iter() {
            records.push(Record {
                key: k,
                op,
                value: v,
            });
        }
        let seg = Segment {
            txn_id: next_txn,
            records,
        };
        let payload =
            serde_json::to_vec(&seg).map_err(|e| Error::Corrupt(format!("kv encode: {e}")))?;
        let new_head = HeadJson {
            version: 1,
            snapshot: None,
            last_segment_seq: Some(next_seq),
            last_txn_id: next_txn,
        };

        let mut seg_written = false;
        let mut bo = ExponentialBackoff::new(self.store.opts.backoff);
        let timer = self.store.opts.timer.clone();

        loop {
            // Reload HEAD to ensure our snapshot tag is still current.
            let current = match self.store.head.load().await {
                Ok(v) => v,
                Err(e) => match classify_error(&e) {
                    RetryClass::RetryTransient if !bo.exhausted() => {
                        let delay = bo.next_delay();
                        timer.sleep(delay).await;
                        continue;
                    }
                    _ => return Err(e),
                },
            };

            let stale = match (expected_tag.as_ref(), current.as_ref()) {
                (None, None) => false,
                (Some(t), Some((_h, cur))) => t != cur,
                (None, Some(_)) => true,
                (Some(_), None) => true,
            };
            if stale {
                return Err(Error::PreconditionFailed);
            }

            if !seg_written {
                match self
                    .store
                    .segment
                    .put_next(next_seq, payload.as_slice(), "application/json")
                    .await
                {
                    Ok(_) => {
                        seg_written = true;
                    }
                    Err(e) => match classify_error(&e) {
                        RetryClass::RetryTransient if !bo.exhausted() => {
                            let delay = bo.next_delay();
                            timer.sleep(delay).await;
                            continue;
                        }
                        _ => return Err(e),
                    },
                }
            }

            let cond = match expected_tag.as_ref() {
                Some(tag) => PutCondition::IfMatch(tag.clone()),
                None => PutCondition::IfNotExists,
            };

            match self.store.head.put(&new_head, cond).await {
                Ok(_) => {
                    if let Some(lease) = self.lease.take() {
                        let _ = self.store.leases.release(lease).await;
                    }
                    return Ok(());
                }
                Err(e) => match classify_error(&e) {
                    RetryClass::RetryTransient if !bo.exhausted() => {
                        let delay = bo.next_delay();
                        timer.sleep(delay).await;
                        continue;
                    }
                    RetryClass::DurableConflict => return Err(Error::PreconditionFailed),
                    _ => return Err(e),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;

    use super::*;
    use crate::impls::mem::{
        checkpoint::MemCheckpointStore, head::MemHeadStore, lease::MemLeaseStore,
        segment::MemSegmentStore,
    };

    #[test]
    fn commit_preflight_rejects_stale_snapshot() {
        block_on(async move {
            let head = MemHeadStore::new();
            let seg = MemSegmentStore::new();
            let ck = MemCheckpointStore::new();
            let ls = MemLeaseStore::new();
            let m: crate::manifest::Manifest<String, String, _, _, _, _> =
                crate::manifest::Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

            let mut a = m.session_write().await.unwrap();
            a.put("k".into(), "v1".into()).unwrap();

            let mut b = m.session_write().await.unwrap();
            b.put("k".into(), "v2".into()).unwrap();
            let _ = b.commit().await.unwrap();

            let r = a.commit().await;
            assert!(matches!(r, Err(crate::types::Error::PreconditionFailed)));

            let segs = seg.list_from(0, 1024).await.unwrap();
            assert_eq!(segs.len(), 1);
        })
    }
}

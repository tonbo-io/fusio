use core::marker::PhantomData;
use std::collections::BTreeMap;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    checkpoint::CheckpointStore,
    db::{Op, Record, Segment},
    head::{HeadJson, HeadStore, PutCondition},
    lease::{LeaseHandle, LeaseStore},
    segment::SegmentIo,
    snapshot::{ScanRange, Snapshot},
    types::{Commit, Error, Lsn, Result},
};

/// Unified Session: can be read-only or writable; may pin GC via a lease.
pub struct Session<K, V, HS, SS, CS, LS>
where
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    _phantom: PhantomData<(K, V)>,
    head: HS,
    seg: SS,
    ckpt: CS,
    leases: LS,
    lease: Option<LeaseHandle>,
    snapshot: Snapshot,
    writable: bool,
    pinned: bool,
    ttl_ms: u64,
    staged: Vec<(K, Op, Option<V>)>,
}

impl<K, V, HS, SS, CS, LS> Session<K, V, HS, SS, CS, LS>
where
    K: Ord + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        head: HS,
        seg: SS,
        ckpt: CS,
        leases: LS,
        lease: Option<LeaseHandle>,
        snapshot: Snapshot,
        writable: bool,
        pinned: bool,
        ttl_ms: u64,
    ) -> Self {
        Self {
            _phantom: PhantomData,
            head,
            seg,
            ckpt,
            leases,
            lease,
            snapshot,
            writable,
            pinned,
            ttl_ms,
            staged: Vec::new(),
        }
    }

    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub async fn heartbeat(&self) -> Result<()> {
        if self.pinned {
            if let Some(lease) = self.lease.as_ref() {
                return self.leases.heartbeat(lease, self.ttl_ms).await;
            }
        }
        Ok(())
    }

    pub async fn end(mut self) -> Result<()> {
        if let Some(lease) = self.lease.take() {
            self.leases.release(lease).await
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
                let mut ids = self.seg.list_from(cursor, 512).await?;
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
                let bytes = self.seg.get(&id).await?;
                let seg: Segment<K, V> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Corrupt(format!("kv segment decode: {e}")))?;
                for r in seg.records.into_iter().rev() {
                    if r.lsn > snap.lsn.0 {
                        continue;
                    }
                    if &r.key == key {
                        return Ok(match r.op {
                            Op::Put => r.value,
                            Op::Del => None,
                        });
                    }
                }
            }
            if let Some(ckpt_id) = snap.checkpoint_id.clone() {
                let (_meta, bytes) = self.ckpt.get_checkpoint(&ckpt_id).await?;
                #[derive(Deserialize)]
                struct CkptPayload<K, V> {
                    version: u32,
                    entries: Vec<(K, V)>,
                }
                let payload: CkptPayload<K, V> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Corrupt(format!("ckpt decode: {e}")))?;
                if payload.version != 1 {
                    return Err(Error::Corrupt(format!(
                        "unsupported checkpoint version: {}",
                        payload.version
                    )));
                }
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
        let mut map: BTreeMap<K, V> = BTreeMap::new();
        self.fold_until(&mut map, &self.snapshot).await?;
        Ok(map.into_iter().collect())
    }

    pub async fn scan_range(&self, range: ScanRange<K>) -> Result<Vec<(K, V)>> {
        let mut map: BTreeMap<K, V> = BTreeMap::new();
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

    async fn fold_until(&self, map: &mut BTreeMap<K, V>, snap: &Snapshot) -> Result<()> {
        let last_seq = match snap.last_segment_seq {
            Some(s) => s,
            None => return Ok(()),
        };
        let mut cursor = snap
            .checkpoint_seq
            .map(|s| s.saturating_add(1))
            .unwrap_or(0);
        loop {
            let ids = self.seg.list_from(cursor, 256).await?;
            if ids.is_empty() {
                break;
            }
            for id in ids {
                if id.seq > last_seq {
                    return Ok(());
                }
                let bytes = self.seg.get(&id).await?;
                let seg: Segment<K, V> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Corrupt(format!("kv segment decode: {e}")))?;
                for r in seg.records.into_iter() {
                    if r.lsn > snap.lsn.0 {
                        // LSNs are monotonic; later segments will only contain larger LSNs.
                        return Ok(());
                    }
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
        let mut map: BTreeMap<K, V> = match range.as_ref() {
            None => self.scan().await?.into_iter().collect::<BTreeMap<K, V>>(),
            Some(r) => self
                .scan_range(ScanRange {
                    start: r.start.as_ref().map(Self::clone_via_json).transpose()?,
                    end: r.end.as_ref().map(Self::clone_via_json).transpose()?,
                })
                .await?
                .into_iter()
                .collect::<BTreeMap<K, V>>(),
        };

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
    pub async fn commit(mut self) -> Result<Commit> {
        if !self.writable {
            return Err(Error::Unimplemented("commit on read-only session"));
        }
        // Preflight CAS: if our snapshot head tag is stale, fail early to avoid orphaning
        let current = self.head.load().await?;
        let stale = match (self.snapshot.head_tag.as_ref(), current.as_ref()) {
            (None, None) => false,
            (Some(t), Some((_h, cur))) => t != cur,
            (None, Some(_)) => true,
            (Some(_), None) => true,
        };
        if stale {
            return Err(Error::PreconditionFailed);
        }
        // Determine new lsn range and segment seq
        let base_lsn = self.snapshot.lsn.0;
        let new_lsn = base_lsn + self.staged.len() as u64;
        let next_seq = self
            .snapshot
            .last_segment_seq
            .unwrap_or(0)
            .saturating_add(1);

        // Encode segment
        let mut lsn = base_lsn;
        let mut records: Vec<Record<K, V>> = Vec::with_capacity(self.staged.len());
        for (k, op, v) in self.staged.drain(..) {
            let key = k;
            let val = v;
            lsn += 1;
            records.push(Record {
                lsn,
                key,
                op,
                value: val,
            });
        }
        let seg = Segment {
            version: 1,
            records,
        };
        let payload =
            serde_json::to_vec(&seg).map_err(|e| Error::Corrupt(format!("kv encode: {e}")))?;

        // Write segment and publish HEAD via CAS
        let seg_id = self
            .seg
            .put_next(next_seq, &payload, "application/json")
            .await?;

        let new_head = HeadJson {
            version: 1,
            snapshot: None,
            last_segment_seq: Some(seg_id.seq),
            last_lsn: new_lsn,
        };
        let tag = match self.snapshot.head_tag.take() {
            None => self.head.put(&new_head, PutCondition::IfNotExists).await?,
            Some(t) => self.head.put(&new_head, PutCondition::IfMatch(t)).await?,
        };
        // release lease on successful commit
        if let Some(lease) = self.lease.take() {
            let _ = self.leases.release(lease).await;
        }
        let _ = tag; // currently unused beyond CAS success
        Ok(Commit {
            lsn: Lsn(new_lsn),
            segment_id: Some(seg_id),
        })
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
            let m: crate::db::Manifest<String, String, _, _, _, _> =
                crate::db::Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

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

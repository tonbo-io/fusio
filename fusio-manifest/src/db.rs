use core::marker::PhantomData;
use std::collections::BTreeMap;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    checkpoint::{CheckpointId, CheckpointMeta, CheckpointStore},
    head::{HeadJson, HeadStore, HeadTag, PutCondition},
    lease::LeaseStore,
    options::Options,
    segment::SegmentIo,
    session::Session,
    snapshot::{ScanRange, Snapshot},
    types::{Error, Lsn, Result},
};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Operation on a key (crate-internal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Op {
    Put,
    Del,
}

/// A single KV record framed into a segment (crate-internal).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Record<K, V> {
    pub lsn: u64,
    pub key: K,
    pub op: Op,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<V>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Segment<K, V> {
    pub(crate) version: u32,
    pub(crate) records: Vec<Record<K, V>>,
}

/// KV database facade over HeadStore + SegmentIo + CheckpointStore.
pub struct Manifest<K, V, HS, SS, CS, LS>
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
    opts: Options,
}

impl<K, V, HS, SS, CS, LS> Manifest<K, V, HS, SS, CS, LS>
where
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    pub fn new(head: HS, seg: SS, ckpt: CS, leases: LS) -> Self {
        Self {
            _phantom: PhantomData,
            head,
            seg,
            ckpt,
            leases,
            opts: Options::default(),
        }
    }

    pub fn new_with_opts(head: HS, seg: SS, ckpt: CS, leases: LS, opts: Options) -> Self {
        Self {
            _phantom: PhantomData,
            head,
            seg,
            ckpt,
            leases,
            opts,
        }
    }
}

impl<K, V, HS, SS, CS, LS> Manifest<K, V, HS, SS, CS, LS>
where
    K: Ord + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    /// Attempt to adopt durable-but-unpublished segments by scanning for a
    /// contiguous run immediately after the current HEAD and CAS-advancing HEAD.
    ///
    /// Returns the number of segments adopted. If a concurrent actor advances
    /// HEAD first, this becomes a no-op (returns 0).
    ///
    /// TODO:
    /// - Add bounded backoff when CAS fails repeatedly; surface metrics.
    /// - Consider verifying segment continuity more strictly (e.g., ensure LSNs are strictly
    ///   increasing without gaps and optionally validate a checksum header).
    /// - Add a hard cap on segments scanned per recovery pass; loop with yield if needed.
    /// - Real-S3 black-box test to simulate orphan segments and verify adoption.
    pub async fn recover_orphans(&self) -> Result<usize> {
        // Load current head once and derive CAS condition.
        let loaded = self.head.load().await?;
        let (cur, cond) = match loaded.clone() {
            None => (
                HeadJson {
                    version: 1,
                    snapshot: None,
                    last_segment_seq: None,
                    last_lsn: 0,
                },
                PutCondition::IfNotExists,
            ),
            Some((h, tag)) => (h, PutCondition::IfMatch(tag)),
        };

        // Determine starting sequence to probe.
        let mut expected: u64 = cur.last_segment_seq.unwrap_or(0).saturating_add(1);
        if expected == 0 {
            expected = 1;
        }

        let mut adopted = 0usize;
        let mut max_seq = cur.last_segment_seq.unwrap_or(0);
        let mut last_lsn = cur.last_lsn;

        'outer: loop {
            // List a window starting at the expected next sequence.
            let ids = self.seg.list_from(expected, 256).await?;
            if ids.is_empty() {
                break;
            }
            let mut progressed = false;
            for id in ids.into_iter() {
                if id.seq < expected {
                    continue;
                }
                if id.seq > expected {
                    // Found a gap; stop adopting.
                    break 'outer;
                }
                let bytes = self.seg.get(&id).await?;
                // Ensure the segment decodes and its last LSN moves forward.
                // TODO: consider reading a small header/summary instead of full decode
                // once segments carry compact headers.
                let seg: Segment<K, V> = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Corrupt(format!("kv segment decode: {e}")))?;
                match seg.records.last() {
                    Some(r) if r.lsn > last_lsn => {
                        last_lsn = r.lsn;
                        max_seq = id.seq;
                        expected = expected.saturating_add(1);
                        adopted += 1;
                        progressed = true;
                    }
                    _ => {
                        // Empty or non-monotonic — do not adopt further.
                        break 'outer;
                    }
                }
            }
            if !progressed {
                break;
            }
        }

        if adopted == 0 {
            return Ok(0);
        }

        // Publish updated HEAD via CAS. If CAS fails, treat as benign race.
        let new_head = HeadJson {
            version: cur.version.max(1),
            snapshot: cur.snapshot.clone(),
            last_segment_seq: Some(max_seq),
            last_lsn,
        };
        match self.head.put(&new_head, cond).await {
            Ok(_t) => Ok(adopted),
            Err(Error::PreconditionFailed) => Ok(0),
            Err(e) => Err(e),
        }
    }
}

/// Convenience constructors and alias for an in-memory Manifest using Mem backends.
// The in-memory constructors and alias moved to `impls::mem` (test-only).

impl<K, V, HS, SS, CS, LS> Manifest<K, V, HS, SS, CS, LS>
where
    K: Ord + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    /// NEW: Open a read session. When `pin` is true, pins GC via a lease.
    pub async fn session_read(&self, pin: bool) -> Result<Session<K, V, HS, SS, CS, LS>> {
        let snap = self.snapshot().await?;
        if pin {
            let ttl = self.opts.retention.lease_ttl_ms;
            let lease = self
                .leases
                .create(snap.lsn.0, snap.head_tag.clone(), ttl)
                .await?;
            Ok(Session::new(
                self.head.clone(),
                self.seg.clone(),
                self.ckpt.clone(),
                self.leases.clone(),
                Some(lease),
                snap,
                false,
                true,
                ttl,
            ))
        } else {
            Ok(Session::new(
                self.head.clone(),
                self.seg.clone(),
                self.ckpt.clone(),
                self.leases.clone(),
                None,
                snap,
                false,
                false,
                self.opts.retention.lease_ttl_ms,
            ))
        }
    }

    /// NEW: Open a write session (pinned, with lease)
    pub async fn session_write(&self) -> Result<Session<K, V, HS, SS, CS, LS>> {
        // Opportunistically adopt durable-but-unpublished segments before opening a writer.
        let _ = self.recover_orphans().await;
        let snap = self.snapshot().await?;
        let ttl = self.opts.retention.lease_ttl_ms;
        let lease = self
            .leases
            .create(snap.lsn.0, snap.head_tag.clone(), ttl)
            .await?;
        Ok(Session::new(
            self.head.clone(),
            self.seg.clone(),
            self.ckpt.clone(),
            self.leases.clone(),
            Some(lease),
            snap,
            true,
            true,
            ttl,
        ))
    }

    /// NEW: Create a session bound to an explicit snapshot (read-only, no pin)
    pub fn session_at(&self, snapshot: Snapshot) -> Session<K, V, HS, SS, CS, LS> {
        Session::new(
            self.head.clone(),
            self.seg.clone(),
            self.ckpt.clone(),
            self.leases.clone(),
            None,
            snapshot,
            false,
            false,
            self.opts.retention.lease_ttl_ms,
        )
    }

    /// One-shot latest get (no pin)
    pub async fn get_latest(&self, key: &K) -> Result<Option<V>> {
        let snap = self.snapshot().await?;
        let sess = self.session_at(snap);
        sess.get(key).await
    }

    /// One-shot latest scan (no pin)
    pub async fn scan_latest(&self, range: Option<ScanRange<K>>) -> Result<Vec<(K, V)>> {
        let snap = self.snapshot().await?;
        let sess = self.session_at(snap);
        match range {
            None => sess.scan().await,
            Some(r) => sess.scan_range(r).await,
        }
    }

    pub async fn snapshot(&self) -> Result<Snapshot> {
        match self.head.load().await? {
            None => Ok(Snapshot {
                head_tag: None,
                lsn: Lsn(0),
                last_segment_seq: None,
                checkpoint_seq: None,
                checkpoint_id: None,
            }),
            Some((h, tag)) => {
                let (checkpoint_id, checkpoint_seq) = if let Some(id) = h.snapshot.as_ref() {
                    let id = CheckpointId(id.clone());
                    let (meta, _payload) = self.ckpt.get_checkpoint(&id).await?;
                    (Some(id), Some(meta.last_segment_seq_at_ckpt))
                } else {
                    (None, None)
                };
                Ok(Snapshot {
                    head_tag: Some(tag),
                    lsn: Lsn(h.last_lsn),
                    last_segment_seq: h.last_segment_seq,
                    checkpoint_seq,
                    checkpoint_id,
                })
            }
        }
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

    /// Build and publish a checkpoint at a stable snapshot. Returns the checkpoint id and HEAD tag.
    pub async fn compact_once(&self) -> Result<(CheckpointId, HeadTag)> {
        // 1) Capture snapshot S
        let snap = self.snapshot().await?;
        // 2) Fold state up to S
        let mut map: BTreeMap<K, V> = BTreeMap::new();
        self.fold_until(&mut map, &snap).await?;
        // 3) Encode checkpoint payload
        #[derive(Serialize)]
        struct CkptPayload<K, V> {
            version: u32,
            entries: Vec<(K, V)>,
        }
        let entries: Vec<(K, V)> = map.into_iter().collect();
        let entries_len = entries.len();
        let payload = serde_json::to_vec(&CkptPayload {
            version: 1,
            entries,
        })
        .map_err(|e| Error::Corrupt(format!("ckpt encode: {e}")))?;
        // 3b) Meta
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let meta = CheckpointMeta {
            lsn: snap.lsn.0,
            key_count: entries_len,
            byte_size: payload.len(),
            created_at_ms: now_ms,
            format: "application/json".into(),
            last_segment_seq_at_ckpt: snap.last_segment_seq.unwrap_or(0),
        };
        let id = self
            .ckpt
            .put_checkpoint(&meta, &payload, "application/json")
            .await?;
        // 4) Publish checkpoint into HEAD via CAS
        match self.head.load().await? {
            None => {
                // Initialize HEAD with checkpoint if empty
                let new_head = HeadJson {
                    version: 1,
                    snapshot: Some(id.0.clone()),
                    last_segment_seq: None,
                    last_lsn: 0,
                };
                // Bounded retry on transient errors only
                let pol = self.opts.backoff;
                let mut bo = crate::backoff::ExponentialBackoff::new(pol);
                let tag = loop {
                    match self.head.put(&new_head, PutCondition::IfNotExists).await {
                        Ok(t) => break t,
                        Err(Error::PreconditionFailed) => return Err(Error::PreconditionFailed),
                        Err(e) => match crate::backoff::classify_error(&e) {
                            crate::backoff::RetryClass::RetryTransient if !bo.exhausted() => {
                                let sleeper = {
                                    #[cfg(feature = "exec")]
                                    {
                                        self.opts.sleeper.as_deref()
                                    }
                                    #[cfg(not(feature = "exec"))]
                                    {
                                        None::<&()>
                                    }
                                };
                                crate::backoff::sleep_ms_with(bo.next_delay_ms(), sleeper).await;
                                continue;
                            }
                            _ => return Err(e),
                        },
                    }
                };
                Ok((id, tag))
            }
            Some((cur, cur_tag)) => {
                let new_head = HeadJson {
                    version: cur.version,
                    snapshot: Some(id.0.clone()),
                    last_segment_seq: cur.last_segment_seq,
                    last_lsn: cur.last_lsn,
                };
                let pol = self.opts.backoff;
                let mut bo = crate::backoff::ExponentialBackoff::new(pol);
                let tag = loop {
                    match self
                        .head
                        .put(&new_head, PutCondition::IfMatch(cur_tag.clone()))
                        .await
                    {
                        Ok(t) => break t,
                        Err(Error::PreconditionFailed) => return Err(Error::PreconditionFailed),
                        Err(e) => match crate::backoff::classify_error(&e) {
                            crate::backoff::RetryClass::RetryTransient if !bo.exhausted() => {
                                let sleeper = {
                                    #[cfg(feature = "exec")]
                                    {
                                        self.opts.sleeper.as_deref()
                                    }
                                    #[cfg(not(feature = "exec"))]
                                    {
                                        None::<&()>
                                    }
                                };
                                crate::backoff::sleep_ms_with(bo.next_delay_ms(), sleeper).await;
                                continue;
                            }
                            _ => return Err(e),
                        },
                    }
                };
                Ok((id, tag))
            }
        }
    }

    /// Compact and then attempt GC of legacy segments and checkpoints based on active leases.
    pub async fn compact_and_gc(&self) -> Result<(CheckpointId, HeadTag)> {
        let (ckpt_id, tag) = self.compact_once().await?;
        // Guard deletes with head tag: if head has advanced, skip GC.
        match self.head.load().await? {
            Some((_h, cur_tag)) if cur_tag != tag => return Ok((ckpt_id, tag)),
            _ => {}
        }
        // Compute watermark from live leases
        let now_ms_u64: u64 = now_ms();
        let leases = self.leases.list_active(now_ms_u64).await?;
        let watermark = leases
            .iter()
            .map(|l| l.snapshot_lsn)
            .min()
            .unwrap_or(u64::MAX);
        let (meta, _bytes) = self.ckpt.get_checkpoint(&ckpt_id).await?;
        if watermark > meta.lsn {
            let _ = self.seg.delete_upto(meta.last_segment_seq_at_ckpt).await;
        }
        // Checkpoint GC: keep newest and the floor (<= watermark), drop others after TTL.
        let ttl_ms: u64 = self.opts.retention.checkpoints_min_ttl_ms;
        let now_ms2: u64 = now_ms();
        let mut list = self.ckpt.list().await.unwrap_or_default();
        // sort by lsn ascending
        list.sort_by_key(|(_id, m)| m.lsn);
        let newest = list.iter().max_by_key(|(_id, m)| m.lsn).cloned();
        let floor = list
            .iter()
            .filter(|(_id, m)| m.lsn <= watermark)
            .max_by_key(|(_id, m)| m.lsn)
            .cloned();
        // Keep last N newest checkpoints regardless of watermark
        let keep_last = self.opts.retention.checkpoints_keep_last;
        let newest_ids: std::collections::BTreeSet<_> = list
            .iter()
            .rev()
            .take(keep_last)
            .map(|(id, _)| id.clone())
            .collect();
        for (id, m) in list.into_iter() {
            let keep_newest = newest.as_ref().map(|(i, _)| i == &id).unwrap_or(false);
            let keep_floor = floor.as_ref().map(|(i, _)| i == &id).unwrap_or(false);
            let keep_in_last = newest_ids.contains(&id);
            let age_ok = now_ms2.saturating_sub(m.created_at_ms) >= ttl_ms;
            if !keep_newest && !keep_floor && !keep_in_last && age_ok {
                let _ = self.ckpt.delete(&id).await;
            }
        }
        Ok((ckpt_id, tag))
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
    fn mem_kv_end_to_end_and_conflict() {
        block_on(async move {
            let head = MemHeadStore::new();
            let seg = MemSegmentStore::new();
            let ck = MemCheckpointStore::new();
            let ls = MemLeaseStore::new();
            let kv: Manifest<String, String, _, _, _, _> =
                Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

            // tx1
            let mut s1 = kv.session_write().await.unwrap();
            s1.put("a".into(), "1".into()).unwrap();
            s1.put("b".into(), "2".into()).unwrap();
            let _c1 = s1.commit().await.unwrap();

            // Read latest via explicit snapshot
            let snap = kv.snapshot().await.unwrap();
            let got = kv.session_at(snap.clone()).get(&"a".into()).await.unwrap();
            assert_eq!(got.as_deref(), Some("1"));
            let all = kv.session_at(snap).scan().await.unwrap();
            assert_eq!(all.len(), 2);

            // tx2 and tx3 from same snapshot; tx2 wins, tx3 loses
            let _snap = kv.snapshot().await.unwrap();
            let mut s2 = kv.session_write().await.unwrap();
            let mut s3 = Manifest::<String, String, _, _, _, _>::new(
                head.clone(),
                seg.clone(),
                ck.clone(),
                ls.clone(),
            )
            .session_write()
            .await
            .unwrap();
            s2.put("a".into(), "10".into()).unwrap();
            s3.delete("a".into()).unwrap();
            let _ = s2.commit().await.unwrap();
            let r3 = s3.commit().await;
            assert!(matches!(r3, Err(Error::PreconditionFailed)));

            let snap2 = kv.snapshot().await.unwrap();
            let got2 = kv.session_at(snap2).get(&"a".into()).await.unwrap();
            assert_eq!(got2.as_deref(), Some("10"));
        })
    }

    #[test]
    fn mem_kv_point_get_and_tombstone() {
        block_on(async move {
            let head = crate::impls::mem::head::MemHeadStore::new();
            let seg = crate::impls::mem::segment::MemSegmentStore::new();
            let kv: Manifest<String, String, _, _, _, _> = Manifest::new(
                head.clone(),
                seg.clone(),
                crate::impls::mem::checkpoint::MemCheckpointStore::new(),
                crate::impls::mem::lease::MemLeaseStore::new(),
            );

            // Seed data
            let mut s = kv.session_write().await.unwrap();
            s.put("a".into(), "1".into()).unwrap();
            s.put("b".into(), "2".into()).unwrap();
            let _ = s.commit().await.unwrap();

            // Point gets
            let snap = kv.snapshot().await.unwrap();
            let sess = kv.session_at(snap.clone());
            let ga = sess.get(&"a".into()).await.unwrap();
            let gb = kv.session_at(snap).get(&"b".into()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));
            assert_eq!(gb.as_deref(), Some("2"));

            // Update and delete on new snapshot
            let mut s2 = kv.session_write().await.unwrap();
            s2.put("a".into(), "10".into()).unwrap();
            s2.delete("b".into()).unwrap();
            let _ = s2.commit().await.unwrap();

            let snap3 = kv.snapshot().await.unwrap();
            let sess3 = kv.session_at(snap3.clone());
            let ga2 = sess3.get(&"a".into()).await.unwrap();
            let gb2 = kv.session_at(snap3).get(&"b".into()).await.unwrap();
            assert_eq!(ga2.as_deref(), Some("10"));
            assert_eq!(gb2, None);
        })
    }

    #[test]
    fn mem_kv_compact_and_read_from_checkpoint() {
        block_on(async move {
            let head = crate::impls::mem::head::MemHeadStore::new();
            let seg = crate::impls::mem::segment::MemSegmentStore::new();
            let ck = crate::impls::mem::checkpoint::MemCheckpointStore::new();
            let ls = crate::impls::mem::lease::MemLeaseStore::new();
            let kv: Manifest<String, String, _, _, _, _> =
                Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

            // Two transactions → two segments
            let mut s1 = kv.session_write().await.unwrap();
            s1.put("a".into(), "1".into()).unwrap();
            s1.put("b".into(), "2".into()).unwrap();
            let _ = s1.commit().await.unwrap();

            let mut s2 = kv.session_write().await.unwrap();
            s2.put("b".into(), "20".into()).unwrap();
            s2.put("c".into(), "3".into()).unwrap();
            let _ = s2.commit().await.unwrap();

            // Compact
            let (_ckpt_id, _tag) = kv.compact_once().await.unwrap();

            // Snapshot now should include checkpoint_seq
            let snap = kv.snapshot().await.unwrap();
            assert!(snap.checkpoint_seq.is_some());

            // Reads still reflect latest values
            let sess = kv.session_at(snap);
            let ga = sess.get(&"a".into()).await.unwrap();
            let gb = sess.get(&"b".into()).await.unwrap();
            let gc = sess.get(&"c".into()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));
            assert_eq!(gb.as_deref(), Some("20"));
            assert_eq!(gc.as_deref(), Some("3"));
        })
    }

    #[test]
    fn mem_recover_orphans_no_head_adopts_seq1() {
        block_on(async move {
            let head = crate::impls::mem::head::MemHeadStore::new();
            let seg = crate::impls::mem::segment::MemSegmentStore::new();
            let ck = crate::impls::mem::checkpoint::MemCheckpointStore::new();
            let ls = crate::impls::mem::lease::MemLeaseStore::new();
            let kv: Manifest<String, String, _, _, _, _> =
                Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

            // Manually write an orphan segment at seq=1 with LSNs 1..=2
            let seg_payload: Segment<String, String> = Segment {
                version: 1,
                records: vec![
                    Record::<String, String> {
                        lsn: 1,
                        key: "a".into(),
                        op: Op::Put,
                        value: Some("1".into()),
                    },
                    Record::<String, String> {
                        lsn: 2,
                        key: "b".into(),
                        op: Op::Put,
                        value: Some("2".into()),
                    },
                ],
            };
            let bytes = serde_json::to_vec(&seg_payload).unwrap();
            let _ = seg.put_next(1, &bytes, "application/json").await.unwrap();

            // Recover
            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 1);
            // Head advanced
            let loaded = head.load().await.unwrap().unwrap();
            assert_eq!(loaded.0.last_segment_seq, Some(1));
            assert_eq!(loaded.0.last_lsn, 2);
        })
    }

    #[test]
    fn mem_recover_orphans_gap_does_not_adopt() {
        block_on(async move {
            let head = crate::impls::mem::head::MemHeadStore::new();
            let seg = crate::impls::mem::segment::MemSegmentStore::new();
            let ck = crate::impls::mem::checkpoint::MemCheckpointStore::new();
            let ls = crate::impls::mem::lease::MemLeaseStore::new();
            let kv: Manifest<String, String, _, _, _, _> =
                Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

            // Write orphan only at seq=2 (gap at 1)
            let seg_payload: Segment<String, String> = Segment {
                version: 1,
                records: vec![Record::<String, String> {
                    lsn: 1,
                    key: "a".into(),
                    op: Op::Put,
                    value: Some("1".into()),
                }],
            };
            let bytes = serde_json::to_vec(&seg_payload).unwrap();
            let _ = seg.put_next(2, &bytes, "application/json").await.unwrap();

            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 0);
            assert!(head.load().await.unwrap().is_none());
        })
    }

    #[test]
    fn mem_recover_orphans_advances_over_existing_head() {
        block_on(async move {
            let head = crate::impls::mem::head::MemHeadStore::new();
            let seg = crate::impls::mem::segment::MemSegmentStore::new();
            let ck = crate::impls::mem::checkpoint::MemCheckpointStore::new();
            let ls = crate::impls::mem::lease::MemLeaseStore::new();
            let kv: Manifest<String, String, _, _, _, _> =
                Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

            // Create initial segment and head via normal commit
            {
                let mut s = kv.session_write().await.unwrap();
                s.put("a".into(), "1".into()).unwrap();
                let _ = s.commit().await.unwrap(); // seq=1, lsn=1
            }

            // Manually write next segment (seq=2) without publishing head
            let seg_payload: Segment<String, String> = Segment {
                version: 1,
                records: vec![Record::<String, String> {
                    lsn: 2,
                    key: "b".into(),
                    op: Op::Put,
                    value: Some("2".into()),
                }],
            };
            let bytes = serde_json::to_vec(&seg_payload).unwrap();
            let _ = seg.put_next(2, &bytes, "application/json").await.unwrap();

            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 1);
            let loaded = head.load().await.unwrap().unwrap();
            assert_eq!(loaded.0.last_segment_seq, Some(2));
            assert_eq!(loaded.0.last_lsn, 2);
        })
    }
}

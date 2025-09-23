use std::{hash::Hash, marker::PhantomData, sync::Arc};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    checkpoint::{CheckpointId, CheckpointStore},
    compactor::Compactor,
    head::{HeadJson, HeadStore, PutCondition},
    lease::LeaseStore,
    options::Options,
    segment::SegmentIo,
    session::Session,
    snapshot::{ScanRange, Snapshot},
    store::{Store, StoreHandle},
    types::{Error, Result, TxnId},
};

#[cfg(test)]
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
    pub key: K,
    pub op: Op,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<V>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Segment<K, V> {
    pub(crate) txn_id: u64,
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
    store: StoreHandle<HS, SS, CS, LS>,
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
            store: Arc::new(Store::new(head, seg, ckpt, leases, Options::default())),
        }
    }

    pub fn new_with_opts(head: HS, seg: SS, ckpt: CS, leases: LS, opts: Options) -> Self {
        Self {
            _phantom: PhantomData,
            store: Arc::new(Store::new(head, seg, ckpt, leases, opts)),
        }
    }
}

impl<K, V, HS, SS, CS, LS> Manifest<K, V, HS, SS, CS, LS>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    /// Build a `Compactor` over the same stores and options. Callers can use this to run
    /// compaction/GC cycles without `Manifest` needing to expose those behaviors directly.
    pub fn compactor(&self) -> Compactor<K, V, HS, SS, CS, LS> {
        Compactor::from_store(self.store.clone())
    }

    /// Attempt to adopt durable-but-unpublished segments by scanning for a
    /// contiguous run immediately after the current HEAD and CAS-advancing HEAD.
    ///
    /// Returns the number of segments adopted. If a concurrent actor advances
    /// HEAD first, this becomes a no-op (returns 0).
    ///
    /// TODO:
    /// - Add bounded backoff when CAS fails repeatedly; surface metrics.
    /// - Consider verifying segment continuity more strictly (e.g., ensure txn_ids are strictly
    ///   increasing without gaps and optionally validate a checksum header).
    /// - Add a hard cap on segments scanned per recovery pass; loop with yield if needed.
    pub async fn recover_orphans(&self) -> Result<usize> {
        let timer = &self.store.opts.timer;
        let pol = self.store.opts.backoff;
        let mut bo = crate::backoff::ExponentialBackoff::new(pol);

        loop {
            // Load current head once and derive CAS condition for this attempt.
            let loaded = self.store.head.load().await?;
            let (cur, cond) = match loaded.clone() {
                None => (
                    HeadJson {
                        version: 1,
                        snapshot: None,
                        last_segment_seq: None,
                        last_txn_id: 0,
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

            let mut adopted = 0;
            let mut max_seq = cur.last_segment_seq.unwrap_or(0);
            let mut last_txn = cur.last_txn_id;

            'probe: loop {
                // List a window starting at the expected next sequence.
                let ids = self.store.segment.list_from(expected, 256).await?;
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
                        break 'probe;
                    }
                    let bytes = self.store.segment.get(&id).await?;
                    // Ensure the segment decodes and its transaction id advances the log.
                    // TODO: consider reading a small header/summary instead of full decode
                    // once segments carry compact headers.
                    let seg: Segment<K, V> = serde_json::from_slice(&bytes)
                        .map_err(|e| Error::Corrupt(format!("kv segment decode: {e}")))?;
                    if seg.txn_id <= last_txn {
                        break 'probe;
                    }
                    if seg.txn_id != last_txn.saturating_add(1) {
                        break 'probe;
                    }
                    last_txn = seg.txn_id;
                    max_seq = id.seq;
                    expected = expected.saturating_add(1);
                    adopted += 1;
                    progressed = true;
                }
                if !progressed {
                    break;
                }
            }

            if adopted == 0 {
                return Ok(0);
            }

            // Publish updated HEAD via CAS. Retry on conflicts with backoff until exhausted.
            let new_head = HeadJson {
                version: cur.version.max(1),
                snapshot: cur.snapshot.clone(),
                last_segment_seq: Some(max_seq),
                last_txn_id: last_txn,
            };

            match self.store.head.put(&new_head, cond.clone()).await {
                Ok(_t) => return Ok(adopted),
                Err(Error::PreconditionFailed) => {
                    if bo.exhausted() {
                        return Err(Error::PreconditionFailed);
                    }
                    let delay = bo.next_delay();
                    timer.sleep(delay).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Convenience constructors and alias for an in-memory Manifest using Mem backends.
// The in-memory constructors and alias moved to `impls::mem` (test-only).
impl<K, V, HS, SS, CS, LS> Manifest<K, V, HS, SS, CS, LS>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
{
    /// Open a read session, always acquiring a lease so GC honors the snapshot.
    pub async fn session_read(&self) -> Result<Session<K, V, HS, SS, CS, LS>> {
        let snap = self.snapshot().await?;
        self.session_at(snap).await
    }

    /// Open a write session (pinned, with lease)
    pub async fn session_write(&self) -> Result<Session<K, V, HS, SS, CS, LS>> {
        // Opportunistically adopt durable-but-unpublished segments before opening a writer.
        self.recover_orphans().await?;
        let snap = self.snapshot().await?;
        let ttl = self.store.opts.retention.lease_ttl();
        let lease = self
            .store
            .leases
            .create(snap.txn_id.0, snap.head_tag.clone(), ttl)
            .await?;
        Ok(Session::new(
            self.store.clone(),
            Some(lease),
            snap,
            true,
            true,
            ttl,
        ))
    }

    /// Create a pinned read session from a previously persisted snapshot, acquiring a lease so GC
    /// honors it.
    pub async fn session_at(&self, snapshot: Snapshot) -> Result<Session<K, V, HS, SS, CS, LS>> {
        let ttl = self.store.opts.retention.lease_ttl();
        let lease = self
            .store
            .leases
            .create(snapshot.txn_id.0, snapshot.head_tag.clone(), ttl)
            .await?;
        Ok(Session::new(
            self.store.clone(),
            Some(lease),
            snapshot,
            false,
            true,
            ttl,
        ))
    }

    /// One-shot latest get (no pin)
    pub async fn get_latest(&self, key: &K) -> Result<Option<V>> {
        let snap = self.snapshot().await?;
        let sess = self.session_at(snap).await?;
        sess.get(key).await
    }

    /// One-shot latest scan (no pin)
    pub async fn scan_latest(&self, range: Option<ScanRange<K>>) -> Result<Vec<(K, V)>> {
        let snap = self.snapshot().await?;
        let sess = self.session_at(snap).await?;
        match range {
            None => sess.scan().await,
            Some(r) => sess.scan_range(r).await,
        }
    }

    pub async fn snapshot(&self) -> Result<Snapshot> {
        match self.store.head.load().await? {
            None => Ok(Snapshot {
                head_tag: None,
                txn_id: TxnId(0),
                last_segment_seq: None,
                checkpoint_seq: None,
                checkpoint_id: None,
            }),
            Some((h, tag)) => {
                let (checkpoint_id, checkpoint_seq) = if let Some(id) = h.snapshot.as_ref() {
                    let id = CheckpointId(id.clone());
                    let meta = self.store.checkpoint.get_checkpoint_meta(&id).await?;
                    (Some(id), Some(meta.last_segment_seq_at_ckpt))
                } else {
                    (None, None)
                };
                Ok(Snapshot {
                    head_tag: Some(tag),
                    txn_id: TxnId(h.last_txn_id),
                    last_segment_seq: h.last_segment_seq,
                    checkpoint_seq,
                    checkpoint_id,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use core::pin::Pin;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::SystemTime,
    };

    use fusio::executor::Timer;
    use fusio_core::MaybeSendFuture;
    use futures_executor::block_on;

    use super::*;
    use crate::{
        head::HeadTag,
        impls::mem::{
            checkpoint::MemCheckpointStore, head::MemHeadStore, lease::MemLeaseStore,
            segment::MemSegmentStore,
        },
    };

    #[derive(Debug, Clone, Default)]
    struct NoopSleeper;

    impl Timer for NoopSleeper {
        fn sleep(&self, _dur: core::time::Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
            Box::pin(async move {})
        }

        fn now(&self) -> SystemTime {
            SystemTime::now()
        }
    }

    #[derive(Clone)]
    struct FlakyHeadStore {
        inner: MemHeadStore,
        fail_first: Arc<AtomicBool>,
    }

    impl FlakyHeadStore {
        fn new() -> Self {
            Self {
                inner: MemHeadStore::new(),
                fail_first: Arc::new(AtomicBool::new(true)),
            }
        }
    }

    impl HeadStore for FlakyHeadStore {
        fn load(
            &self,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, Error>>>>
        {
            self.inner.load()
        }

        fn put(
            &self,
            head: &HeadJson,
            cond: PutCondition,
        ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<HeadTag, Error>>>> {
            if self.fail_first.swap(false, Ordering::SeqCst) {
                Box::pin(async move { Err(Error::PreconditionFailed) })
            } else {
                self.inner.put(head, cond)
            }
        }
    }

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
            let mut sess = kv.session_at(snap.clone()).await.unwrap();
            let got = sess.get(&"a".into()).await.unwrap();
            assert_eq!(got.as_deref(), Some("1"));
            let mut sess_again = kv.session_at(snap).await.unwrap();
            let all = sess_again.scan().await.unwrap();
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
            let mut sess2 = kv.session_at(snap2).await.unwrap();
            let got2 = sess2.get(&"a".into()).await.unwrap();
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
            let mut sess = kv.session_at(snap.clone()).await.unwrap();
            let ga = sess.get(&"a".into()).await.unwrap();
            let mut sess_b = kv.session_at(snap).await.unwrap();
            let gb = sess_b.get(&"b".into()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));
            assert_eq!(gb.as_deref(), Some("2"));

            // Update and delete on new snapshot
            let mut s2 = kv.session_write().await.unwrap();
            s2.put("a".into(), "10".into()).unwrap();
            s2.delete("b".into()).unwrap();
            let _ = s2.commit().await.unwrap();

            let snap3 = kv.snapshot().await.unwrap();
            let mut sess3 = kv.session_at(snap3.clone()).await.unwrap();
            let ga2 = sess3.get(&"a".into()).await.unwrap();
            let mut sess4 = kv.session_at(snap3).await.unwrap();
            let gb2 = sess4.get(&"b".into()).await.unwrap();
            assert_eq!(ga2.as_deref(), Some("10"));
            assert_eq!(gb2, None);
        })
    }

    #[test]
    fn mem_session_at_acquires_and_releases_lease() {
        block_on(async move {
            let head = crate::impls::mem::head::MemHeadStore::new();
            let seg = crate::impls::mem::segment::MemSegmentStore::new();
            let ck = crate::impls::mem::checkpoint::MemCheckpointStore::new();
            let ls = crate::impls::mem::lease::MemLeaseStore::new();
            let kv: Manifest<String, String, _, _, _, _> =
                Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

            let mut s = kv.session_write().await.unwrap();
            s.put("a".into(), "1".into()).unwrap();
            let _ = s.commit().await.unwrap();

            let snap = kv.snapshot().await.unwrap();
            assert!(ls.list_active(now_ms()).await.unwrap().is_empty());

            let mut pinned = kv.session_at(snap.clone()).await.unwrap();
            let active = ls.list_active(now_ms()).await.unwrap();
            assert_eq!(active.len(), 1);
            let ga = pinned.get(&"a".into()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));

            pinned.end().await.unwrap();
            let active_after = ls.list_active(now_ms()).await.unwrap();
            assert!(active_after.is_empty());
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
            let compactor = kv.compactor();
            let (_ckpt_id, _tag) = compactor.compact_once().await.unwrap();

            // Snapshot now should include checkpoint_seq
            let snap = kv.snapshot().await.unwrap();
            assert!(snap.checkpoint_seq.is_some());

            // Reads still reflect latest values
            let mut sess = kv.session_at(snap).await.unwrap();
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

            // Manually write an orphan segment at seq=1 with txn_id=1
            let seg_payload: Segment<String, String> = Segment {
                txn_id: 1,
                records: vec![
                    Record::<String, String> {
                        key: "a".into(),
                        op: Op::Put,
                        value: Some("1".into()),
                    },
                    Record::<String, String> {
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
            assert_eq!(loaded.0.last_txn_id, 1);
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
                txn_id: 1,
                records: vec![Record::<String, String> {
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
                let _ = s.commit().await.unwrap(); // seq=1, txn_id=1
            }

            // Manually write next segment (seq=2) without publishing head
            let seg_payload: Segment<String, String> = Segment {
                txn_id: 2,
                records: vec![Record::<String, String> {
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
            assert_eq!(loaded.0.last_txn_id, 2);
        })
    }

    #[test]
    fn mem_recover_orphans_retries_on_conflicting_head() {
        block_on(async move {
            let head = FlakyHeadStore::new();
            let seg = MemSegmentStore::new();
            let ck = MemCheckpointStore::new();
            let ls = MemLeaseStore::new();

            let timer: crate::backoff::TimerHandle = Arc::new(NoopSleeper::default());
            let mut opts = Options::default().with_timer(timer);
            let mut backoff = crate::backoff::BackoffPolicy::default();
            backoff.base_ms = 1;
            backoff.max_ms = 1;
            backoff.multiplier_times_100 = 100;
            backoff.jitter_frac_times_100 = 0;
            backoff.max_retries = 4;
            backoff.max_elapsed_ms = 10;
            opts.backoff = backoff;

            let kv: Manifest<String, String, _, _, _, _> =
                Manifest::new_with_opts(head.clone(), seg.clone(), ck.clone(), ls.clone(), opts);

            // Manually write an orphan segment at seq=1 with txn_id=1
            let seg_payload: Segment<String, String> = Segment {
                txn_id: 1,
                records: vec![
                    Record::<String, String> {
                        key: "a".into(),
                        op: Op::Put,
                        value: Some("1".into()),
                    },
                    Record::<String, String> {
                        key: "b".into(),
                        op: Op::Put,
                        value: Some("2".into()),
                    },
                ],
            };
            let bytes = serde_json::to_vec(&seg_payload).unwrap();
            let _ = seg.put_next(1, &bytes, "application/json").await.unwrap();

            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 1);

            let loaded = head.load().await.unwrap().unwrap();
            assert_eq!(loaded.0.last_segment_seq, Some(1));
            assert_eq!(loaded.0.last_txn_id, 1);
            assert!(!head.fail_first.load(Ordering::SeqCst));
        })
    }
}

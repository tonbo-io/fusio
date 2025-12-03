use std::{
    collections::HashSet,
    hash::Hash,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};

use fusio::executor::{Executor, Timer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    backoff::{classify_error, RetryClass},
    checkpoint::CheckpointStore,
    compactor::Compactor,
    context::ManifestContext,
    head::{HeadJson, HeadStore, PutCondition},
    lease::LeaseStore,
    retention::{DefaultRetention, RetentionPolicy},
    segment::SegmentIo,
    session::{ReadSession, WriteSession},
    snapshot::{ScanRange, Snapshot},
    store::Store,
    types::{Error, Result, TxnId},
    DefaultExecutor,
};

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
#[derive(Clone)]
pub struct Manifest<K, V, HS, SS, CS, LS, E = DefaultExecutor, R = DefaultRetention>
where
    HS: HeadStore + 'static,
    SS: SegmentIo + 'static,
    CS: CheckpointStore + 'static,
    LS: LeaseStore + 'static,
    E: Executor + Timer + Clone + 'static,
    R: RetentionPolicy + Clone,
{
    _phantom: PhantomData<(K, V)>,
    store: Arc<Store<HS, SS, CS, LS, E, R>>,
}

#[cfg(any(feature = "tokio", all(feature = "wasm", target_arch = "wasm32")))]
impl<K, V, HS, SS, CS, LS> Manifest<K, V, HS, SS, CS, LS>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + 'static,
    SS: SegmentIo + 'static,
    CS: CheckpointStore + 'static,
    LS: LeaseStore + 'static,
{
    pub fn new(head: HS, seg: SS, ckpt: CS, leases: LS) -> Self {
        Self::new_with_context(
            head,
            seg,
            ckpt,
            leases,
            Arc::new(ManifestContext::<DefaultRetention, DefaultExecutor>::default()),
        )
    }
}

impl<K, V, HS, SS, CS, LS, E, R> Manifest<K, V, HS, SS, CS, LS, E, R>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + 'static,
    SS: SegmentIo + 'static,
    CS: CheckpointStore + 'static,
    LS: LeaseStore + 'static,
    E: Executor + Timer + Clone + 'static,
    R: RetentionPolicy + Clone,
{
    pub fn new_with_context(
        head: HS,
        seg: SS,
        ckpt: CS,
        leases: LS,
        opts: Arc<ManifestContext<R, E>>,
    ) -> Self {
        Self::from_store(Arc::new(Store::new(head, seg, ckpt, leases, opts)))
    }

    pub(crate) fn from_store(store: Arc<Store<HS, SS, CS, LS, E, R>>) -> Self {
        Self {
            _phantom: PhantomData,
            store,
        }
    }

    /// Build a `Compactor` over the same stores and context. Callers can use this to run
    /// compaction/GC cycles without `Manifest` needing to expose those behaviors directly.
    pub fn compactor(&self) -> Compactor<K, V, HS, SS, CS, LS, E, R> {
        Compactor::from_store(self.store.clone())
    }

    /// Attempt to adopt durable-but-unpublished segments by scanning for a
    /// contiguous run immediately after the current HEAD and CAS-advancing HEAD.
    ///
    /// Returns the number of segments adopted. If a concurrent actor advances
    /// HEAD first, this becomes a no-op (returns 0).
    ///
    /// TODO:
    /// - Surface metrics for CAS backoff/retries.
    /// - Consider verifying segment continuity more strictly (e.g., ensure txn_ids are strictly
    ///   increasing without gaps and optionally validate a checksum header).
    /// - Yield between batches if recovery spans many windows to avoid starving other tasks.
    #[tracing::instrument(skip(self))]
    pub async fn recover_orphans(&self) -> Result<usize> {
        let timer = self.store.opts.timer().clone();
        let pol = self.store.opts.backoff;
        let mut backoff_iter = pol.build_backoff();

        loop {
            let now_ms = timer
                .now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            let now = Duration::from_millis(now_ms);
            let active_txns: HashSet<u64> = match self.store.leases.list_active(now).await {
                Ok(leases) => leases.into_iter().map(|l| l.snapshot_txn_id).collect(),
                Err(e) => match classify_error(&e) {
                    RetryClass::RetryTransient => {
                        if let Some(delay) = backoff_iter.next() {
                            timer.sleep(delay).await;
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                    _ => return Err(e),
                },
            };

            // Load current head once and derive CAS condition for this attempt.
            let loaded = self.store.head.load().await?;
            let (cur, cond) = match loaded.clone() {
                None => (
                    HeadJson {
                        version: 1,
                        checkpoint_id: None,
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
            tracing::debug!(from_seq = ?expected, "recovering orphans");

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
                    let txn_id = self.store.segment.load_meta(&id).await?.txn_id;
                    if txn_id <= last_txn {
                        break 'probe;
                    }
                    if txn_id != last_txn.saturating_add(1) {
                        break 'probe;
                    }
                    if active_txns.contains(&txn_id) {
                        break 'probe;
                    }
                    last_txn = txn_id;
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
                checkpoint_id: cur.checkpoint_id.clone(),
                last_segment_seq: Some(max_seq),
                last_txn_id: last_txn,
            };

            match self.store.head.put(&new_head, cond.clone()).await {
                Ok(_t) => {
                    let adopted_seqs: Vec<u64> =
                        ((max_seq - adopted as u64 + 1)..=max_seq).collect();
                    tracing::warn!(
                        count = adopted,
                        segment_seqs = ?adopted_seqs,
                        "orphan segments recovered"
                    );
                    return Ok(adopted);
                }
                Err(Error::PreconditionFailed) => {
                    if let Some(delay) = backoff_iter.next() {
                        timer.sleep(delay).await;
                        continue;
                    } else {
                        return Err(Error::PreconditionFailed);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Convenience constructors and alias for an in-memory Manifest using Mem backends.
// In-memory constructors live in `testing` helpers (test-only).
impl<K, V, HS, SS, CS, LS, E, R> Manifest<K, V, HS, SS, CS, LS, E, R>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + 'static,
    SS: SegmentIo + 'static,
    CS: CheckpointStore + 'static,
    LS: LeaseStore + 'static,
    E: Executor + Timer + Clone + 'static,
    R: RetentionPolicy + Clone,
{
    /// Open a read session, always acquiring a lease so GC honors the snapshot.
    #[tracing::instrument(skip(self), fields(session_type = "read"))]
    pub async fn session_read(&self) -> Result<ReadSession<K, V, HS, SS, CS, LS, E, R>> {
        tracing::debug!("session_read started");
        let snap = self.snapshot().await?;
        self.session_at(snap).await
    }

    /// Open a write session (pinned, with lease)
    #[tracing::instrument(skip(self), fields(session_type = "write"))]
    pub async fn session_write(&self) -> Result<WriteSession<K, V, HS, SS, CS, LS, E, R>> {
        tracing::debug!("session_write started");
        // Opportunistically adopt durable-but-unpublished segments before opening a writer.
        self.recover_orphans().await?;
        let snap = self.snapshot().await?;
        let ttl = self.store.opts.retention.lease_ttl();
        let next_txn = snap.txn_id.0.saturating_add(1);
        let lease = self
            .store
            .leases
            .create(next_txn, snap.head_tag.clone(), ttl)
            .await?;
        Ok(WriteSession::new(
            self.store.clone(),
            Some(lease),
            snap,
            true,
            ttl,
        ))
    }

    /// Create a pinned read session from a previously persisted snapshot, acquiring a lease so GC
    /// honors it.
    pub async fn session_at(
        &self,
        snapshot: Snapshot,
    ) -> Result<ReadSession<K, V, HS, SS, CS, LS, E, R>> {
        let ttl = self.store.opts.retention.lease_ttl();
        let lease = self
            .store
            .leases
            .create(snapshot.txn_id.0, snapshot.head_tag.clone(), ttl)
            .await?;
        Ok(ReadSession::new(
            self.store.clone(),
            Some(lease),
            snapshot,
            true,
            ttl,
        ))
    }

    /// One-shot latest get (no pin)
    pub async fn get_latest(&self, key: &K) -> Result<Option<V>> {
        let snap = self.snapshot().await?;
        let sess = self.session_at(snap).await?;
        let value = sess.get(key).await?;
        sess.end().await?;
        Ok(value)
    }

    /// One-shot latest scan (no pin)
    pub async fn scan_latest(&self, range: Option<ScanRange<K>>) -> Result<Vec<(K, V)>> {
        let snap = self.snapshot().await?;
        let sess = self.session_at(snap).await?;
        let result = match range {
            None => sess.scan().await,
            Some(r) => sess.scan_range(r).await,
        }?;
        sess.end().await?;
        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    pub async fn snapshot(&self) -> Result<Snapshot> {
        tracing::debug!("loading snapshot");
        match self.store.head.load().await? {
            None => Ok(Snapshot {
                head_tag: None,
                txn_id: TxnId(0),
                last_segment_seq: None,
                checkpoint_seq: None,
                checkpoint_id: None,
            }),
            Some((h, tag)) => {
                let (checkpoint_id, checkpoint_seq) = if let Some(id) = h.checkpoint_id.as_ref() {
                    let meta = self.store.checkpoint.get_checkpoint_meta(id).await?;
                    (Some(id.clone()), Some(meta.last_segment_seq_at_ckpt))
                } else {
                    (None, None)
                };
                let snap = Snapshot {
                    head_tag: Some(tag),
                    txn_id: TxnId(h.last_txn_id),
                    last_segment_seq: h.last_segment_seq,
                    checkpoint_seq,
                    checkpoint_id,
                };
                tracing::info!(
                    txn_id = %snap.txn_id.0,
                    last_segment_seq = ?snap.last_segment_seq,
                    "snapshot loaded"
                );
                Ok(snap)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::{Duration, SystemTime},
    };

    use fusio::{executor::NoopExecutor, impls::mem::fs::InMemoryFs};
    use fusio_core::MaybeSendFuture;
    use futures_executor::block_on;
    use rstest::rstest;

    use super::*;
    use crate::{
        checkpoint::CheckpointStoreImpl,
        context::ManifestContext,
        head::{HeadStoreImpl, HeadTag},
        lease::LeaseStoreImpl,
        retention::DefaultRetention,
        segment::SegmentStoreImpl,
        test_utils::{in_memory_stores, InMemoryStores},
    };

    fn test_manifest(
        stores: &InMemoryStores,
    ) -> Manifest<
        String,
        String,
        HeadStoreImpl<InMemoryFs>,
        SegmentStoreImpl<InMemoryFs>,
        CheckpointStoreImpl<InMemoryFs>,
        LeaseStoreImpl<InMemoryFs, fusio::executor::BlockingExecutor>,
        NoopExecutor,
    > {
        let opts: ManifestContext<DefaultRetention, NoopExecutor> =
            ManifestContext::new(NoopExecutor::default());
        Manifest::new_with_context(
            stores.head.clone(),
            stores.segment.clone(),
            stores.checkpoint.clone(),
            stores.lease.clone(),
            Arc::new(opts),
        )
    }

    fn now_duration() -> Duration {
        Duration::from_millis(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        )
    }

    #[derive(Clone)]
    struct FlakyHeadStore {
        inner: HeadStoreImpl<InMemoryFs>,
        fail_first: Arc<AtomicBool>,
    }

    impl FlakyHeadStore {
        fn new() -> Self {
            let head = crate::test_utils::in_memory_stores().head;
            Self {
                inner: head,
                fail_first: Arc::new(AtomicBool::new(true)),
            }
        }
    }

    impl HeadStore for FlakyHeadStore {
        fn load(
            &self,
        ) -> impl MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, Error>> + '_
        {
            self.inner.load()
        }

        fn put(
            &self,
            head: &HeadJson,
            cond: PutCondition,
        ) -> impl MaybeSendFuture<Output = Result<HeadTag, Error>> + '_ {
            let fail = self.fail_first.swap(false, Ordering::SeqCst);
            let inner = self.inner.clone();
            let head_cloned = head.clone();
            let cond_cloned = cond.clone();
            async move {
                if fail {
                    Err(Error::PreconditionFailed)
                } else {
                    inner.put(&head_cloned, cond_cloned).await
                }
            }
        }
    }

    #[rstest]
    fn mem_kv_end_to_end_and_conflict(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let kv = test_manifest(&in_memory_stores);

            // tx1
            let mut s1 = kv.session_write().await.unwrap();
            s1.put("a".into(), "1".into());
            s1.put("b".into(), "2".into());
            let _c1 = s1.commit().await.unwrap();

            // Read latest via explicit snapshot
            let snap = kv.snapshot().await.unwrap();
            let sess = kv.session_at(snap.clone()).await.unwrap();
            let got = sess.get(&"a".into()).await.unwrap();
            assert_eq!(got.as_deref(), Some("1"));
            sess.end().await.unwrap();

            let sess_again = kv.session_at(snap).await.unwrap();
            let all = sess_again.scan().await.unwrap();
            assert_eq!(all.len(), 2);
            sess_again.end().await.unwrap();

            // tx2 and tx3 from same snapshot; tx2 wins, tx3 loses
            let _snap = kv.snapshot().await.unwrap();
            let mut s2 = kv.session_write().await.unwrap();
            let mut s3 = test_manifest(&in_memory_stores)
                .session_write()
                .await
                .unwrap();
            s2.put("a".into(), "10".into());
            s3.delete("a".into());
            let _ = s2.commit().await.unwrap();
            let r3 = s3.commit().await;
            assert!(matches!(r3, Err(Error::PreconditionFailed)));

            let snap2 = kv.snapshot().await.unwrap();
            let sess2 = kv.session_at(snap2).await.unwrap();
            let got2 = sess2.get(&"a".into()).await.unwrap();
            assert_eq!(got2.as_deref(), Some("10"));
            sess2.end().await.unwrap();
        })
    }

    #[rstest]
    fn mem_kv_point_get_and_tombstone(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let kv = test_manifest(&in_memory_stores);

            // Seed data
            let mut s = kv.session_write().await.unwrap();
            s.put("a".into(), "1".into());
            s.put("b".into(), "2".into());
            let _ = s.commit().await.unwrap();

            // Point gets
            let snap = kv.snapshot().await.unwrap();
            let sess = kv.session_at(snap.clone()).await.unwrap();
            let ga = sess.get(&"a".into()).await.unwrap();
            sess.end().await.unwrap();

            let sess_b = kv.session_at(snap).await.unwrap();
            let gb = sess_b.get(&"b".into()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));
            assert_eq!(gb.as_deref(), Some("2"));
            sess_b.end().await.unwrap();

            // Update and delete on new snapshot
            let mut s2 = kv.session_write().await.unwrap();
            s2.put("a".into(), "10".into());
            s2.delete("b".into());
            let _ = s2.commit().await.unwrap();

            let snap3 = kv.snapshot().await.unwrap();
            let sess3 = kv.session_at(snap3.clone()).await.unwrap();
            let ga2 = sess3.get(&"a".into()).await.unwrap();
            sess3.end().await.unwrap();

            let sess4 = kv.session_at(snap3).await.unwrap();
            let gb2 = sess4.get(&"b".into()).await.unwrap();
            assert_eq!(ga2.as_deref(), Some("10"));
            assert_eq!(gb2, None);
            sess4.end().await.unwrap();
        })
    }

    #[rstest]
    fn mem_session_at_acquires_and_releases_lease(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let ls = in_memory_stores.lease.clone();
            let kv = test_manifest(&in_memory_stores);

            let mut s = kv.session_write().await.unwrap();
            s.put("a".into(), "1".into());
            let _ = s.commit().await.unwrap();

            let snap = kv.snapshot().await.unwrap();
            assert!(ls.list_active(now_duration()).await.unwrap().is_empty());

            let pinned = kv.session_at(snap.clone()).await.unwrap();
            let active = ls.list_active(now_duration()).await.unwrap();
            assert_eq!(active.len(), 1);
            let ga = pinned.get(&"a".into()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));

            pinned.end().await.unwrap();
            let active_after = ls.list_active(now_duration()).await.unwrap();
            assert!(active_after.is_empty());
        })
    }

    #[rstest]
    fn mem_kv_compact_and_read_from_checkpoint(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let kv = test_manifest(&in_memory_stores);

            // Two transactions → two segments
            let mut s1 = kv.session_write().await.unwrap();
            s1.put("a".into(), "1".into());
            s1.put("b".into(), "2".into());
            let _ = s1.commit().await.unwrap();

            let mut s2 = kv.session_write().await.unwrap();
            s2.put("b".into(), "20".into());
            s2.put("c".into(), "3".into());
            let _ = s2.commit().await.unwrap();

            // Compact
            let compactor = kv.compactor();
            let (_ckpt_id, _tag) = compactor.compact_once().await.unwrap();

            // Snapshot now should include checkpoint_seq
            let snap = kv.snapshot().await.unwrap();
            assert!(snap.checkpoint_seq.is_some());

            // Reads still reflect latest values
            let sess = kv.session_at(snap).await.unwrap();
            let ga = sess.get(&"a".into()).await.unwrap();
            let gb = sess.get(&"b".into()).await.unwrap();
            let gc = sess.get(&"c".into()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));
            assert_eq!(gb.as_deref(), Some("20"));
            assert_eq!(gc.as_deref(), Some("3"));
            sess.end().await.unwrap();
        })
    }

    #[rstest]
    fn mem_recover_orphans_no_head_adopts_seq1(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let head = in_memory_stores.head.clone();
            let seg = in_memory_stores.segment.clone();
            let kv = test_manifest(&in_memory_stores);

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
            let _ = seg
                .put_next(1, 1, &bytes, "application/json")
                .await
                .unwrap();

            // Recover
            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 1);
            // Head advanced
            let loaded = head.load().await.unwrap().unwrap();
            assert_eq!(loaded.0.last_segment_seq, Some(1));
            assert_eq!(loaded.0.last_txn_id, 1);
        })
    }

    #[rstest]
    fn mem_recover_orphans_respects_active_leases(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let head = in_memory_stores.head.clone();
            let seg = in_memory_stores.segment.clone();
            let ls = in_memory_stores.lease.clone();
            let kv = test_manifest(&in_memory_stores);

            let seg_payload: Segment<String, String> = Segment {
                txn_id: 1,
                records: vec![Record::<String, String> {
                    key: "a".into(),
                    op: Op::Put,
                    value: Some("1".into()),
                }],
            };
            let bytes = serde_json::to_vec(&seg_payload).unwrap();
            let _ = seg
                .put_next(1, 1, &bytes, "application/json")
                .await
                .unwrap();

            // Writer lease still alive for txn_id=1 → adoption should defer.
            let lease = ls.create(1, None, Duration::from_secs(60)).await.unwrap();

            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 0);
            assert!(head.load().await.unwrap().is_none());

            ls.release(lease).await.unwrap();

            let adopted_after = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted_after, 1);
            let loaded = head.load().await.unwrap().unwrap();
            assert_eq!(loaded.0.last_segment_seq, Some(1));
            assert_eq!(loaded.0.last_txn_id, 1);
        })
    }

    #[rstest]
    fn mem_recover_orphans_gap_does_not_adopt(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let head = in_memory_stores.head.clone();
            let seg = in_memory_stores.segment.clone();
            let kv = test_manifest(&in_memory_stores);

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
            let _ = seg
                .put_next(2, 1, &bytes, "application/json")
                .await
                .unwrap();

            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 0);
            assert!(head.load().await.unwrap().is_none());
        })
    }

    #[rstest]
    fn mem_recover_orphans_advances_over_existing_head(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let head = in_memory_stores.head.clone();
            let seg = in_memory_stores.segment.clone();
            let kv = test_manifest(&in_memory_stores);

            // Create initial segment and head via normal commit
            {
                let mut s = kv.session_write().await.unwrap();
                s.put("a".into(), "1".into());
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
            let _ = seg
                .put_next(2, 2, &bytes, "application/json")
                .await
                .unwrap();

            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 1);
            let loaded = head.load().await.unwrap().unwrap();
            assert_eq!(loaded.0.last_segment_seq, Some(2));
            assert_eq!(loaded.0.last_txn_id, 2);
        })
    }

    #[rstest]
    fn mem_recover_orphans_retries_on_conflicting_head(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let head = FlakyHeadStore::new();
            let seg = in_memory_stores.segment;
            let ck = in_memory_stores.checkpoint;
            let ls = in_memory_stores.lease;

            let opts: ManifestContext<DefaultRetention, fusio::executor::NoopExecutor> =
                ManifestContext::new(fusio::executor::NoopExecutor::default());
            let mut backoff = crate::backoff::BackoffPolicy::default();
            backoff.base_ms = 1;
            backoff.max_ms = 1;
            backoff.multiplier_times_100 = 100;
            backoff.jitter_frac_times_100 = 0;
            backoff.max_retries = 4;
            backoff.max_elapsed_ms = 10;
            let opts = opts.with_backoff(backoff);

            let kv: Manifest<String, String, _, _, _, _, fusio::executor::NoopExecutor> =
                Manifest::new_with_context(
                    head.clone(),
                    seg.clone(),
                    ck.clone(),
                    ls.clone(),
                    Arc::new(opts),
                );

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
            let _ = seg
                .put_next(1, 1, &bytes, "application/json")
                .await
                .unwrap();

            let adopted = kv.recover_orphans().await.unwrap();
            assert_eq!(adopted, 1);

            let loaded = head.load().await.unwrap().unwrap();
            assert_eq!(loaded.0.last_segment_seq, Some(1));
            assert_eq!(loaded.0.last_txn_id, 1);
            assert!(!head.fail_first.load(Ordering::SeqCst));
        })
    }
}

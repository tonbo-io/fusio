//! Independent compactor for headless compaction + GC.
//!
//! This module provides a small façade that can run compaction and GC without
//! the caller owning a long‑lived `Manifest` instance. Under the hood it
//! constructs a temporary `Manifest` over the provided stores and invokes the
//! existing logic. This keeps `Manifest` as the primary user API while enabling
//! remote/scheduled compaction jobs.

use core::{hash::Hash, marker::PhantomData, time::Duration};
use std::{collections::HashMap, sync::Arc, time::SystemTime};

use fusio::executor::{Executor, Timer};
use futures_util::TryStreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    backoff::{classify_error, ExponentialBackoff, RetryClass},
    checkpoint::{CheckpointId, CheckpointMeta, CheckpointStore},
    context::ManifestContext,
    gc::{GcPlan, GcPlanStore, GcTag, SegmentRange},
    head::{HeadJson, HeadStore, HeadTag, PutCondition},
    lease::LeaseStore,
    manifest::{Manifest, Op, Segment},
    retention::{DefaultRetention, RetentionPolicy},
    segment::SegmentIo,
    snapshot::Snapshot,
    store::Store,
    types::{Error, Result, TxnId},
    BlockingExecutor,
};

/// Headless compactor that orchestrates compaction + GC using the same logic
/// as `Manifest`, without requiring a long‑lived manifest instance owned by
/// the caller.
pub struct Compactor<K, V, HS, SS, CS, LS, E = BlockingExecutor, R = DefaultRetention>
where
    HS: HeadStore + Send + Sync + 'static,
    SS: SegmentIo + Send + Sync + 'static,
    CS: CheckpointStore + Send + Sync + 'static,
    LS: LeaseStore + Send + Sync + 'static,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    _phantom: PhantomData<(K, V)>,
    store: Arc<Store<HS, SS, CS, LS, E, R>>,
}

#[derive(Default)]
struct CheckpointRetentionStats {
    entries: Vec<(CheckpointId, CheckpointMeta)>,
    newest: Option<(CheckpointId, CheckpointMeta)>,
    floor: Option<(CheckpointId, CheckpointMeta)>,
    keep_last_ids: std::collections::BTreeSet<CheckpointId>,
}

impl<K, V, HS, SS, CS, LS, E, R> Compactor<K, V, HS, SS, CS, LS, E, R>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore + Send + Sync + 'static,
    SS: SegmentIo + Send + Sync + 'static,
    CS: CheckpointStore + Send + Sync + 'static,
    LS: LeaseStore + Send + Sync + 'static,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    pub fn new(head: HS, seg: SS, ckpt: CS, leases: LS, opts: Arc<ManifestContext<R, E>>) -> Self {
        Self {
            _phantom: PhantomData,
            store: Arc::new(Store::new(head, seg, ckpt, leases, opts)),
        }
    }

    pub(crate) fn from_store(store: Arc<Store<HS, SS, CS, LS, E, R>>) -> Self {
        Self {
            _phantom: PhantomData,
            store,
        }
    }

    fn manifest(&self) -> Manifest<K, V, HS, SS, CS, LS, E, R> {
        Manifest::from_store(self.store.clone())
    }

    async fn snapshot(&self) -> Result<Snapshot> {
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
                    let (meta, _payload) = self.store.checkpoint.get_checkpoint(id).await?;
                    (Some(id.clone()), Some(meta.last_segment_seq_at_ckpt))
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

    async fn fold_until(&self, map: &mut HashMap<K, V>, snap: &Snapshot) -> Result<()> {
        if let Some(id) = snap.checkpoint_id.as_ref() {
            #[derive(Deserialize)]
            struct CkptPayload<K, V> {
                entries: Vec<(K, V)>,
            }

            let (_meta, bytes) = self.store.checkpoint.get_checkpoint(id).await?;
            let payload: CkptPayload<K, V> = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Corrupt(format!("ckpt decode: {e}")))?;
            for (k, v) in payload.entries {
                map.insert(k, v);
            }
        }

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
                if id.seq < cursor {
                    continue;
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
                cursor = id.seq.saturating_add(1);
            }
        }
        Ok(())
    }

    /// Fold segments into a checkpoint and CAS-publish it into HEAD. Returns the checkpoint id
    /// and HEAD tag.
    pub async fn compact_once(&self) -> Result<(CheckpointId, HeadTag)> {
        let snap = self.snapshot().await?;
        let mut map: HashMap<K, V> = HashMap::new();
        self.fold_until(&mut map, &snap).await?;

        #[derive(Serialize)]
        struct CkptPayload<K, V> {
            entries: Vec<(K, V)>,
        }

        let entries: Vec<(K, V)> = map.into_iter().collect();
        let entries_len = entries.len();
        let payload = serde_json::to_vec(&CkptPayload { entries })
            .map_err(|e| Error::Corrupt(format!("ckpt encode: {e}")))?;

        let meta = CheckpointMeta {
            lsn: snap.txn_id.0,
            key_count: entries_len,
            byte_size: payload.len(),
            created_at_ms: system_time_to_ms(self.store.opts.timer().now()),
            format: "application/json".into(),
            last_segment_seq_at_ckpt: snap.last_segment_seq.unwrap_or(0),
        };

        let id = self
            .store
            .checkpoint
            .put_checkpoint(&meta, &payload, "application/json")
            .await?;

        match self.store.head.load().await? {
            None => {
                let new_head = HeadJson {
                    version: 1,
                    checkpoint_id: Some(id.clone()),
                    last_segment_seq: None,
                    last_txn_id: 0,
                };
                let pol = self.store.opts.backoff;
                let timer = self.store.opts.timer().clone();
                let mut bo = ExponentialBackoff::new(pol, timer.clone());
                let tag = loop {
                    match self
                        .store
                        .head
                        .put(&new_head, PutCondition::IfNotExists)
                        .await
                    {
                        Ok(t) => break t,
                        Err(Error::PreconditionFailed) => return Err(Error::PreconditionFailed),
                        Err(e) => match crate::backoff::classify_error(&e) {
                            RetryClass::RetryTransient if !bo.exhausted() => {
                                let delay = bo.next_delay();
                                timer.sleep(delay).await;
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
                    checkpoint_id: Some(id.clone()),
                    last_segment_seq: cur.last_segment_seq,
                    last_txn_id: cur.last_txn_id,
                };
                let pol = self.store.opts.backoff;
                let timer = self.store.opts.timer().clone();
                let mut bo = ExponentialBackoff::new(pol, timer.clone());
                let tag = loop {
                    match self
                        .store
                        .head
                        .put(&new_head, PutCondition::IfMatch(cur_tag.clone()))
                        .await
                    {
                        Ok(t) => break t,
                        Err(Error::PreconditionFailed) => return Err(Error::PreconditionFailed),
                        Err(e) => match crate::backoff::classify_error(&e) {
                            RetryClass::RetryTransient if !bo.exhausted() => {
                                let delay = bo.next_delay();
                                timer.sleep(delay).await;
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
        match self.store.head.load().await? {
            Some((_h, cur_tag)) if cur_tag != tag => return Ok((ckpt_id, tag)),
            _ => {}
        }

        let now = Duration::from_millis(system_time_to_ms(self.store.opts.timer().now()));
        let leases = self.store.leases.list_active(now).await?;
        let watermark = leases
            .iter()
            .map(|l| l.snapshot_txn_id)
            .min()
            .unwrap_or(u64::MAX);

        let meta = self.store.checkpoint.get_checkpoint_meta(&ckpt_id).await?;
        if watermark > meta.lsn {
            let _ = self
                .store
                .segment
                .delete_upto(meta.last_segment_seq_at_ckpt)
                .await;
        }

        let ttl_ms: u64 = self
            .store
            .opts
            .retention
            .checkpoints_min_ttl()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let now_ms2 = system_time_to_ms(self.store.opts.timer().now());
        let mut list = match self.store.checkpoint.list().await {
            Ok(stream) => stream.try_collect::<Vec<_>>().await.unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        list.sort_by_key(|(_id, m)| m.lsn);
        let newest = list.iter().max_by_key(|(_id, m)| m.lsn).cloned();
        let floor = list
            .iter()
            .filter(|(_id, m)| m.lsn <= watermark)
            .max_by_key(|(_id, m)| m.lsn)
            .cloned();
        let keep_last = self.store.opts.retention.checkpoints_keep_last();
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
                let _ = self.store.checkpoint.delete(&id).await;
            }
        }

        Ok((ckpt_id, tag))
    }

    /// Execute one compaction + GC cycle using the provided stores.
    pub async fn run_once(&self) -> Result<()> {
        let m = self.manifest();
        // Adopt any durable-but-unpublished segments before planning compaction/GC.
        m.recover_orphans().await?;
        let _ = self.compact_and_gc().await?;
        Ok(())
    }

    /// Compute GC plan from HEAD + active leases and install via CAS (IfNotExists).
    pub async fn gc_compute<S: GcPlanStore + Clone + 'static>(
        &self,
        store: &S,
    ) -> Result<Option<GcTag>> {
        let pol = self.store.opts.backoff;
        let timer = self.store.opts.timer().clone();
        let mut bo = ExponentialBackoff::new(pol, timer.clone());
        loop {
            // Read current HEAD snapshot/tag
            let head = match self.store.head.load().await {
                Ok(h) => h,
                Err(e) => match classify_error(&e) {
                    RetryClass::RetryTransient if !bo.exhausted() => {
                        let delay = bo.next_delay();
                        timer.sleep(delay).await;
                        continue;
                    }
                    _ => return Err(e),
                },
            };
            let (head_json, head_tag) = match head {
                None => return Ok(None),
                Some(v) => v,
            };
            // Determine watermark from active leases
            let now_ms = system_time_to_ms(self.store.opts.timer().now());
            let now = Duration::from_millis(now_ms);
            let leases = match self.store.leases.list_active(now).await {
                Ok(ls) => ls,
                Err(e) => match classify_error(&e) {
                    RetryClass::RetryTransient if !bo.exhausted() => {
                        let delay = bo.next_delay();
                        timer.sleep(delay).await;
                        continue;
                    }
                    _ => return Err(e),
                },
            };
            if leases.is_empty() {
                // No active readers/writers → nothing to compute
                return Ok(None);
            }
            let watermark = leases
                .iter()
                .map(|l| l.snapshot_txn_id)
                .min()
                .unwrap_or(u64::MAX);

            // Build checkpoint delete set based on retention and watermark
            let ttl_ms = self
                .store
                .opts
                .retention
                .checkpoints_min_ttl()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            let keep_last = self.store.opts.retention.checkpoints_keep_last();

            let retention = self
                .collect_checkpoint_retention_state(watermark, keep_last)
                .await?;
            let delete_checkpoints = Self::checkpoints_to_delete(&retention, now_ms, ttl_ms);
            let delete_segments = Self::segments_to_delete(&retention, &head_json);

            let plan = GcPlan {
                against_head_tag: Some(head_tag.0.clone()),
                not_before: now
                    .checked_add(self.store.opts.retention.segments_min_ttl())
                    .unwrap_or(Duration::from_millis(u64::MAX)),
                delete_segments,
                delete_checkpoints,
                make_checkpoints: Vec::new(),
            };

            // Install plan only if there isn't one already
            match store
                .put(&plan, crate::head::PutCondition::IfNotExists)
                .await
            {
                Ok(tag) => return Ok(Some(tag)),
                Err(crate::types::Error::PreconditionFailed) => return Ok(None),
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
    }

    async fn collect_checkpoint_retention_state(
        &self,
        watermark: u64,
        keep_last: usize,
    ) -> Result<CheckpointRetentionStats> {
        use core::cmp::Reverse;
        use std::collections::BinaryHeap;

        let mut state = CheckpointRetentionStats::default();
        let mut keep_last_heap = BinaryHeap::<Reverse<(u64, CheckpointId)>>::new();

        if let Ok(stream) = self.store.checkpoint.list().await {
            futures_util::pin_mut!(stream);
            loop {
                match stream.as_mut().try_next().await {
                    Ok(Some((id, meta))) => {
                        let lsn = meta.lsn;
                        if state
                            .newest
                            .as_ref()
                            .map(|(_, m)| m.lsn < lsn)
                            .unwrap_or(true)
                        {
                            state.newest = Some((id.clone(), meta.clone()));
                        }
                        if lsn <= watermark
                            && state
                                .floor
                                .as_ref()
                                .map(|(_, m)| m.lsn < lsn)
                                .unwrap_or(true)
                        {
                            state.floor = Some((id.clone(), meta.clone()));
                        }
                        if keep_last > 0 {
                            keep_last_heap.push(Reverse((lsn, id.clone())));
                            if keep_last_heap.len() > keep_last {
                                keep_last_heap.pop();
                            }
                        }
                        state.entries.push((id, meta));
                    }
                    Ok(None) => break,
                    Err(_) => return Ok(CheckpointRetentionStats::default()),
                }
            }
        }

        state.keep_last_ids = keep_last_heap
            .into_iter()
            .map(|Reverse((_lsn, id))| id)
            .collect();

        Ok(state)
    }

    fn checkpoints_to_delete(
        state: &CheckpointRetentionStats,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Vec<String> {
        let newest_id = state.newest.as_ref().map(|(id, _)| id);
        let floor_id = state.floor.as_ref().map(|(id, _)| id);

        state
            .entries
            .iter()
            .filter_map(|(id, meta)| {
                let keep_newest = newest_id.map(|candidate| candidate == id).unwrap_or(false);
                let keep_floor = floor_id.map(|candidate| candidate == id).unwrap_or(false);
                let keep_in_last = state.keep_last_ids.contains(id);
                let age_ok = now_ms.saturating_sub(meta.created_at_ms) >= ttl_ms;
                if !keep_newest && !keep_floor && !keep_in_last && age_ok {
                    Some(id.0.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn segments_to_delete(
        state: &CheckpointRetentionStats,
        head_json: &HeadJson,
    ) -> Vec<SegmentRange> {
        if let Some((_floor_id, floor_meta)) = &state.floor {
            if let Some(last_seq) = head_json.last_segment_seq {
                let upto = core::cmp::min(floor_meta.last_segment_seq_at_ckpt, last_seq);
                if upto > 0 {
                    return vec![SegmentRange::new(1, upto)];
                }
            }
        }
        Vec::new()
    }

    /// Apply the plan to HEAD.
    ///
    /// Ensures HEAD references a checkpoint whose `last_segment_seq_at_ckpt` is
    /// \>= the highest segment slated for deletion. If HEAD has changed since the
    /// plan was computed, CAS-resets the plan to empty to signal invalidation.
    pub async fn gc_apply<S: GcPlanStore + Clone + 'static>(&self, store: &S) -> Result<()> {
        let pol = self.store.opts.backoff;
        let timer = self.store.opts.timer().clone();
        let mut bo = ExponentialBackoff::new(pol, timer.clone());
        'outer: loop {
            // Load plan; nothing to do if absent/empty
            let loaded = match store.load().await {
                Ok(v) => v,
                Err(e) => match classify_error(&e) {
                    RetryClass::RetryTransient if !bo.exhausted() => {
                        let delay = bo.next_delay();
                        timer.sleep(delay).await;
                        continue 'outer;
                    }
                    _ => return Err(e),
                },
            };
            let Some((plan, plan_tag)) = loaded else {
                return Ok(());
            };
            if plan.delete_segments.is_empty() && plan.delete_checkpoints.is_empty() {
                return Ok(());
            }

            // Determine highest segment to delete
            let upto = plan
                .delete_segments
                .iter()
                .map(|r| r.end)
                .max()
                .unwrap_or(0);
            // If there are no segment deletes, no head changes are strictly required.
            if upto == 0 {
                return Ok(());
            }

            // Load HEAD and verify the plan still applies to this HEAD tag
            let head_loaded = match self.store.head.load().await {
                Ok(v) => v,
                Err(e) => match classify_error(&e) {
                    RetryClass::RetryTransient if !bo.exhausted() => {
                        let delay = bo.next_delay();
                        timer.sleep(delay).await;
                        continue 'outer;
                    }
                    _ => return Err(e),
                },
            };
            let Some((cur_head, cur_tag)) = head_loaded else {
                // No head → invalidate plan
                let _ = store
                    .put(
                        &GcPlan::default(),
                        crate::head::PutCondition::IfMatch(crate::head::HeadTag(
                            plan_tag.0.clone(),
                        )),
                    )
                    .await;
                return Ok(());
            };

            if let Some(expected) = plan.against_head_tag.as_ref() {
                if &cur_tag.0 != expected {
                    // Plan invalidated by concurrent head change → reset plan to empty via CAS on
                    // plan tag
                    let _ = store
                        .put(
                            &GcPlan::default(),
                            crate::head::PutCondition::IfMatch(crate::head::HeadTag(
                                plan_tag.0.clone(),
                            )),
                        )
                        .await;
                    return Ok(());
                }
            }

            // Decide target checkpoint: one whose last_segment_seq_at_ckpt >= upto, preferring the
            // smallest such.
            let mut ckpts = match self.store.checkpoint.list().await {
                Ok(stream) => stream.try_collect::<Vec<_>>().await.unwrap_or_default(),
                Err(_) => Vec::new(),
            };
            ckpts.sort_by_key(|(_id, m)| m.last_segment_seq_at_ckpt);
            let candidate = ckpts
                .into_iter()
                .find(|(_id, m)| m.last_segment_seq_at_ckpt >= upto);

            let Some((target_id, _target_meta)) = candidate else {
                // No suitable checkpoint; plan cannot be applied — invalidate to force recompute.
                let _ = store
                    .put(
                        &GcPlan::default(),
                        crate::head::PutCondition::IfMatch(crate::head::HeadTag(
                            plan_tag.0.clone(),
                        )),
                    )
                    .await;
                return Ok(());
            };

            // If HEAD already references a sufficient checkpoint, nothing to do.
            let already_ok = if let Some(cur_ckpt_id) = cur_head.checkpoint_id.as_ref() {
                match self.store.checkpoint.get_checkpoint(cur_ckpt_id).await {
                    Ok((meta, _)) => meta.last_segment_seq_at_ckpt >= upto,
                    Err(_) => false,
                }
            } else {
                false
            };
            if already_ok {
                return Ok(());
            }

            // Publish new HEAD with updated checkpoint id via CAS against current head tag
            let new_head = crate::head::HeadJson {
                version: cur_head.version,
                checkpoint_id: Some(target_id.clone()),
                last_segment_seq: cur_head.last_segment_seq,
                last_txn_id: cur_head.last_txn_id,
            };
            match self
                .store
                .head
                .put(&new_head, crate::head::PutCondition::IfMatch(cur_tag))
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => match classify_error(&e) {
                    RetryClass::DurableConflict => {
                        // Invalidate plan; others advanced HEAD
                        let _ = store
                            .put(
                                &GcPlan::default(),
                                crate::head::PutCondition::IfMatch(crate::head::HeadTag(
                                    plan_tag.0.clone(),
                                )),
                            )
                            .await;
                        return Ok(());
                    }
                    RetryClass::RetryTransient if !bo.exhausted() => {
                        let delay = bo.next_delay();
                        timer.sleep(delay).await;
                        continue 'outer;
                    }
                    _ => return Err(e),
                },
            }
        }
    }

    /// Delete objects and reset plan.
    ///
    /// After `not_before` passes, deletes planned segments and checkpoints
    /// (best-effort, idempotent), then CAS-resets the GC plan to empty.
    pub async fn gc_delete_and_reset<S: GcPlanStore + 'static>(&self, store: &S) -> Result<()> {
        let pol = self.store.opts.backoff;
        let timer = self.store.opts.timer().clone();
        let mut bo = ExponentialBackoff::new(pol, timer.clone());
        // Load plan
        let loaded = match store.load().await {
            Ok(v) => v,
            Err(e) => match classify_error(&e) {
                RetryClass::RetryTransient if !bo.exhausted() => {
                    let delay = bo.next_delay();
                    timer.sleep(delay).await;
                    return Ok(()); // next pass will try again
                }
                _ => return Err(e),
            },
        };
        let Some((plan, plan_tag)) = loaded else {
            return Ok(());
        };
        if plan.delete_segments.is_empty() && plan.delete_checkpoints.is_empty() {
            // Nothing to delete; ensure plan is empty
            return Ok(());
        }

        // Time guard
        let now = Duration::from_millis(system_time_to_ms(self.store.opts.timer().now()));
        if now < plan.not_before {
            // Not yet — leave plan as-is
            return Ok(());
        }

        // Segments: prefer contiguous range optimization
        if let Some(upto) = plan.delete_segments.iter().map(|r| r.end).max() {
            // Best-effort; stores may return Unimplemented
            // TODO: use batch deletes where available and bound per-iteration work.
            let _ = self.store.segment.delete_upto(upto).await;
        }

        // Checkpoints
        for id in plan.delete_checkpoints.iter() {
            let _ = self
                .store
                .checkpoint
                .delete(&CheckpointId(id.clone()))
                .await;
        }

        // Reset plan to empty using CAS on the plan tag we observed
        loop {
            match store
                .put(
                    &GcPlan::default(),
                    crate::head::PutCondition::IfMatch(crate::head::HeadTag(plan_tag.0.clone())),
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(crate::types::Error::PreconditionFailed) => return Ok(()),
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
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;

    use super::*;
    use crate::{context::ManifestContext, manifest::Manifest, testing::new_inmemory_stores};

    #[test]
    fn headless_compactor_invokes_manifest_logic() {
        block_on(async move {
            let opts = Arc::new(ManifestContext::default());
            // Construct a simple Manifest to seed data, then run headless compactor.
            let (head, segment, checkpoint, lease) = new_inmemory_stores();
            let m = Manifest::<String, String, _, _, _, _>::new_with_context(
                head,
                segment,
                checkpoint,
                lease,
                Arc::clone(&opts),
            );
            let mut s = m.session_write().await.unwrap();
            s.put("a".into(), "1".into());
            s.put("b".into(), "2".into());
            let _ = s.commit().await.unwrap();

            let (head, segment, checkpoint, lease) = new_inmemory_stores();
            let comp = Compactor::<String, String, _, _, _, _>::new(
                head,
                segment,
                checkpoint,
                lease,
                Arc::clone(&opts),
            );
            // Running on empty stores does nothing harmful.
            comp.run_once().await.unwrap();
        })
    }
}

#[cfg(test)]
mod gc_compute_tests {
    use fusio::{
        executor::{BlockingExecutor, Timer},
        impls::mem::fs::InMemoryFs,
    };
    use futures_executor::block_on;

    use super::*;
    use crate::{backoff::BackoffPolicy, gc::FsGcPlanStore, testing::new_inmemory_stores};

    #[test]
    fn compute_plan_no_head_or_no_leases_yields_none() {
        block_on(async move {
            let opts = Arc::new(ManifestContext::default());
            let (head, segment, checkpoint, lease) = new_inmemory_stores();
            let comp = Compactor::<String, String, _, _, _, _>::new(
                head, segment, checkpoint, lease, opts,
            );
            let timer: Arc<dyn Timer + Send + Sync> = Arc::new(BlockingExecutor::default());
            let store = FsGcPlanStore::new(InMemoryFs::new(), "", BackoffPolicy::default(), timer);
            // No head yet → None
            let t = comp.gc_compute(&store).await.unwrap();
            assert!(t.is_none());
        })
    }
}

fn system_time_to_ms(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

use std::{collections::HashMap, hash::Hash, marker::PhantomData, sync::Arc, time::Duration};

use backon::{BackoffBuilder, ExponentialBuilder};
use fusio::executor::{Executor, Timer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    backoff::{classify_error, RetryClass},
    checkpoint::CheckpointStore,
    head::{HeadJson, HeadStore, PutCondition},
    lease::{keeper::LeaseKeeper, LeaseHandle, LeaseStore},
    manifest::{Op, Record, Segment},
    retention::{DefaultRetention, RetentionPolicy},
    segment::SegmentIo,
    snapshot::{ScanRange, Snapshot},
    store::Store,
    types::{Error, Result},
    BlockingExecutor,
};

struct SessionInner<K, V, HS, SS, CS, LS, E = BlockingExecutor, R = DefaultRetention>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore,
    SS: SegmentIo,
    CS: CheckpointStore,
    LS: LeaseStore,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    store: Arc<Store<HS, SS, CS, LS, E, R>>,
    lease: Option<LeaseHandle>,
    snapshot: Snapshot,
    pinned: bool,
    ttl: Duration,
    _marker: PhantomData<(K, V)>,
}

impl<K, V, HS, SS, CS, LS, E, R> Drop for SessionInner<K, V, HS, SS, CS, LS, E, R>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore,
    SS: SegmentIo,
    CS: CheckpointStore,
    LS: LeaseStore,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    fn drop(&mut self) {
        debug_assert!(
            self.lease.is_none(),
            "Session dropped without releasing its lease; call end().await or commit().await",
        );
    }
}

impl<K, V, HS, SS, CS, LS, E, R> SessionInner<K, V, HS, SS, CS, LS, E, R>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore,
    SS: SegmentIo,
    CS: CheckpointStore,
    LS: LeaseStore,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    fn new(
        store: Arc<Store<HS, SS, CS, LS, E, R>>,
        lease: Option<LeaseHandle>,
        snapshot: Snapshot,
        pinned: bool,
        ttl: Duration,
    ) -> Self {
        Self {
            store,
            lease,
            snapshot,
            pinned,
            ttl,
            _marker: PhantomData,
        }
    }

    fn store(&self) -> &Arc<Store<HS, SS, CS, LS, E, R>> {
        &self.store
    }

    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn lease_mut(&mut self) -> &mut Option<LeaseHandle> {
        &mut self.lease
    }

    fn timer(&self) -> ArcTimer {
        let timer = self.store.opts.timer().clone();
        Arc::new(timer) as ArcTimer
    }

    async fn release_lease(&mut self) -> Result<()> {
        if let Some(lease) = self.lease.take() {
            self.store.leases.release(lease).await
        } else {
            Ok(())
        }
    }

    async fn release_lease_silent(&mut self) {
        if let Some(lease) = self.lease.take() {
            let _ = self.store.leases.release(lease).await;
        }
    }

    async fn heartbeat(&self) -> Result<()> {
        if self.pinned {
            if let Some(lease) = self.lease.as_ref() {
                return self.store.leases.heartbeat(lease, self.ttl).await;
            }
        }
        Ok(())
    }

    fn start_lease_keeper(&self) -> Result<LeaseKeeper>
    where
        LS: LeaseStore + Clone + 'static,
    {
        if !self.pinned {
            return Err(Error::Unimplemented(
                "lease keeper requires a pinned session",
            ));
        }
        let lease = self
            .lease
            .as_ref()
            .ok_or(Error::Unimplemented("lease keeper requires active lease"))?
            .clone();
        let executor = self.store.opts.executor().clone();
        let timer = self.timer();
        let leases = self.store.leases.clone();
        let ttl = self.ttl;
        LeaseKeeper::spawn(executor, timer, leases, lease, ttl)
    }

    async fn fold_into_map(&self, map: &mut HashMap<K, V>) -> Result<()> {
        #[derive(Deserialize)]
        struct CkptPayload<K, V> {
            entries: Vec<(K, V)>,
        }

        if let Some(id) = self.snapshot.checkpoint_id.as_ref() {
            let (_meta, bytes) = self.store.checkpoint.get_checkpoint(id).await?;
            let payload: CkptPayload<K, V> = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Corrupt(format!("ckpt decode: {e}")))?;
            for (k, v) in payload.entries {
                map.insert(k, v);
            }
        }

        let last_seq = match self.snapshot.last_segment_seq {
            Some(s) => s,
            None => return Ok(()),
        };
        let mut cursor = self
            .snapshot
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
                if seg.txn_id > self.snapshot.txn_id.0 {
                    return Ok(());
                }
                for record in seg.records.into_iter() {
                    match record.op {
                        Op::Put => {
                            if let Some(v) = record.value {
                                map.insert(record.key, v);
                            }
                        }
                        Op::Del => {
                            map.remove(&record.key);
                        }
                    }
                }
                cursor = id.seq.saturating_add(1);
            }
        }

        Ok(())
    }

    async fn base_scan(&self) -> Result<Vec<(K, V)>> {
        let mut map: HashMap<K, V> = HashMap::new();
        self.fold_into_map(&mut map).await?;
        Ok(map.into_iter().collect())
    }

    async fn base_scan_range(&self, range: ScanRange<K>) -> Result<Vec<(K, V)>> {
        let mut map: HashMap<K, V> = HashMap::new();
        self.fold_into_map(&mut map).await?;
        let mut out: Vec<(K, V)> = map.into_iter().collect();
        if let Some(start) = range.start.as_ref() {
            out.retain(|(k, _)| k >= start);
        }
        if let Some(end) = range.end.as_ref() {
            out.retain(|(k, _)| k < end);
        }
        Ok(out)
    }

    async fn base_get(&self, key: &K) -> Result<Option<V>> {
        if let Some(last_seq) = self.snapshot.last_segment_seq {
            let start_seq = self
                .snapshot
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
                if seg.txn_id > self.snapshot.txn_id.0 {
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
            if let Some(ckpt_id) = self.snapshot.checkpoint_id.clone() {
                #[derive(Deserialize)]
                struct CkptPayload<K, V> {
                    entries: Vec<(K, V)>,
                }
                let (_meta, bytes) = self.store.checkpoint.get_checkpoint(&ckpt_id).await?;
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
}

type ArcTimer = Arc<dyn Timer + Send + Sync>;

/// Read-only pinned session.
#[must_use = "Sessions hold a lease; call end().await before dropping"]
pub struct ReadSession<K, V, HS, SS, CS, LS, E = BlockingExecutor, R = DefaultRetention>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore,
    SS: SegmentIo,
    CS: CheckpointStore,
    LS: LeaseStore,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    inner: SessionInner<K, V, HS, SS, CS, LS, E, R>,
}

impl<K, V, HS, SS, CS, LS, E, R> ReadSession<K, V, HS, SS, CS, LS, E, R>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore,
    SS: SegmentIo,
    CS: CheckpointStore,
    LS: LeaseStore,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    pub(crate) fn new(
        store: Arc<Store<HS, SS, CS, LS, E, R>>,
        lease: Option<LeaseHandle>,
        snapshot: Snapshot,
        pinned: bool,
        ttl: Duration,
    ) -> Self {
        Self {
            inner: SessionInner::new(store, lease, snapshot, pinned, ttl),
        }
    }

    pub fn snapshot(&self) -> &Snapshot {
        self.inner.snapshot()
    }

    pub fn start_lease_keeper(&self) -> Result<LeaseKeeper>
    where
        LS: LeaseStore + Clone + 'static,
    {
        self.inner.start_lease_keeper()
    }

    pub async fn heartbeat(&self) -> Result<()> {
        self.inner.heartbeat().await
    }

    pub async fn end(mut self) -> Result<()> {
        self.inner.release_lease().await
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.inner.base_get(key).await
    }

    pub async fn scan(&self) -> Result<Vec<(K, V)>> {
        self.inner.base_scan().await
    }

    pub async fn scan_range(&self, range: ScanRange<K>) -> Result<Vec<(K, V)>> {
        self.inner.base_scan_range(range).await
    }
}

/// Writable session with staged operations.
#[must_use = "Sessions hold a lease; call commit().await or end().await before dropping"]
pub struct WriteSession<K, V, HS, SS, CS, LS, E = BlockingExecutor, R = DefaultRetention>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore,
    SS: SegmentIo,
    CS: CheckpointStore,
    LS: LeaseStore,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    inner: SessionInner<K, V, HS, SS, CS, LS, E, R>,
    staged: Vec<(K, Op, Option<V>)>,
}

impl<K, V, HS, SS, CS, LS, E, R> WriteSession<K, V, HS, SS, CS, LS, E, R>
where
    K: PartialOrd + Eq + Hash + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    HS: HeadStore,
    SS: SegmentIo,
    CS: CheckpointStore,
    LS: LeaseStore,
    E: Executor + Timer + Clone + Send + Sync + 'static,
    R: RetentionPolicy + Clone,
{
    pub(crate) fn new(
        store: Arc<Store<HS, SS, CS, LS, E, R>>,
        lease: Option<LeaseHandle>,
        snapshot: Snapshot,
        pinned: bool,
        ttl: Duration,
    ) -> Self {
        Self {
            inner: SessionInner::new(store, lease, snapshot, pinned, ttl),
            staged: Vec::new(),
        }
    }

    pub fn snapshot(&self) -> &Snapshot {
        self.inner.snapshot()
    }

    pub fn start_lease_keeper(&self) -> Result<LeaseKeeper>
    where
        HS: Clone,
        SS: Clone,
        CS: Clone,
        LS: Clone + 'static,
    {
        self.inner.start_lease_keeper()
    }

    pub async fn heartbeat(&self) -> Result<()> {
        self.inner.heartbeat().await
    }

    pub fn put(&mut self, key: K, value: V) {
        self.staged.push((key, Op::Put, Some(value)));
    }

    pub fn delete(&mut self, key: K) {
        self.staged.push((key, Op::Del, None));
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        self.inner.base_get(key).await
    }

    pub async fn get_local(&self, key: &K) -> Result<Option<V>> {
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
        self.inner.base_get(key).await
    }

    pub async fn scan(&self) -> Result<Vec<(K, V)>> {
        self.inner.base_scan().await
    }

    pub async fn scan_local(&self, range: Option<ScanRange<K>>) -> Result<Vec<(K, V)>> {
        let mut base_entries: Vec<(K, V)> = match range.as_ref() {
            None => self.inner.base_scan().await?,
            Some(r) => {
                self.inner
                    .base_scan_range(ScanRange {
                        start: r.start.as_ref().map(Self::clone_via_json).transpose()?,
                        end: r.end.as_ref().map(Self::clone_via_json).transpose()?,
                    })
                    .await?
            }
        };
        let mut map: HashMap<K, V> = base_entries.drain(..).collect();

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

    pub async fn end(mut self) -> Result<()> {
        self.inner.release_lease().await
    }

    pub async fn commit(mut self) -> Result<()> {
        let snapshot = self.inner.snapshot().clone();
        let store = self.inner.store().clone();
        let pol = store.opts.backoff;
        let timer = store.opts.timer();

        let mut backoff_strategy = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(pol.base_ms))
            .with_max_delay(Duration::from_millis(pol.max_ms))
            .with_factor(pol.multiplier_times_100 as f32 / 100.0)
            .with_max_times(pol.max_retries as usize);

        if pol.jitter_frac_times_100 > 0 {
            backoff_strategy = backoff_strategy.with_jitter();
        }

        if pol.max_backoff_sleep_ms > 0 {
            backoff_strategy = backoff_strategy
                .with_total_delay(Some(Duration::from_millis(pol.max_backoff_sleep_ms)));
        }

        let mut backoff_iter = backoff_strategy.build();

        // Manual timer check for total elapsed time (user-facing operation)
        let start_time = timer.now();
        let max_elapsed = Duration::from_millis(pol.max_elapsed_ms);

        let base_txn = snapshot.txn_id.0;
        let next_txn = base_txn.saturating_add(1);
        let next_seq = snapshot.last_segment_seq.unwrap_or(0).saturating_add(1);
        let expected_tag = snapshot.head_tag.clone();

        let staged = core::mem::take(&mut self.staged);
        let mut records = Vec::with_capacity(staged.len());
        for (k, op, v) in staged.into_iter() {
            records.push(Record {
                key: k,
                op,
                value: v,
            });
        }
        let segment = Segment {
            txn_id: next_txn,
            records,
        };
        let payload = serde_json::to_vec(&segment)
            .map_err(|e| Error::Corrupt(format!("segment encode: {e}")))?;
        let mut seg_written = false;

        loop {
            let head_loaded = store.head.load().await?;
            let stale = match (&expected_tag, head_loaded.as_ref()) {
                (None, None) => false,
                (Some(t), Some((_h, cur))) => t != cur,
                (None, Some(_)) => true,
                (Some(_), None) => true,
            };
            if stale {
                self.inner.release_lease_silent().await;
                return Err(Error::PreconditionFailed);
            }

            if !seg_written {
                match store
                    .segment
                    .put_next(next_seq, next_txn, payload.as_slice(), "application/json")
                    .await
                {
                    Ok(_) => {
                        seg_written = true;
                    }
                    Err(e) => match classify_error(&e) {
                        RetryClass::RetryTransient => {
                            // Check total elapsed time (user-facing operation)
                            if pol.max_elapsed_ms > 0 {
                                let elapsed =
                                    timer.now().duration_since(start_time).unwrap_or_default();
                                if elapsed >= max_elapsed {
                                    self.inner.release_lease_silent().await;
                                    return Err(e);
                                }
                            }

                            // Check retry count + backoff sleep limit
                            if let Some(delay) = backoff_iter.next() {
                                timer.sleep(delay).await;
                                continue;
                            } else {
                                self.inner.release_lease_silent().await;
                                return Err(e);
                            }
                        }
                        _ => {
                            self.inner.release_lease_silent().await;
                            return Err(e);
                        }
                    },
                }
            }

            let cur_head = head_loaded.map(|(h, _)| h);
            let new_head = match cur_head {
                None => HeadJson {
                    version: 1,
                    checkpoint_id: None,
                    last_segment_seq: Some(next_seq),
                    last_txn_id: next_txn,
                },
                Some(h) => HeadJson {
                    version: h.version,
                    checkpoint_id: h.checkpoint_id,
                    last_segment_seq: Some(next_seq),
                    last_txn_id: next_txn,
                },
            };

            let cond = match expected_tag.as_ref() {
                Some(tag) => PutCondition::IfMatch(tag.clone()),
                None => PutCondition::IfNotExists,
            };

            match store.head.put(&new_head, cond).await {
                Ok(_) => {
                    if let Some(lease) = self.inner.lease_mut().take() {
                        let _ = store.leases.release(lease).await;
                    }
                    return Ok(());
                }
                Err(e) => match classify_error(&e) {
                    RetryClass::RetryTransient => {
                        // Check total elapsed time (user-facing operation)
                        if pol.max_elapsed_ms > 0 {
                            let elapsed =
                                timer.now().duration_since(start_time).unwrap_or_default();
                            if elapsed >= max_elapsed {
                                self.inner.release_lease_silent().await;
                                return Err(e);
                            }
                        }

                        // Check retry count + backoff sleep limit
                        if let Some(delay) = backoff_iter.next() {
                            timer.sleep(delay).await;
                            continue;
                        } else {
                            self.inner.release_lease_silent().await;
                            return Err(e);
                        }
                    }
                    RetryClass::DurableConflict => {
                        self.inner.release_lease_silent().await;
                        return Err(Error::PreconditionFailed);
                    }
                    _ => {
                        self.inner.release_lease_silent().await;
                        return Err(e);
                    }
                },
            }
        }
    }

    fn clone_via_json<T: Serialize + DeserializeOwned>(t: &T) -> Result<T> {
        let bytes = serde_json::to_vec(t)
            .map_err(|e| Error::Corrupt(format!("local clone encode: {e}")))?;
        let val = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Corrupt(format!("local clone decode: {e}")))?;
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use fusio::executor::NoopExecutor;
    use futures_executor::block_on;

    use super::*;
    use crate::{context::ManifestContext, testing::new_inmemory_stores};

    #[test]
    fn read_session_end_releases_lease() {
        block_on(async move {
            let (head, seg, ck, ls) = new_inmemory_stores();
            let opts: ManifestContext<DefaultRetention, NoopExecutor> =
                ManifestContext::new(NoopExecutor::default());
            let manifest: crate::manifest::Manifest<String, String, _, _, _, _, NoopExecutor> =
                crate::manifest::Manifest::new_with_context(
                    head,
                    seg,
                    ck,
                    ls.clone(),
                    Arc::new(opts),
                );

            let session = manifest.session_read().await.unwrap();
            session.end().await.unwrap();

            let active = ls.list_active(Duration::from_secs(0)).await.unwrap();
            assert!(active.is_empty());
        })
    }

    #[test]
    fn write_session_commit_conflict() {
        block_on(async move {
            let (head, seg, ck, ls) = new_inmemory_stores();
            let opts: ManifestContext<DefaultRetention, NoopExecutor> =
                ManifestContext::new(NoopExecutor::default());
            let shared = Arc::new(opts);
            let manifest: crate::manifest::Manifest<String, String, _, _, _, _, NoopExecutor> =
                crate::manifest::Manifest::new_with_context(
                    head.clone(),
                    seg.clone(),
                    ck.clone(),
                    ls.clone(),
                    Arc::clone(&shared),
                );

            let mut writer = manifest.session_write().await.unwrap();
            writer.put("k".into(), "v".into());
            writer.commit().await.unwrap();

            let mut writer2 = manifest.session_write().await.unwrap();
            writer2.put("k".into(), "v2".into());

            // Simulate conflicting head advance
            let (_, tag) = head.load().await.unwrap().unwrap();
            head.put(
                &HeadJson {
                    version: 1,
                    checkpoint_id: None,
                    last_segment_seq: Some(10),
                    last_txn_id: 10,
                },
                PutCondition::IfMatch(tag),
            )
            .await
            .unwrap();

            assert!(matches!(
                writer2.commit().await,
                Err(Error::PreconditionFailed)
            ));
        })
    }
}

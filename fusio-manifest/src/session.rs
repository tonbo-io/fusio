use std::{collections::HashMap, hash::Hash, marker::PhantomData, sync::Arc, time::Duration};

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
        let backoff_policy = store.opts.backoff;
        let timer = store.opts.timer();
        let mut backoff_iter = backoff_policy.build_backoff();

        // Manual timer check for total elapsed time (user-facing operation)
        let start_time = timer.now();
        let max_elapsed = Duration::from_millis(backoff_policy.max_elapsed_ms);

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
                            if backoff_policy.max_elapsed_ms > 0 {
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
                        if backoff_policy.max_elapsed_ms > 0 {
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
    use std::{thread, time::Duration};

    use fusio::{executor::NoopExecutor, mem::fs::InMemoryFs};
    use futures_executor::block_on;
    use rstest::rstest;
    use tokio::sync::Barrier;

    use super::*;
    use crate::{
        checkpoint::CheckpointStoreImpl,
        head::HeadStoreImpl,
        lease::LeaseStoreImpl,
        manifest::Manifest,
        segment::SegmentStoreImpl,
        test_utils::{self, in_memory_stores, InMemoryStores},
    };

    type StringManifest = Manifest<
        String,
        String,
        HeadStoreImpl<InMemoryFs>,
        SegmentStoreImpl<InMemoryFs>,
        CheckpointStoreImpl<InMemoryFs>,
        LeaseStoreImpl<InMemoryFs, BlockingExecutor>,
        NoopExecutor,
    >;

    #[rstest]
    fn read_session_end_releases_lease(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let manifest = test_utils::string_in_memory_manifest(in_memory_stores.clone());

            let session = manifest.session_read().await.unwrap();
            session.end().await.unwrap();

            let active = in_memory_stores
                .lease
                .list_active(Duration::from_secs(0))
                .await
                .unwrap();
            assert!(active.is_empty());
        })
    }

    #[rstest]
    fn write_session_commits_fail_with_stale_head(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let manifest = test_utils::string_in_memory_manifest(in_memory_stores.clone());

            let mut writer2 = manifest.session_write().await.unwrap();
            writer2.put("k".into(), "v2".into());

            in_memory_stores
                .head
                .put(
                    &HeadJson {
                        version: 1,
                        checkpoint_id: None,
                        last_segment_seq: Some(10),
                        last_txn_id: 10,
                    },
                    PutCondition::IfNotExists,
                )
                .await
                .unwrap();

            assert!(matches!(
                writer2.commit().await,
                Err(Error::PreconditionFailed)
            ));
        })
    }

    #[rstest]
    fn write_session_commits_fail_with_conflict_last_segment(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let manifest = test_utils::string_in_memory_manifest(in_memory_stores.clone());

            let mut writer2 = manifest.session_write().await.unwrap();
            writer2.put("k2".into(), "v2".into());

            // Simulate writer1 session crashed after putting segments but haven't commited to HEAD
            let segment: Segment<String, &str> = Segment {
                txn_id: 1,
                records: vec![Record {
                    key: "k1".into(),
                    op: Op::Put,
                    value: "v1".into(),
                }],
            };
            let payload = serde_json::to_vec(&segment)
                .map_err(|e| Error::Corrupt(format!("segment encode: {e}")))
                .unwrap();
            let _segment_id = in_memory_stores
                .segment
                .put_next(1, 1, payload.as_slice(), "application/json")
                .await
                .unwrap();
            assert!(manifest
                .get_latest(&"k1".to_owned())
                .await
                .unwrap()
                .is_none());

            // segment put failed as CasCondition::IfNotExists is false
            assert!(matches!(
                writer2.commit().await,
                Err(Error::PreconditionFailed)
            ));

            // Retry succeeds and will also fix the orphan record
            writer2 = manifest.session_write().await.unwrap();
            writer2.put("k2".into(), "v2".into());
            assert!(writer2.commit().await.is_ok());
            assert_eq!(
                manifest.get_latest(&"k1".to_owned()).await.unwrap(),
                Some("v1".to_owned())
            );
            assert_eq!(
                manifest.get_latest(&"k2".to_owned()).await.unwrap(),
                Some("v2".to_owned())
            );
        })
    }

    #[rstest]
    fn read_session_prevents_dirty_and_phantom_read(in_memory_stores: InMemoryStores) {
        block_on(async move {
            let manifest = test_utils::string_in_memory_manifest(in_memory_stores.clone());

            let mut writer = manifest.session_write().await.unwrap();
            writer.put("k".into(), "v1".into());
            writer.commit().await.unwrap();

            let reader = manifest.session_read().await.unwrap();
            assert_eq!(
                reader.get(&"k".to_owned()).await.unwrap(),
                Some("v1".to_owned())
            );

            writer = manifest.session_write().await.unwrap();
            writer.put("k".into(), "v2".into());
            assert_eq!(
                reader.get(&"k".to_owned()).await.unwrap(),
                Some("v1".to_owned())
            );

            writer.commit().await.unwrap();
            assert_eq!(
                reader.get(&"k".to_owned()).await.unwrap(),
                Some("v1".to_owned())
            );

            reader.end().await.unwrap();
        })
    }

    #[rstest]
    fn write_session_concurrent_threads_single_winner_load_test(in_memory_stores: InMemoryStores) {
        let manifest = test_utils::string_in_memory_manifest(in_memory_stores);
        for attempt in 0..100 {
            let mut writer1 = block_on(manifest.session_write()).unwrap();
            let mut writer2 = block_on(manifest.session_write()).unwrap();

            let (res1, res2) = thread::scope(|scope| {
                let handle1 = scope.spawn({
                    let key = format!("thread-1-{attempt}").to_owned();
                    let val = format!("value-1-{attempt}").to_owned();
                    move || {
                        writer1.put(key.clone(), val.clone());
                        block_on(async move {
                            match writer1.commit().await {
                                Ok(()) => Ok(key),
                                Err(err) => Err(err),
                            }
                        })
                    }
                });

                let handle2 = scope.spawn({
                    let key = format!("thread-2-{attempt}").to_owned();
                    let val = format!("value-2-{attempt}").to_owned();
                    move || {
                        writer2.put(key.clone(), val.clone());
                        block_on(async move {
                            match writer2.commit().await {
                                Ok(()) => Ok(key),
                                Err(err) => Err(err),
                            }
                        })
                    }
                });

                (handle1.join().unwrap(), handle2.join().unwrap())
            });

            let success_count = res1.is_ok() as u8 + res2.is_ok() as u8;
            assert_eq!(success_count, 1);
            assert!(
                (res1.is_ok() && matches!(res2, Err(Error::PreconditionFailed)))
                    || (res2.is_ok() && matches!(res1, Err(Error::PreconditionFailed))),
                "expected losing commit to fail with PreconditionFailed in iteration {attempt}: \
                 res1={res1:?}, res2={res2:?}"
            );
        }
    }

    #[tokio::test]
    #[rstest]
    async fn write_session_prevents_write_skew(in_memory_stores: InMemoryStores) {
        let manifest: StringManifest = test_utils::string_in_memory_manifest(in_memory_stores);

        // 1. Initial state: Both key points to the same value
        let mut setup_session = manifest.session_write().await.unwrap();
        setup_session.put("k1".to_string(), "v1".to_string());
        setup_session.put("k2".to_string(), "v1".to_string());
        setup_session.commit().await.unwrap();

        let barrier = Arc::new(Barrier::new(2));

        async fn worker(
            key: String,
            manifest: Arc<StringManifest>,
            barrier: Arc<Barrier>,
        ) -> Result<(), crate::types::Error> {
            let mut session = manifest.session_write().await.unwrap();

            let all_keys = session.scan().await.unwrap();
            let keys_equal_to_v1 = all_keys.into_iter().filter(|(_, v)| &(*v) == "v1").count();

            // Both transactions will see count > 1 and proceed. Barrier ensures they both read
            // before either tries to write.
            barrier.wait().await;
            assert_eq!(keys_equal_to_v1, 2);

            session.put(key, "v2".to_owned());
            return session.commit().await; // This should fail for one of the transactions
        }

        // 2. Run both transactions concurrently.
        let handle1 = tokio::spawn(worker(
            "k1".to_owned(),
            Arc::new(manifest.clone()),
            Arc::clone(&barrier),
        ));
        let handle2 = tokio::spawn(worker(
            "k2".to_owned(),
            Arc::new(manifest.clone()),
            Arc::clone(&barrier),
        ));
        let res_a = handle1.await.unwrap();
        let res_b = handle2.await.unwrap();

        // 3. Assert: One transaction MUST fail. It's a failure if both succeed.
        let success_count = [res_a, res_b].iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            success_count, 1,
            "Expected exactly one transaction to succeed, but got {}",
            success_count
        );

        // 4. Final check: The business rule must be intact.
        let final_state = manifest.scan_latest(None).await.unwrap();
        let final_on_call_count = final_state
            .into_iter()
            .filter(|(_, v)| &(*v) == "v2")
            .count();
        assert_eq!(
            final_on_call_count, 1,
            "The invariant was violated; expected only one key is modified, found {}.",
            final_on_call_count
        );
    }
}

use core::marker::PhantomData;
use std::collections::BTreeMap;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    checkpoint::{CheckpointId, CheckpointMeta, CheckpointStore},
    head::{HeadJson, HeadStore, HeadTag, PutCondition},
    lease::{LeaseHandle, LeaseStore},
    options::Options,
    segment::SegmentIo,
    types::{Commit, Error, Lsn, Result},
};

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Operation on a key (crate-internal).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Op {
    Put,
    Del,
}

/// A single KV record framed into a segment (crate-internal).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record<K, V> {
    pub lsn: u64,
    pub key: K,
    pub op: Op,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<V>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Segment<K, V> {
    version: u32,
    records: Vec<Record<K, V>>,
}

/// Default codec is serde JSON via generic types.
pub trait KeyCodec<K>: Clone + 'static {
    fn encode_key(&self, k: &K) -> K;
    fn decode_key(&self, k: K) -> K;
}

#[derive(Debug, Clone, Default)]
pub struct DefaultKeyCodec;

impl<K: Clone> KeyCodec<K> for DefaultKeyCodec {
    fn encode_key(&self, k: &K) -> K {
        // Identity codec for JSON-encodable keys
        k.clone()
    }
    fn decode_key(&self, k: K) -> K {
        k
    }
}

pub trait ValueCodec<V>: Clone + 'static {
    fn encode_value(&self, v: &V) -> V;
    fn decode_value(&self, v: V) -> V;
}

#[derive(Debug, Clone, Default)]
pub struct DefaultValueCodec;

impl<V: Clone> ValueCodec<V> for DefaultValueCodec {
    fn encode_value(&self, v: &V) -> V {
        v.clone()
    }
    fn decode_value(&self, v: V) -> V {
        v
    }
}

/// A stable snapshot for serializable reads.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub head_tag: Option<HeadTag>,
    pub lsn: Lsn,
    pub last_segment_seq: Option<u64>,
    /// If a checkpoint is published, last seq included in it.
    pub checkpoint_seq: Option<u64>,
    /// The checkpoint id published in HEAD, if any.
    pub checkpoint_id: Option<CheckpointId>,
}

#[derive(Debug, Clone, Default)]
pub struct ReadOptions<K> {
    pub as_of: Option<Snapshot>,
    pub start: Option<K>,
    pub end: Option<K>,
}

/// KV database facade over HeadStore + SegmentIo + CheckpointStore.
pub struct Manifest<K, V, HS, SS, CS, LS, KC = DefaultKeyCodec, VC = DefaultValueCodec>
where
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
    KC: KeyCodec<K> + Clone,
    VC: ValueCodec<V> + Clone,
{
    _phantom: PhantomData<(K, V)>,
    head: HS,
    seg: SS,
    ckpt: CS,
    leases: LS,
    opts: Options,
    kcodec: KC,
    vcodec: VC,
}

impl<K: Clone, V: Clone, HS, SS, CS, LS>
    Manifest<K, V, HS, SS, CS, LS, DefaultKeyCodec, DefaultValueCodec>
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
            kcodec: DefaultKeyCodec,
            vcodec: DefaultValueCodec,
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
            kcodec: DefaultKeyCodec,
            vcodec: DefaultValueCodec,
        }
    }
}

/// Convenience constructors and alias for an in-memory Manifest using Mem backends.
#[cfg(feature = "mem")]
impl<K: Clone, V: Clone>
    Manifest<
        K,
        V,
        crate::head::MemHeadStore,
        crate::segment::MemSegmentStore,
        crate::checkpoint::MemCheckpointStore,
        crate::lease::MemLeaseStore,
        DefaultKeyCodec,
        DefaultValueCodec,
    >
{
    /// Construct an in-memory Manifest with default Options and Mem backends.
    pub fn new_mem() -> Self {
        Self::new(
            crate::head::MemHeadStore::new(),
            crate::segment::MemSegmentStore::new(),
            crate::checkpoint::MemCheckpointStore::new(),
            crate::lease::MemLeaseStore::new(),
        )
    }

    /// Construct an in-memory Manifest with custom Options.
    pub fn new_mem_with_opts(opts: Options) -> Self {
        Self::new_with_opts(
            crate::head::MemHeadStore::new(),
            crate::segment::MemSegmentStore::new(),
            crate::checkpoint::MemCheckpointStore::new(),
            crate::lease::MemLeaseStore::new(),
            opts,
        )
    }
}

/// Type alias for the common in-memory Manifest configuration.
#[cfg(feature = "mem")]
pub type MemManifest<K, V> = Manifest<
    K,
    V,
    crate::head::MemHeadStore,
    crate::segment::MemSegmentStore,
    crate::checkpoint::MemCheckpointStore,
    crate::lease::MemLeaseStore,
>;

impl<K, V, HS, SS, CS, LS, KC, VC> Manifest<K, V, HS, SS, CS, LS, KC, VC>
where
    K: Ord + Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    CS: CheckpointStore + Clone,
    LS: LeaseStore + Clone,
    KC: KeyCodec<K> + Clone,
    VC: ValueCodec<V> + Clone,
{
    pub async fn begin(&self) -> Result<Tx<K, V, HS, SS, LS, KC, VC>> {
        let snap = self.snapshot().await?;
        // create a lease for active snapshot tracking
        let ttl = self.opts.retention.lease_ttl_ms;
        let lease = self
            .leases
            .create(snap.lsn.0, snap.head_tag.clone(), ttl)
            .await?;
        Ok(Tx {
            _phantom: PhantomData,
            head: self.head.clone(),
            seg: self.seg.clone(),
            leases: self.leases.clone(),
            lease: Some(lease),
            kcodec: self.kcodec.clone(),
            vcodec: self.vcodec.clone(),
            snapshot: snap,
            staged: Vec::new(),
        })
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

    pub async fn get(&self, key: &K, ro: &ReadOptions<K>) -> Result<Option<V>> {
        let snap = if let Some(s) = &ro.as_of {
            s.clone()
        } else {
            self.snapshot().await?
        };
        // Fast path point lookup: walk segments from newest to oldest, scan records
        // in reverse, and stop at the first match at/before the snapshot LSN.
        if let Some(last_seq) = snap.last_segment_seq {
            let start_seq = snap
                .checkpoint_seq
                .map(|s| s.saturating_add(1))
                .unwrap_or(0);
            // Gather segment ids up to last_seq (in chunks), then iterate in reverse.
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
            // Not found in post-checkpoint segments; if a checkpoint exists, consult it.
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

    pub async fn scan(&self, ro: ReadOptions<K>) -> Result<Vec<(K, V)>> {
        let snap = if let Some(s) = ro.as_of {
            s
        } else {
            self.snapshot().await?
        };
        let mut map: BTreeMap<K, V> = BTreeMap::new();
        self.fold_until(&mut map, &snap).await?;
        let mut out: Vec<(K, V)> = map.into_iter().collect();
        if let Some(start) = ro.start.as_ref() {
            out.retain(|(k, _)| k >= start);
        }
        if let Some(end) = ro.end.as_ref() {
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
        let payload = serde_json::to_vec(&CkptPayload {
            version: 1,
            entries: entries.clone(),
        })
        .map_err(|e| Error::Corrupt(format!("ckpt encode: {e}")))?;
        // 3b) Meta
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let meta = CheckpointMeta {
            lsn: snap.lsn.0,
            key_count: entries.len(),
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
                let tag = self.head.put(&new_head, PutCondition::IfNotExists).await?;
                Ok((id, tag))
            }
            Some((cur, cur_tag)) => {
                let new_head = HeadJson {
                    version: cur.version,
                    snapshot: Some(id.0.clone()),
                    last_segment_seq: cur.last_segment_seq,
                    last_lsn: cur.last_lsn,
                };
                let tag = self
                    .head
                    .put(&new_head, PutCondition::IfMatch(cur_tag))
                    .await?;
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
        // Segment GC: if watermark has moved past the checkpoint LSN, delete legacy segments.
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

/// Transaction staging KV operations and committing via HEAD CAS.
pub struct Tx<K, V, HS, SS, LS, KC = DefaultKeyCodec, VC = DefaultValueCodec>
where
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    LS: LeaseStore + Clone,
    KC: KeyCodec<K> + Clone,
    VC: ValueCodec<V> + Clone,
{
    _phantom: PhantomData<(K, V)>,
    head: HS,
    seg: SS,
    leases: LS,
    lease: Option<LeaseHandle>,
    kcodec: KC,
    vcodec: VC,
    snapshot: Snapshot,
    staged: Vec<(K, Op, Option<V>)>,
}

impl<K, V, HS, SS, LS, KC, VC> Tx<K, V, HS, SS, LS, KC, VC>
where
    K: Ord + Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
    HS: HeadStore + Clone,
    SS: SegmentIo + Clone,
    LS: LeaseStore + Clone,
    KC: KeyCodec<K> + Clone,
    VC: ValueCodec<V> + Clone,
{
    pub fn put(&mut self, k: K, v: V) {
        self.staged.push((k, Op::Put, Some(v)));
    }

    pub fn delete(&mut self, k: K) {
        self.staged.push((k, Op::Del, None));
    }

    pub async fn commit(mut self) -> Result<Commit> {
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
            let key = self.kcodec.encode_key(&k);
            let val = v.map(|x| self.vcodec.encode_value(&x));
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

#[cfg(all(test, feature = "mem"))]
mod tests {
    use futures_executor::block_on;

    use super::*;
    use crate::{
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
            let mut tx1 = kv.begin().await.unwrap();
            tx1.put("a".into(), "1".into());
            tx1.put("b".into(), "2".into());
            let _c1 = tx1.commit().await.unwrap();

            // Read latest
            let got = kv.get(&"a".into(), &ReadOptions::default()).await.unwrap();
            assert_eq!(got.as_deref(), Some("1"));
            let all = kv.scan(ReadOptions::default()).await.unwrap();
            assert_eq!(all.len(), 2);

            // tx2 and tx3 from same snapshot; tx2 wins, tx3 loses
            let _snap = kv.snapshot().await.unwrap();
            let mut tx2 = kv.begin().await.unwrap();
            let mut tx3 = Manifest::<String, String, _, _, _, _>::new(
                head.clone(),
                seg.clone(),
                ck.clone(),
                ls.clone(),
            )
            .begin()
            .await
            .unwrap();
            tx2.put("a".into(), "10".into());
            tx3.delete("a".into());
            let _ = tx2.commit().await.unwrap();
            let r3 = tx3.commit().await;
            assert!(matches!(r3, Err(Error::PreconditionFailed)));

            let got2 = kv
                .get(
                    &"a".into(),
                    &ReadOptions {
                        as_of: None,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(got2.as_deref(), Some("10"));
        })
    }

    #[test]
    fn mem_kv_point_get_and_tombstone() {
        block_on(async move {
            let head = MemHeadStore::new();
            let seg = MemSegmentStore::new();
            let kv: Manifest<String, String, _, _, _, _> = Manifest::new(
                head.clone(),
                seg.clone(),
                MemCheckpointStore::new(),
                MemLeaseStore::new(),
            );

            // Seed data
            let mut tx = kv.begin().await.unwrap();
            tx.put("a".into(), "1".into());
            tx.put("b".into(), "2".into());
            let _ = tx.commit().await.unwrap();

            // Point gets
            let ga = kv.get(&"a".into(), &ReadOptions::default()).await.unwrap();
            let gb = kv.get(&"b".into(), &ReadOptions::default()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));
            assert_eq!(gb.as_deref(), Some("2"));

            // Update and delete on new snapshot
            let mut tx2 = kv.begin().await.unwrap();
            tx2.put("a".into(), "10".into());
            tx2.delete("b".into());
            let _ = tx2.commit().await.unwrap();

            let ga2 = kv.get(&"a".into(), &ReadOptions::default()).await.unwrap();
            let gb2 = kv.get(&"b".into(), &ReadOptions::default()).await.unwrap();
            assert_eq!(ga2.as_deref(), Some("10"));
            assert_eq!(gb2, None);
        })
    }

    #[test]
    fn mem_kv_compact_and_read_from_checkpoint() {
        block_on(async move {
            let head = MemHeadStore::new();
            let seg = MemSegmentStore::new();
            let ck = MemCheckpointStore::new();
            let ls = MemLeaseStore::new();
            let kv: Manifest<String, String, _, _, _, _> =
                Manifest::new(head.clone(), seg.clone(), ck.clone(), ls.clone());

            // Two transactions → two segments
            let mut tx1 = kv.begin().await.unwrap();
            tx1.put("a".into(), "1".into());
            tx1.put("b".into(), "2".into());
            let _ = tx1.commit().await.unwrap();

            let mut tx2 = kv.begin().await.unwrap();
            tx2.put("b".into(), "20".into());
            tx2.put("c".into(), "3".into());
            let _ = tx2.commit().await.unwrap();

            // Compact
            let (_ckpt_id, _tag) = kv.compact_once().await.unwrap();

            // Snapshot now should include checkpoint_seq
            let snap = kv.snapshot().await.unwrap();
            assert!(snap.checkpoint_seq.is_some());

            // Reads still reflect latest values
            let ga = kv.get(&"a".into(), &ReadOptions::default()).await.unwrap();
            let gb = kv.get(&"b".into(), &ReadOptions::default()).await.unwrap();
            let gc = kv.get(&"c".into(), &ReadOptions::default()).await.unwrap();
            assert_eq!(ga.as_deref(), Some("1"));
            assert_eq!(gb.as_deref(), Some("20"));
            assert_eq!(gc.as_deref(), Some("3"));
        })
    }
}

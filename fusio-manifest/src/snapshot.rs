use crate::{checkpoint::CheckpointId, head::HeadTag, types::TxnId};

/// Stable read snapshot used for serializable reads.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub head_tag: Option<HeadTag>,
    pub txn_id: TxnId,
    pub last_segment_seq: Option<u64>,
    /// If a checkpoint is published, last seq included in it.
    pub checkpoint_seq: Option<u64>,
    /// The checkpoint id published in HEAD, if any.
    pub checkpoint_id: Option<CheckpointId>,
}

/// Range helper for scans.
#[derive(Debug, Clone, Default)]
pub struct ScanRange<K> {
    pub start: Option<K>,
    pub end: Option<K>,
}

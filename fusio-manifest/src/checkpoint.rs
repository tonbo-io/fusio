use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use serde::{Deserialize, Serialize};

use crate::types::Result;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CheckpointId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMeta {
    pub lsn: u64,
    pub key_count: usize,
    pub byte_size: usize,
    pub created_at_ms: u64,
    pub format: String,
    /// The last segment sequence number included in this checkpoint.
    pub last_segment_seq_at_ckpt: u64,
}

pub trait CheckpointStore: MaybeSend + MaybeSync + Clone {
    fn put_checkpoint(
        &self,
        meta: &CheckpointMeta,
        payload: &[u8],
        content_type: &str,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<CheckpointId>>>>;

    fn get_checkpoint(
        &self,
        id: &CheckpointId,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>>>>;

    /// List all checkpoints (meta only). Default returns Unimplemented.
    fn list(
        &self,
    ) -> core::pin::Pin<
        Box<dyn MaybeSendFuture<Output = Result<Vec<(CheckpointId, CheckpointMeta)>>>>,
    >
    where
        Self: Sized,
    {
        Box::pin(async move { Err(crate::types::Error::Unimplemented("ckpt list")) })
    }

    /// Delete a checkpoint by id. Default returns Unimplemented.
    fn delete(
        &self,
        _id: &CheckpointId,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>>
    where
        Self: Sized,
    {
        Box::pin(async move { Err(crate::types::Error::Unimplemented("ckpt delete")) })
    }
}

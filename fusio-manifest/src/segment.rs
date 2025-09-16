use core::pin::Pin;

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};

use crate::types::{Result, SegmentId};

/// Abstraction for immutable segment IO on durable storage.
pub trait SegmentIo: MaybeSend + MaybeSync {
    /// Write the given payload as a new segment with a writer-provided sequence number.
    /// Returns the durable segment identifier.
    fn put_next(
        &self,
        seq: u64,
        payload: &[u8],
        content_type: &str,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<SegmentId>>>>;

    /// Fetch a previously written segment payload by id.
    fn get(&self, id: &SegmentId) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<u8>>>>>;

    /// List segment ids starting from a minimum sequence number (inclusive), up to `limit` items.
    fn list_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<SegmentId>>>>>
    where
        Self: Sized;

    /// Delete all segments with sequence number <= upto (best-effort; idempotent).
    /// Default implementation returns Unimplemented.
    fn delete_upto(&self, _upto_seq: u64) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>>
    where
        Self: Sized,
    {
        Box::pin(async move { Err(crate::types::Error::Unimplemented("segment delete")) })
    }
}

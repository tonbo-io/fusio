#![cfg(test)]

use core::pin::Pin;
use std::sync::{Arc, Mutex};

use fusio_core::MaybeSendFuture;

use crate::{
    segment::SegmentIo,
    types::{Result, SegmentId},
};

/// In-memory segment store for tests.
#[derive(Debug, Clone, Default)]
pub struct MemSegmentStore {
    inner: Arc<Mutex<Vec<(u64, Vec<u8>, String)>>>,
}

impl MemSegmentStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SegmentIo for MemSegmentStore {
    fn put_next(
        &self,
        seq: u64,
        payload: &[u8],
        content_type: &str,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<SegmentId>>>> {
        let inner = self.inner.clone();
        let ct = content_type.to_string();
        let data = payload.to_vec();
        Box::pin(async move {
            inner.lock().unwrap().push((seq, data, ct));
            Ok(SegmentId { seq })
        })
    }

    fn get(&self, id: &SegmentId) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<u8>>>>> {
        let inner = self.inner.clone();
        let seq = id.seq;
        Box::pin(async move {
            let guard = inner.lock().unwrap();
            let found = guard.iter().find(|(s, _, _)| *s == seq);
            match found {
                Some((_s, data, _ct)) => Ok(data.clone()),
                None => Err(crate::types::Error::Corrupt(format!(
                    "segment seq {} not found",
                    seq
                ))),
            }
        })
    }

    fn list_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<SegmentId>>>>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut v: Vec<_> = inner
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(seq, _data, _)| if *seq >= from_seq { Some(*seq) } else { None })
                .collect();
            v.sort_unstable();
            v.truncate(limit);
            Ok(v.into_iter().map(|seq| SegmentId { seq }).collect())
        })
    }

    fn delete_upto(&self, upto_seq: u64) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut g = inner.lock().unwrap();
            g.retain(|(seq, _data, _)| *seq > upto_seq);
            Ok(())
        })
    }
}

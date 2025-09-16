#![cfg(test)]

use core::marker::PhantomData;
use std::sync::{Arc, Mutex};

use fusio_core::MaybeSendFuture;

use crate::{
    checkpoint::{CheckpointId, CheckpointMeta, CheckpointStore},
    types::Result,
};

#[derive(Debug, Clone, Default)]
pub struct MemCheckpointStore {
    _phantom: PhantomData<u8>,
    inner: Arc<Mutex<Vec<(CheckpointId, CheckpointMeta, Vec<u8>, String)>>>,
}

impl MemCheckpointStore {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
            inner: Default::default(),
        }
    }
}

impl CheckpointStore for MemCheckpointStore {
    fn put_checkpoint(
        &self,
        meta: &CheckpointMeta,
        payload: &[u8],
        content_type: &str,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<CheckpointId>>>> {
        let id = CheckpointId(format!("ckpt-{:020}", meta.lsn));
        let inner = self.inner.clone();
        let meta = meta.clone();
        let data = payload.to_vec();
        let ct = content_type.to_string();
        Box::pin(async move {
            inner.lock().unwrap().push((id.clone(), meta, data, ct));
            Ok(id)
        })
    }

    fn get_checkpoint(
        &self,
        id: &CheckpointId,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<(CheckpointMeta, Vec<u8>)>>>> {
        let inner = self.inner.clone();
        let id = id.clone();
        Box::pin(async move {
            let guard = inner.lock().unwrap();
            let (meta, data) = guard
                .iter()
                .find(|(cid, _, _, _)| *cid == id)
                .map(|(_, m, d, _)| (m.clone(), d.clone()))
                .ok_or_else(|| crate::types::Error::Corrupt("checkpoint not found".into()))?;
            Ok((meta, data))
        })
    }

    fn list(
        &self,
    ) -> core::pin::Pin<
        Box<dyn MaybeSendFuture<Output = Result<Vec<(CheckpointId, CheckpointMeta)>>>>,
    >
    where
        Self: Sized,
    {
        let inner = self.inner.clone();
        Box::pin(async move {
            let g = inner.lock().unwrap();
            Ok(g.iter()
                .map(|(id, m, _d, _)| (id.clone(), m.clone()))
                .collect())
        })
    }

    fn delete(
        &self,
        id: &CheckpointId,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>>
    where
        Self: Sized,
    {
        let inner = self.inner.clone();
        let id = id.clone();
        Box::pin(async move {
            let mut g = inner.lock().unwrap();
            g.retain(|(cid, _m, _d, _)| *cid != id);
            Ok(())
        })
    }
}

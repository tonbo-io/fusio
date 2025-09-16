#![cfg(test)]

use core::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use fusio_core::MaybeSendFuture;

use crate::{
    gc::{GcPlan, GcPlanStore, GcTag},
    head::PutCondition,
    types::Error,
};

/// Minimal in-memory GC plan store for testing.
#[derive(Debug, Default, Clone)]
pub struct MemGcPlanStore {
    inner: std::sync::Arc<std::sync::Mutex<Option<(GcPlan, GcTag)>>>,
    ctr: std::sync::Arc<AtomicU64>,
}

impl MemGcPlanStore {
    pub fn new() -> Self {
        Self::default()
    }
    fn next_tag(&self) -> GcTag {
        let n = self.ctr.fetch_add(1, Ordering::Relaxed) + 1;
        GcTag(format!("mem-gc-{}", n))
    }
}

impl GcPlanStore for MemGcPlanStore {
    fn load(
        &self,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(GcPlan, GcTag)>, Error>>>> {
        let v = self.inner.lock().unwrap().clone();
        Box::pin(async move { Ok(v) })
    }

    fn put(
        &self,
        plan: &GcPlan,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<GcTag, Error>>>> {
        let mut guard = self.inner.lock().unwrap();
        let res = match (&*guard, cond) {
            (None, PutCondition::IfNotExists) => {
                let tag = self.next_tag();
                *guard = Some((plan.clone(), tag.clone()));
                Ok(tag)
            }
            (Some((_p, cur)), PutCondition::IfMatch(expected)) => {
                if GcTag(expected.0) == *cur {
                    let tag = self.next_tag();
                    *guard = Some((plan.clone(), tag.clone()));
                    Ok(tag)
                } else {
                    Err(Error::PreconditionFailed)
                }
            }
            (Some(_), PutCondition::IfNotExists) => Err(Error::PreconditionFailed),
            (None, PutCondition::IfMatch(_)) => Err(Error::PreconditionFailed),
        };
        Box::pin(async move { res })
    }
}

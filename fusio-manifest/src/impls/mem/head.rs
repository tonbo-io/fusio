#![cfg(test)]

use core::pin::Pin;
use std::sync::{Arc, Mutex};

use fusio_core::MaybeSendFuture;

use crate::{
    head::{HeadJson, HeadStore, HeadTag, PutCondition},
    types::Error,
};

/// Minimal in-memory head store for testing.
#[derive(Debug, Default, Clone)]
pub struct MemHeadStore {
    head: Arc<Mutex<Option<(HeadJson, HeadTag)>>>,
}

impl MemHeadStore {
    pub fn new() -> Self {
        Self {
            head: Arc::new(Mutex::new(None)),
        }
    }
}

impl HeadStore for MemHeadStore {
    fn load(
        &self,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, Error>>>> {
        let v = self.head.lock().unwrap().clone();
        Box::pin(async move { Ok(v) })
    }

    fn put(
        &self,
        head: &HeadJson,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<HeadTag, Error>>>> {
        let mut guard = self.head.lock().unwrap();
        let res = match (&*guard, cond) {
            (None, PutCondition::IfNotExists) => {
                let tag = HeadTag(format!(
                    "v{}:{}:{}",
                    head.version,
                    head.last_txn_id,
                    guard.is_some()
                ));
                *guard = Some((head.clone(), tag.clone()));
                Ok(tag)
            }
            (Some((_h, cur)), PutCondition::IfMatch(expected)) => {
                if *cur == expected {
                    let tag = HeadTag(format!("v{}:{}:{}", head.version, head.last_txn_id, true));
                    *guard = Some((head.clone(), tag.clone()));
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

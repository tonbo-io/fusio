use core::pin::Pin;

use fusio::{
    fs::{CasCondition, FsCas},
    path::Path,
    Error as FsError,
};
use fusio_core::MaybeSendFuture;

use crate::{
    backoff::{classify_error, ExponentialBackoff, RetryClass, TimerHandle},
    gc::{GcPlan, GcPlanStore, GcTag},
    head::PutCondition,
    types::Error,
    BackoffPolicy,
};

#[derive(Clone)]
pub struct FsGcPlanStore<C> {
    cas: C,
    key: String,
    backoff: BackoffPolicy,
    timer: TimerHandle,
}

impl<C> FsGcPlanStore<C> {
    pub fn new(
        cas: C,
        prefix: impl Into<String>,
        backoff: BackoffPolicy,
        timer: TimerHandle,
    ) -> Self {
        let prefix = prefix.into().trim_end_matches('/').to_string();
        let key = if prefix.is_empty() {
            "gc/GARBAGE".to_string()
        } else {
            format!("{}/gc/GARBAGE", prefix)
        };
        Self {
            cas,
            key,
            backoff,
            timer,
        }
    }
}

impl<C> GcPlanStore for FsGcPlanStore<C>
where
    C: FsCas + Clone + Send + Sync + 'static,
{
    fn load(
        &self,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Option<(GcPlan, GcTag)>, Error>>>> {
        let cas = self.cas.clone();
        let key = self.key.clone();
        let backoff = self.backoff;
        let timer = self.timer.clone();
        Box::pin(async move {
            let path = Path::parse(&key).map_err(|e| Error::Other(Box::new(e)))?;
            let mut bo = ExponentialBackoff::new(backoff);
            loop {
                match cas.load_with_tag(&path).await.map_err(map_fs_error) {
                    Ok(None) => return Ok(None),
                    Ok(Some((bytes, etag))) => {
                        let plan: GcPlan = serde_json::from_slice(&bytes)
                            .map_err(|e| Error::Corrupt(format!("invalid gc plan json: {e}")))?;
                        return Ok(Some((plan, GcTag(etag))));
                    }
                    Err(err) => match classify_error(&err) {
                        RetryClass::RetryTransient if !bo.exhausted() => {
                            let delay = bo.next_delay();
                            timer.sleep(delay).await;
                            continue;
                        }
                        _ => return Err(err),
                    },
                }
            }
        })
    }

    fn put(
        &self,
        plan: &GcPlan,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<GcTag, Error>>>> {
        let cas = self.cas.clone();
        let key = self.key.clone();
        let body = match serde_json::to_vec(plan) {
            Ok(v) => v,
            Err(e) => {
                return Box::pin(
                    async move { Err(Error::Corrupt(format!("serialize gc plan: {e}"))) },
                )
            }
        };
        let backoff = self.backoff;
        let timer = self.timer.clone();
        Box::pin(async move {
            let cond = cond.clone();
            let path = Path::parse(&key).map_err(|e| Error::Other(Box::new(e)))?;
            let mut bo = ExponentialBackoff::new(backoff);
            loop {
                let condition = match &cond {
                    PutCondition::IfNotExists => CasCondition::IfNotExists,
                    PutCondition::IfMatch(tag) => CasCondition::IfMatch(tag.0.clone()),
                };
                match cas
                    .put_conditional(&path, body.clone(), Some("application/json"), condition)
                    .await
                    .map_err(map_fs_error)
                {
                    Ok(etag) => return Ok(GcTag(etag)),
                    Err(err) => match classify_error(&err) {
                        RetryClass::RetryTransient if !bo.exhausted() => {
                            let delay = bo.next_delay();
                            timer.sleep(delay).await;
                            continue;
                        }
                        _ => return Err(err),
                    },
                }
            }
        })
    }
}

fn map_fs_error(err: FsError) -> Error {
    match err {
        FsError::PreconditionFailed => Error::PreconditionFailed,
        other => Error::Other(Box::new(other)),
    }
}

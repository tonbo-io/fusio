use core::pin::Pin;

use fusio::{
    fs::{CasCondition, FsCas},
    path::Path,
    Error as FsError,
};
use fusio_core::MaybeSendFuture;

use crate::head::{HeadJson, HeadStore, HeadTag, PutCondition};

fn map_fs_error(err: FsError) -> crate::types::Error {
    match err {
        FsError::PreconditionFailed => crate::types::Error::PreconditionFailed,
        other => crate::types::Error::Other(Box::new(other)),
    }
}

#[derive(Clone)]
pub struct FsHeadStore<C> {
    cas: C,
    key: String,
}

impl<C> FsHeadStore<C> {
    pub fn new(cas: C, key: impl Into<String>) -> Self {
        Self {
            cas,
            key: key.into(),
        }
    }
}

impl<C> HeadStore for FsHeadStore<C>
where
    C: FsCas + Clone + Send + Sync + 'static,
{
    fn load(
        &self,
    ) -> Pin<
        Box<dyn MaybeSendFuture<Output = Result<Option<(HeadJson, HeadTag)>, crate::types::Error>>>,
    > {
        let cas = self.cas.clone();
        let key = self.key.clone();
        Box::pin(async move {
            let path = Path::parse(&key).map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            match cas.load_with_tag(&path).await.map_err(map_fs_error)? {
                None => Ok(None),
                Some((bytes, tag)) => {
                    let head: HeadJson = serde_json::from_slice(&bytes).map_err(|e| {
                        crate::types::Error::Corrupt(format!("invalid head json: {e}"))
                    })?;
                    Ok(Some((head, HeadTag(tag))))
                }
            }
        })
    }

    fn put(
        &self,
        head: &HeadJson,
        cond: PutCondition,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<HeadTag, crate::types::Error>>>> {
        let cas = self.cas.clone();
        let key = self.key.clone();
        let body = match serde_json::to_vec(head) {
            Ok(v) => v,
            Err(e) => {
                return Box::pin(async move {
                    Err(crate::types::Error::Corrupt(format!("serialize head: {e}")))
                })
            }
        };
        Box::pin(async move {
            let path = Path::parse(&key).map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            let condition = match cond {
                PutCondition::IfNotExists => CasCondition::IfNotExists,
                PutCondition::IfMatch(tag) => CasCondition::IfMatch(tag.0.clone()),
            };
            let tag = cas
                .put_conditional(&path, body, Some("application/json"), condition)
                .await
                .map_err(map_fs_error)?;
            Ok(HeadTag(tag))
        })
    }
}

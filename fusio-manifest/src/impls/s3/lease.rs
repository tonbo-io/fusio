use core::pin::Pin;

use bytes::Bytes;
use fusio::{
    fs::Fs as _,
    impls::remotes::aws::{
        fs::AmazonS3,
        head::{get_with_etag, put_if_match, put_if_none_match, ETag},
    },
    path::Path,
    Read,
};
use fusio_core::MaybeSendFuture;
use serde::{Deserialize, Serialize};

use crate::{
    head::HeadTag,
    lease::{ActiveLease, LeaseHandle, LeaseId, LeaseStore},
    types::Error,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseDoc {
    id: String,
    snapshot_lsn: u64,
    expires_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    head_tag: Option<String>,
}

/// S3-backed LeaseStore that keeps JSON docs under `<prefix>/leases/`.
#[derive(Clone)]
pub struct S3LeaseStore {
    s3: AmazonS3,
    prefix: String,
}

impl S3LeaseStore {
    pub fn new(s3: AmazonS3, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into().trim_end_matches('/').to_string();
        Self { s3, prefix }
    }

    fn key_for(&self, id: &str) -> String {
        if self.prefix.is_empty() {
            format!("leases/{}.json", id)
        } else {
            format!("{}/leases/{}.json", self.prefix, id)
        }
    }

    fn unix_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl LeaseStore for S3LeaseStore {
    fn create(
        &self,
        snapshot_lsn: u64,
        head_tag: Option<HeadTag>,
        ttl_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<LeaseHandle, crate::types::Error>>>> {
        let s3 = self.s3.clone();
        let prefix = self.prefix.clone();
        Box::pin(async move {
            let now = Self::unix_ms();
            let expires_at_ms = now.saturating_add(ttl_ms);
            // Deterministic unique-ish id with retry on collision: lease-<ms>-<ctr>
            let mut ctr: u32 = 0;
            loop {
                let id = if ctr == 0 {
                    format!("lease-{}", now)
                } else {
                    format!("lease-{}-{}", now, ctr)
                };
                let key = format!("{}/leases/{}.json", prefix, id);
                let doc = LeaseDoc {
                    id: id.clone(),
                    snapshot_lsn,
                    expires_at_ms,
                    head_tag: head_tag.as_ref().map(|t| t.0.clone()),
                };
                let body = serde_json::to_vec(&doc)
                    .map_err(|e| Error::Corrupt(format!("lease encode: {e}")))?;
                match put_if_none_match(&s3, &key, Bytes::from(body), Some("application/json"))
                    .await
                {
                    Ok(_etag) => {
                        return Ok(LeaseHandle {
                            id: LeaseId(id),
                            snapshot_lsn,
                        });
                    }
                    Err(_e) => {
                        // TODO: check error code for 412; backoff with jitter on busy
                        // prefixes.
                        ctr = ctr.saturating_add(1);
                        if ctr > 16 {
                            return Err(Error::PreconditionFailed);
                        }
                    }
                }
            }
        })
    }

    fn heartbeat(
        &self,
        lease: &LeaseHandle,
        ttl_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), crate::types::Error>>>> {
        let s3 = self.s3.clone();
        let key = self.key_for(&lease.id.0);
        Box::pin(async move {
            // Load with ETag, update expiry, write If-Match
            let (bytes, etag) = match get_with_etag(&s3, &key).await {
                Ok(Some(v)) => v,
                Ok(None) => return Err(Error::Corrupt("lease missing".into())),
                Err(e) => return Err(Error::Other(Box::new(e))),
            };
            let mut doc: LeaseDoc = serde_json::from_slice(&bytes)
                .map_err(|e| Error::Corrupt(format!("lease decode: {e}")))?;
            // Preserve id/snapshot_lsn; extend expiry
            let now = Self::unix_ms();
            doc.expires_at_ms = now.saturating_add(ttl_ms);
            let body = serde_json::to_vec(&doc)
                .map_err(|e| Error::Corrupt(format!("lease encode: {e}")))?;
            let _ = put_if_match(
                &s3,
                &key,
                Bytes::from(body),
                &ETag(etag.0),
                Some("application/json"),
            )
            .await
            .map_err(|e| Error::Other(Box::new(e)))?;
            Ok(())
        })
    }

    fn release(
        &self,
        lease: LeaseHandle,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<(), crate::types::Error>>>> {
        let s3 = self.s3.clone();
        let key = self.key_for(&lease.id.0);
        Box::pin(async move {
            let _ = s3.remove(&Path::from(key)).await; // best-effort
            Ok(())
        })
    }

    fn list_active(
        &self,
        now_ms: u64,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<ActiveLease>, crate::types::Error>>>> {
        let s3 = self.s3.clone();
        let dir = if self.prefix.is_empty() {
            "leases".to_string()
        } else {
            format!("{}/leases", self.prefix)
        };
        Box::pin(async move {
            use futures_util::StreamExt;
            let mut out = Vec::new();
            let prefix_path = Path::from(dir.clone());
            let stream = s3
                .list(&prefix_path)
                .await
                .map_err(|e| Error::Other(Box::new(e)))?;
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| Error::Other(Box::new(e)))?;
                // Read JSON doc
                let mut f = s3
                    .open(&meta.path)
                    .await
                    .map_err(|e| Error::Other(Box::new(e)))?;
                let (res, bytes) = f.read_to_end_at(Vec::new(), 0).await;
                res.map_err(|e| Error::Other(Box::new(e)))?;
                if let Ok(doc) = serde_json::from_slice::<LeaseDoc>(&bytes) {
                    if doc.expires_at_ms > now_ms {
                        out.push(ActiveLease {
                            id: LeaseId(doc.id),
                            snapshot_lsn: doc.snapshot_lsn,
                            expires_at_ms: doc.expires_at_ms,
                        });
                    }
                }
            }
            Ok(out)
        })
    }
}

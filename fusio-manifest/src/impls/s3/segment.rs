use core::pin::Pin;

use bytes::Bytes;
use fusio::{
    durability::FileCommit,
    fs::{Fs, OpenOptions},
    impls::remotes::aws::fs::AmazonS3,
    path::Path,
    Write,
};
use fusio_core::MaybeSendFuture;
use futures_util::StreamExt;

use crate::{
    segment::SegmentIo,
    types::{Result, SegmentId},
};

/// S3-backed SegmentStore that writes each segment object under a fixed prefix.
#[derive(Clone)]
#[allow(dead_code)] // staging: used by object-store backend in upcoming wiring
pub struct S3SegmentStore {
    s3: AmazonS3,
    pub(crate) prefix: String,
}

#[allow(dead_code)] // staging
impl S3SegmentStore {
    pub fn new(s3: AmazonS3, prefix: impl Into<String>) -> Self {
        Self {
            s3,
            prefix: prefix.into(),
        }
    }

    fn key_for(&self, seq: u64, ext: &str) -> String {
        if self.prefix.is_empty() {
            format!("seg-{:020}{}", seq, ext)
        } else {
            format!("{}/seg-{:020}{}", self.prefix, seq, ext)
        }
    }

    fn parse_seq(filename: &str) -> Option<u64> {
        if let Some(rest) = filename.strip_prefix("seg-") {
            let (num, _) = rest.split_once('.').unwrap_or((rest, ""));
            if let Ok(v) = num.parse::<u64>() {
                return Some(v);
            }
        }
        None
    }
}

impl SegmentIo for S3SegmentStore {
    fn put_next(
        &self,
        seq: u64,
        payload: &[u8],
        content_type: &str,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<SegmentId>>>> {
        // Use extension based on content type for readability.
        let ext = match content_type {
            "application/json" => ".json",
            _ => ".bin",
        };
        let key = self.key_for(seq, ext);
        let s3 = self.s3.clone();
        let payload = payload.to_vec();
        Box::pin(async move {
            let mut file = s3
                .open_options(
                    &Path::from(key.clone()),
                    OpenOptions::default()
                        .create(true)
                        .truncate(true)
                        .write(true),
                )
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            let (res, _) = file.write_all(Bytes::from(payload)).await;
            res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            // Finalize MPU / publish visibility when applicable
            FileCommit::commit(&mut file)
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            Ok(SegmentId { seq })
        })
    }

    fn get(&self, id: &SegmentId) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<u8>>>>> {
        let s3 = self.s3.clone();
        let key_json = self.key_for(id.seq, ".json");
        let key_bin = self.key_for(id.seq, ".bin");
        Box::pin(async move {
            // Helper to read whole object
            async fn read_all(s3: &AmazonS3, key: &str) -> Result<Vec<u8>, crate::types::Error> {
                use fusio::{fs::Fs, path::Path as FusioPath, Read};
                let mut f = s3
                    .open(&FusioPath::from(key.to_string()))
                    .await
                    .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                let (res, buf) = f.read_to_end_at(Vec::new(), 0).await;
                res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                Ok(buf)
            }

            match read_all(&s3, &key_json).await {
                Ok(v) => Ok(v),
                Err(_) => read_all(&s3, &key_bin).await,
            }
        })
    }

    fn list_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<SegmentId>>>>> {
        let s3 = self.s3.clone();
        let prefix = self.prefix.clone();
        Box::pin(async move {
            use fusio::{fs::Fs as _, path::Path as FusioPath};

            let mut out = Vec::new();
            let prefix_path = FusioPath::from(prefix.clone());
            let stream = s3
                .list(&prefix_path)
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            let mut stream = Box::pin(stream);
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                if let Some(filename) = meta.path.filename() {
                    if let Some(seq) = S3SegmentStore::parse_seq(filename) {
                        if seq >= from_seq {
                            out.push(SegmentId { seq });
                        }
                    }
                }
                if out.len() >= limit {
                    break;
                }
            }
            out.sort_by_key(|s| s.seq);
            Ok(out)
        })
    }

    fn delete_upto(&self, upto_seq: u64) -> Pin<Box<dyn MaybeSendFuture<Output = Result<()>>>> {
        let s3 = self.s3.clone();
        let prefix = self.prefix.clone();
        Box::pin(async move {
            use fusio::{fs::Fs as _, path::Path as FusioPath};
            let prefix_path = FusioPath::from(prefix.clone());
            let stream = s3
                .list(&prefix_path)
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                if let Some(filename) = meta.path.filename() {
                    if let Some(seq) = S3SegmentStore::parse_seq(filename) {
                        if seq <= upto_seq {
                            // TODO: use batch delete API when available.
                            let _ = s3.remove(&meta.path).await; // best-effort
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

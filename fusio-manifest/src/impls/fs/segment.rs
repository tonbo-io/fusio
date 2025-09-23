use core::pin::Pin;

use fusio::{
    fs::{CasCondition, Fs, FsCas},
    impls::remotes::aws::fs::AmazonS3,
    path::Path,
    Error as FsError, Read,
};
use fusio_core::MaybeSendFuture;
use futures_util::StreamExt;

use crate::{
    segment::SegmentIo,
    types::{Result, SegmentId},
};

/// Segment store backed by an Amazon S3 filesystem handle.
#[derive(Clone)]
pub struct FsSegmentStore {
    fs: AmazonS3,
    pub(crate) prefix: String,
}

impl FsSegmentStore {
    pub fn new(fs: AmazonS3, prefix: impl Into<String>) -> Self {
        Self {
            fs,
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

impl SegmentIo for FsSegmentStore {
    fn put_next(
        &self,
        seq: u64,
        payload: &[u8],
        content_type: &str,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<SegmentId>>>> {
        let ext = match content_type {
            "application/json" => ".json",
            _ => ".bin",
        };
        let key = self.key_for(seq, ext);
        let fs = self.fs.clone();
        let body = payload.to_vec();
        let ct = if content_type.is_empty() {
            None
        } else {
            Some(content_type.to_string())
        };
        Box::pin(async move {
            let path = Path::parse(&key).map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            fs.put_conditional(&path, body, ct.as_deref(), CasCondition::IfNotExists)
                .await
                .map_err(map_fs_error)?;
            Ok(SegmentId { seq })
        })
    }

    fn get(&self, id: &SegmentId) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<u8>>>>> {
        let fs = self.fs.clone();
        let key_json = self.key_for(id.seq, ".json");
        let key_bin = self.key_for(id.seq, ".bin");
        Box::pin(async move {
            async fn read_all(fs: &AmazonS3, key: &str) -> Result<Vec<u8>, crate::types::Error> {
                let path = Path::parse(key).map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                let mut f = fs
                    .open(&path)
                    .await
                    .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                let (res, buf) = f.read_to_end_at(Vec::new(), 0).await;
                res.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                Ok(buf)
            }

            match read_all(&fs, &key_json).await {
                Ok(v) => Ok(v),
                Err(_) => read_all(&fs, &key_bin).await,
            }
        })
    }

    fn list_from(
        &self,
        from_seq: u64,
        limit: usize,
    ) -> Pin<Box<dyn MaybeSendFuture<Output = Result<Vec<SegmentId>>>>> {
        let fs = self.fs.clone();
        let prefix = self.prefix.clone();
        Box::pin(async move {
            use fusio::path::Path as FusioPath;

            let mut out = Vec::new();
            let prefix_path = FusioPath::from(prefix.clone());
            let stream = fs
                .list(&prefix_path)
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            let mut stream = Box::pin(stream);
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                if let Some(filename) = meta.path.filename() {
                    if let Some(seq) = FsSegmentStore::parse_seq(filename) {
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
        let fs = self.fs.clone();
        let prefix = self.prefix.clone();
        Box::pin(async move {
            use fusio::path::Path as FusioPath;
            let prefix_path = FusioPath::from(prefix.clone());
            let stream = fs
                .list(&prefix_path)
                .await
                .map_err(|e| crate::types::Error::Other(Box::new(e)))?;
            futures_util::pin_mut!(stream);
            while let Some(item) = stream.next().await {
                let meta = item.map_err(|e| crate::types::Error::Other(Box::new(e)))?;
                if let Some(filename) = meta.path.filename() {
                    if let Some(seq) = FsSegmentStore::parse_seq(filename) {
                        if seq <= upto_seq {
                            let _ = fs.remove(&meta.path).await;
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

fn map_fs_error(err: FsError) -> crate::types::Error {
    match err {
        FsError::PreconditionFailed => crate::types::Error::PreconditionFailed,
        other => crate::types::Error::Other(Box::new(other)),
    }
}

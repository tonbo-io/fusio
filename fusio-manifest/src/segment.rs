use core::pin::Pin;
#[cfg(any(test, feature = "mem"))]
use std::sync::{Arc, Mutex};

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

#[cfg(any(feature = "aws-tokio", feature = "aws-wasm"))]
pub mod s3 {
    use bytes::Bytes;
    use fusio::{
        durability::FileCommit,
        fs::{Fs, OpenOptions},
        impls::remotes::aws::fs::AmazonS3,
        path::Path,
        Write,
    };
    use futures_util::StreamExt;

    use super::*;

    /// S3-backed SegmentStore that writes each segment object under a fixed prefix.
    #[derive(Clone)]
    pub struct S3SegmentStore {
        s3: AmazonS3,
        prefix: String,
    }

    impl S3SegmentStore {
        pub fn new(s3: AmazonS3, prefix: impl Into<String>) -> Self {
            Self {
                s3,
                prefix: prefix.into(),
            }
        }

        fn key_for(&self, seq: u64, ext: &str) -> String {
            format!("{}/seg-{:020}{}", self.prefix, seq, ext)
        }

        fn parse_seq(filename: &str) -> Option<u64> {
            // expected: seg-00000000000000000000.json or .bin
            if let Some(rest) = filename.strip_prefix("seg-") {
                let num = rest.split_once('.').map(|(n, _)| n).unwrap_or(rest);
                return num.parse::<u64>().ok();
            }
            None
        }
    }

    impl super::SegmentIo for S3SegmentStore {
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
            // Try JSON first, then BIN for flexibility
            let key_json = self.key_for(id.seq, ".json");
            let key_bin = self.key_for(id.seq, ".bin");
            Box::pin(async move {
                // Helper to read whole object
                async fn read_all(
                    s3: &AmazonS3,
                    key: &str,
                ) -> Result<Vec<u8>, crate::types::Error> {
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
    }
}

/// In-memory segment store for tests.
#[cfg(feature = "mem")]
#[derive(Debug, Clone, Default)]
pub struct MemSegmentStore {
    inner: Arc<Mutex<Vec<(u64, Vec<u8>, String)>>>,
}

#[cfg(feature = "mem")]
impl MemSegmentStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(feature = "mem")]
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

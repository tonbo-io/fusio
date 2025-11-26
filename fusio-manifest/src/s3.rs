use core::hash::Hash;
use std::sync::Arc;

use fusio::{
    executor::{Executor, Timer},
    impls::remotes::aws::{
        credential::AwsCredential,
        fs::{AmazonS3, AmazonS3Builder},
    },
};
use fusio_core::{MaybeSend, MaybeSync};

#[cfg(feature = "cache-moka")]
use crate::cache::{BlobCache, MemoryBlobCache};
use crate::{
    backoff::BackoffPolicy,
    checkpoint::CheckpointStoreImpl,
    compactor::Compactor,
    context::ManifestContext,
    gc::FsGcPlanStore,
    head::HeadStoreImpl,
    lease::LeaseStoreImpl,
    manifest::Manifest,
    retention::{DefaultRetention, RetentionPolicy},
    segment::SegmentStoreImpl,
    session::{ReadSession, WriteSession},
    snapshot::Snapshot,
    types::Result,
    BlockingExecutor,
};

/// Default file name for the manifest head object.
pub const DEFAULT_HEAD_FILE: &str = "HEAD.json";

/// Default maximum cache size for S3 manifests (in bytes).
#[cfg(feature = "cache-moka")]
const DEFAULT_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Minimal S3 configuration for a manifest collection under a single prefix.
pub struct Config<R = DefaultRetention, E = BlockingExecutor>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
{
    pub s3: AmazonS3,
    pub prefix: String,
    pub opts: Arc<ManifestContext<R, E>>,
}

impl Config<DefaultRetention, BlockingExecutor> {
    pub fn new(s3: AmazonS3, prefix: impl Into<String>) -> Self {
        #[cfg_attr(not(feature = "cache-moka"), allow(unused_mut))]
        let mut opts = ManifestContext::default();
        #[cfg(feature = "cache-moka")]
        {
            let cache: Arc<dyn BlobCache> = Arc::new(MemoryBlobCache::new(DEFAULT_CACHE_MAX_BYTES));
            opts.cache = Some(cache);
        }
        Self {
            s3,
            prefix: prefix.into().trim_end_matches('/').to_string(),
            opts: Arc::new(opts),
        }
    }
}

impl<R, E> Config<R, E>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
{
    pub fn with_context<E2, C>(self, opts: C) -> Config<R, E2>
    where
        E2: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
        C: Into<Arc<ManifestContext<R, E2>>>,
    {
        Config {
            s3: self.s3,
            prefix: self.prefix,
            opts: opts.into(),
        }
    }

    /// Override the retry/backoff policy while keeping the rest of the context intact.
    pub fn with_backoff(mut self, backoff: BackoffPolicy) -> Self {
        Arc::make_mut(&mut self.opts).set_backoff(backoff);
        self
    }

    /// Full object key for the manifest head using the default naming.
    pub fn head_key(&self) -> String {
        join_path(&self.prefix, DEFAULT_HEAD_FILE)
    }
}

/// Builder that constructs `AmazonS3` internally for ergonomic initialization.
#[derive(Debug)]
pub struct Builder<R = DefaultRetention, E = BlockingExecutor>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
{
    bucket: String,
    prefix: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    credential: Option<AwsCredential>,
    sign_payload: bool,
    checksum: bool,
    opts: Arc<ManifestContext<R, E>>,
}

impl Builder<DefaultRetention, BlockingExecutor> {
    /// Create a new builder for a given S3 `bucket` and manifest `prefix`.
    /// `prefix` is trimmed of any trailing slash.
    pub fn new(bucket: impl Into<String>) -> Self {
        #[cfg_attr(not(feature = "cache-moka"), allow(unused_mut))]
        let mut opts = ManifestContext::default();
        #[cfg(feature = "cache-moka")]
        {
            let cache: Arc<dyn BlobCache> = Arc::new(MemoryBlobCache::new(DEFAULT_CACHE_MAX_BYTES));
            opts.cache = Some(cache);
        }
        Self {
            bucket: bucket.into(),
            prefix: None,
            region: None,
            endpoint: None,
            credential: None,
            sign_payload: false,
            checksum: false,
            opts: Arc::new(opts),
        }
    }
}

impl<R, E> Clone for Config<R, E>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            s3: self.s3.clone(),
            prefix: self.prefix.clone(),
            opts: self.opts.clone(),
        }
    }
}

impl<R, E> Builder<R, E>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
{
    /// Optional manifest prefix. Defaults to root (no prefix).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into().trim_end_matches('/').to_string());
        self
    }

    /// Override the AWS region. When the bucket is an access point ARN the
    /// region is inferred automatically, so this is only needed to override
    /// that behaviour.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set a custom endpoint (e.g., MinIO/Localstack). When set, path-style addressing is used.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set static credentials.
    pub fn credential(mut self, credential: AwsCredential) -> Self {
        self.credential = Some(credential);
        self
    }

    /// Enable SigV4 payload signing (default: false). Recommended for AWS/MinIO.
    pub fn sign_payload(mut self, yes: bool) -> Self {
        self.sign_payload = yes;
        self
    }

    /// Enable checksum when available (default: false).
    pub fn checksum(mut self, yes: bool) -> Self {
        self.checksum = yes;
        self
    }

    /// Override just the backoff policy in the embedded context.
    pub fn backoff(mut self, backoff: BackoffPolicy) -> Self {
        Arc::make_mut(&mut self.opts).set_backoff(backoff);
        self
    }

    /// Replace the manifest context (retention, runtime, etc.). Consumes the builder and returns
    /// one typed for the new runtime and retention policy.
    pub fn with_context<R2, E2, C>(self, opts: C) -> Builder<R2, E2>
    where
        R2: RetentionPolicy + Clone,
        E2: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
        C: Into<Arc<ManifestContext<R2, E2>>>,
    {
        Builder {
            bucket: self.bucket,
            prefix: self.prefix,
            region: self.region,
            endpoint: self.endpoint,
            credential: self.credential,
            sign_payload: self.sign_payload,
            checksum: self.checksum,
            opts: opts.into(),
        }
    }

    /// Build a `Config` by constructing `AmazonS3` internally.
    pub fn build(self) -> Config<R, E> {
        let mut b = AmazonS3Builder::new(self.bucket.clone());
        if let Some(cred) = self.credential {
            b = b.credential(cred);
        }
        if let Some(ep) = self.endpoint {
            b = b.endpoint(ep);
        }
        if let Some(region) = self.region {
            b = b.region(region);
        }
        let s3 = b
            .sign_payload(self.sign_payload)
            .checksum(self.checksum)
            .build();

        let prefix = self.prefix.unwrap_or_default();
        Config {
            s3,
            prefix,
            opts: self.opts,
        }
    }
}

impl<R, E> Clone for Builder<R, E>
where
    R: RetentionPolicy + Clone,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
            credential: self.credential.clone(),
            sign_payload: self.sign_payload,
            checksum: self.checksum,
            opts: self.opts.clone(),
        }
    }
}

fn join_path(prefix: &str, child: &str) -> String {
    if prefix.is_empty() {
        child.to_string()
    } else {
        format!("{}/{}", prefix, child)
    }
}

/// Public wrapper over a Manifest with S3 stores to avoid leaking store types.
type HeadStore = HeadStoreImpl<AmazonS3>;
type SegmentStore = SegmentStoreImpl<AmazonS3>;
type CheckpointStore = CheckpointStoreImpl<AmazonS3>;
type LeaseStore<T> = LeaseStoreImpl<AmazonS3, T>;

pub struct S3Manifest<K, V, E = BlockingExecutor, R = DefaultRetention>
where
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
    R: RetentionPolicy + Clone,
{
    inner: Manifest<K, V, HeadStore, SegmentStore, CheckpointStore, LeaseStore<E>, E, R>,
    cfg: Config<R, E>,
}

impl<K, V, E, R> S3Manifest<K, V, E, R>
where
    K: PartialOrd + Eq + Hash + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: serde::Serialize + for<'de> serde::Deserialize<'de>,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
    R: RetentionPolicy + Clone,
{
    pub async fn session_write(
        &self,
    ) -> Result<WriteSession<K, V, HeadStore, SegmentStore, CheckpointStore, LeaseStore<E>, E, R>>
    {
        self.inner.session_write().await
    }

    pub async fn session_read(
        &self,
    ) -> Result<ReadSession<K, V, HeadStore, SegmentStore, CheckpointStore, LeaseStore<E>, E, R>>
    {
        self.inner.session_read().await
    }

    pub async fn session_at(
        &self,
        snapshot: Snapshot,
    ) -> Result<ReadSession<K, V, HeadStore, SegmentStore, CheckpointStore, LeaseStore<E>, E, R>>
    {
        self.inner.session_at(snapshot).await
    }

    pub async fn snapshot(&self) -> Result<Snapshot> {
        self.inner.snapshot().await
    }

    pub async fn get_latest(&self, key: &K) -> Result<Option<V>> {
        self.inner.get_latest(key).await
    }

    pub async fn scan_latest(
        &self,
        range: Option<crate::snapshot::ScanRange<K>>,
    ) -> Result<Vec<(K, V)>> {
        self.inner.scan_latest(range).await
    }

    /// Construct an `S3Compactor` over the same configuration as this manifest.
    pub fn compactor(&self) -> S3Compactor<K, V, E, R> {
        self.cfg.clone().into()
    }
}

/// Wrapper that hides the GC plan store and exposes ergonomic GC methods.
pub struct S3Compactor<K, V, E = BlockingExecutor, R = DefaultRetention>
where
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
    R: RetentionPolicy + Clone,
{
    inner: Compactor<K, V, HeadStore, SegmentStore, CheckpointStore, LeaseStore<E>, E, R>,
    plan: FsGcPlanStore<AmazonS3, E>,
}

impl<K, V, E, R> S3Compactor<K, V, E, R>
where
    K: PartialOrd + Eq + Hash + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: serde::Serialize + for<'de> serde::Deserialize<'de>,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
    R: RetentionPolicy + Clone,
{
    pub async fn compact_once(
        &self,
    ) -> Result<(crate::checkpoint::CheckpointId, crate::head::HeadTag)> {
        self.inner.compact_once().await
    }

    pub async fn compact_and_gc(
        &self,
    ) -> Result<(crate::checkpoint::CheckpointId, crate::head::HeadTag)> {
        self.inner.compact_and_gc().await
    }

    pub async fn run_once(&self) -> Result<()> {
        self.inner.run_once().await
    }

    pub async fn gc_compute(&self) -> Result<Option<crate::gc::GcTag>> {
        self.inner.gc_compute(&self.plan).await
    }

    pub async fn gc_apply(&self) -> Result<()> {
        self.inner.gc_apply(&self.plan).await
    }

    pub async fn gc_delete_and_reset(&self) -> Result<()> {
        self.inner.gc_delete_and_reset(&self.plan).await
    }
}

/// Open an S3 compactor over the given config (consumes `Config`).
fn compactor<K, V, E, R>(cfg: Config<R, E>) -> S3Compactor<K, V, E, R>
where
    K: PartialOrd + Eq + Hash + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: serde::Serialize + for<'de> serde::Deserialize<'de>,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
    R: RetentionPolicy + Clone,
{
    let head = HeadStoreImpl::new(cfg.s3.clone(), cfg.head_key());
    let segs = SegmentStoreImpl::new(cfg.s3.clone(), join_path(&cfg.prefix, "segments"));
    let ckpt = CheckpointStoreImpl::new(cfg.s3.clone(), cfg.prefix.clone());
    let leases = LeaseStoreImpl::new(
        cfg.s3.clone(),
        cfg.prefix.clone(),
        cfg.opts.backoff,
        cfg.opts.timer().clone(),
    );
    let opts = cfg.opts.clone();
    let inner =
        Compactor::<K, V, _, _, _, _, E, R>::new(head, segs, ckpt, leases, Arc::clone(&opts));
    let plan = FsGcPlanStore::new(
        cfg.s3.clone(),
        cfg.prefix.clone(),
        opts.backoff,
        opts.timer().clone(),
    );
    S3Compactor { inner, plan }
}

impl<K, V, E, R> From<Config<R, E>> for S3Manifest<K, V, E, R>
where
    K: PartialOrd + Eq + Hash + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: serde::Serialize + for<'de> serde::Deserialize<'de>,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
    R: RetentionPolicy + Clone,
{
    fn from(cfg: Config<R, E>) -> Self {
        let head = HeadStoreImpl::new(cfg.s3.clone(), cfg.head_key());
        let segs = SegmentStoreImpl::new(cfg.s3.clone(), join_path(&cfg.prefix, "segments"));
        let ckpt = CheckpointStoreImpl::new(cfg.s3.clone(), cfg.prefix.clone());
        let leases = LeaseStoreImpl::new(
            cfg.s3.clone(),
            cfg.prefix.clone(),
            cfg.opts.backoff,
            cfg.opts.timer().clone(),
        );
        let inner = Manifest::new_with_context(head, segs, ckpt, leases, cfg.opts.clone());
        S3Manifest { inner, cfg }
    }
}

impl<K, V, E, R> From<Config<R, E>> for S3Compactor<K, V, E, R>
where
    K: PartialOrd + Eq + Hash + serde::Serialize + for<'de> serde::Deserialize<'de>,
    V: serde::Serialize + for<'de> serde::Deserialize<'de>,
    E: Executor + Timer + Clone + MaybeSend + MaybeSync + 'static,
    R: RetentionPolicy + Clone,
{
    fn from(cfg: Config<R, E>) -> Self {
        compactor::<K, V, E, R>(cfg)
    }
}

# Fusio Manifest Examples

These examples live inside the `fusio-manifest` crate so they build directly
against the current API. Each binary expects AWS-compatible credentials (AWS,
LocalStack, or MinIO) and a bucket configured via the following environment
variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` (defaults to `us-east-1`)
- `AWS_SESSION_TOKEN` (optional)
- `FUSIO_MANIFEST_BUCKET`

Every run writes to a unique prefix to avoid collisions. Execute them with
`cargo run -p fusio-manifest --example <name>`:

1. `step1_basic`: open an S3 manifest, append a couple of keys, and read
   them back through snapshots and a pinned read lease.
2. `step2_leases`: demonstrates how pinned leases protect segments
   during `compact_and_gc`, then reruns compaction after the lease is released.
3. `step3_remote_compactor`: overrides the exponential backoff policy and demonstrates
   running compaction + GC from a separate process built via the shared S3
   config/GC plan store.
4. `step4_lease_keeper`: starts a background lease keeper task that maintains
   a pinned read lease via the configured executor.

## Sharing the blob cache across manifests

`s3::Builder` seeds each manifest with an in-memory cache that is local to the
instance. If you intentionally share an `Arc<dyn BlobCache>` between multiple
manifests (for example, when constructing contexts manually), set a namespace on
the `ManifestContext` so cached entries stay scoped to a single bucket/prefix:

```rust
use std::sync::Arc;
use fusio_manifest::{cache::{BlobCache, MemoryBlobCache}, ManifestContext};

let shared_cache: Arc<dyn BlobCache> = Arc::new(MemoryBlobCache::new(256 * 1024 * 1024));

let mut ctx = ManifestContext::default();
ctx.set_cache(Some(shared_cache.clone()));
ctx.set_cache_namespace(Some("s3://my-bucket/prefix-a"));
```

Manifests that should see the same cached blobs must reuse the exact namespace
string; manifests on other prefixes should pick a different namespace (or leave
it unset, which keeps the default per-manifest isolation).

Example:

```bash
cargo run -p fusio-manifest --example step1_basic
```

> **Feature flag:** `MemoryBlobCache` ships behind the `cache-moka` feature, which is
> enabled by default. Disable it (e.g. `cargo run -p fusio-manifest --no-default-features --features std,examples`)
> if you want to supply a different `BlobCache` implementation.

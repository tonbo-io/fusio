# fusio-manifest

Backend-agnostic, append-only manifest store providing serializable snapshot isolation for key-value metadata.

## Architecture

```
Manifest<K,V> → ReadSession / WriteSession
                         ↓
    ┌──────────┬──────────────┬─────────────────┬─────────────┐
    │ HeadStore│ SegmentStore │ CheckpointStore │ LeaseStore  │
    │ (CAS)    │ (immutable)  │ (merged KV)     │ (TTL-based) │
    └──────────┴──────────────┴─────────────────┴─────────────┘
                         ↓
              ManifestContext (backoff, retention, executor, cache)
```

## Key Components

- **`Manifest<K,V>`**: Main facade for sessions and one-shot operations
- **`ReadSession`/`WriteSession`**: Lease-protected snapshot views with staged operations
- **`HeadStore`**: CAS-based HEAD pointer (`HeadStoreImpl` uses `FsCas`)
- **`SegmentIo`**: Immutable segment storage with `put_next`, `get`, `list_from`
- **`CheckpointStore`**: Merged key-value snapshots for read optimization
- **`LeaseStore`**: TTL-based leases protecting snapshots from GC
- **`Compactor`**: Headless compaction/GC with three-phase distributed GC

## Build/Test Commands

```bash
# Build (requires tokio runtime via fusio dependency)
cargo build -p fusio-manifest

# Test
cargo test -p fusio-manifest

# Build with moka cache
cargo build -p fusio-manifest --features cache-moka
```

## Usage Pattern

```rust
// Create manifest with S3 backend
let manifest: S3Manifest<String, String, _> = s3::Builder::new("bucket")
    .prefix("my-manifest")
    .credential(cred)
    .build()
    .into();

// Write session
let mut write = manifest.session_write().await?;
write.put("key".into(), "value".into());
write.commit().await?;

// Read session (lease-protected)
let reader = manifest.session_read().await?;
let value = reader.get(&"key".to_string()).await?;
reader.end().await?;
```

## Concurrency Model

- CAS-based HEAD updates ensure serializable commits
- Sessions acquire leases; GC respects watermarks from active leases
- Orphan recovery handles durable-but-unpublished segments
- Loom-based deterministic tests verify concurrency correctness

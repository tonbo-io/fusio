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

Example:

```bash
cargo run -p fusio-manifest --example step1_basic
```

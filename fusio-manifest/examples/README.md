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

1. `manifest_step1_basic`: open an S3 manifest, append a couple of keys, and read
   them back through snapshots and a pinned read lease.
2. `manifest_step2_leases`: demonstrates how pinned leases protect segments
   during `compact_and_gc`, then reruns compaction after the lease is released.
3. `manifest_step3_s3`: overrides the exponential backoff policy and constructs
   both a writer manifest and a matching compactor via the S3 builder.
4. `manifest_lease_keeper`: starts a background lease keeper task that maintains
   a pinned read lease via the configured executor.

Example:

```bash
cargo run -p fusio-manifest --example manifest_step1_basic
```

# Fusio Examples

This crate collects runnable snippets that demonstrate how to glue different
Fusio components together. The `manifest_step*` binaries walk through the
`fusio-manifest` workflow using the S3 backend. Point them at AWS, LocalStack,
or MinIO by configuring:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` (defaults to `us-east-1`)
- `AWS_SESSION_TOKEN` (optional)
- `FUSIO_MANIFEST_BUCKET`

Each run generates a unique prefix under the bucket so prefixes do not collide.

1. `manifest_step1_basic`: open an S3 manifest, append a couple of keys, and read
   them back via snapshots and a pinned read lease.
2. `manifest_step2_leases`: demonstrates how pinned leases protect segments
   during `compact_and_gc`, then releases the lease and reruns compaction. It
   finishes with the headless compactor façade.
3. `manifest_step3_s3`: customizes the exponential backoff policy and builds both
   a writer manifest and a matching compactor from the S3 builder.

Run any step with `cargo run -p examples --bin <name>`. Use `--features monoio`
if you want to experiment with the alternative runtime.

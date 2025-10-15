# fusio-manifest — Backend-Agnostic Manifest with Serializable KV and Compaction

Status: Draft (Unified RFD — Supersedes RFC-0001 through RFC-0006)

Date: 2025-09-11

Authors: fusio maintainers

## Summary

`fusio-manifest` is a backend-agnostic manifest/WAL crate that runs equally over in-memory and object-store (S3) backends. It provides a single visibility barrier via conditional publish of a small HEAD object (CAS), strict serializable transactions for a simple MVCC key–value API, and a checkpoint/compaction path to bound recovery time. This unified RFD consolidates and replaces RFC-0001 through RFC-0006 (manifest/WAL, HEAD CAS, backend families, serializable KV, and wal3-aligned recovery/GC).

Key properties:
- Append-only segments with monotonic sequence numbers (`seq`) and a single transaction identifier (`txn_id`) assigned per committed session.
- A single publish barrier (HEAD CAS) that defines global order and acknowledgement semantics.
- Snapshot-based reads for serializability; transactions commit by CAS’ing HEAD.
- Periodic compaction into durable checkpoints; GC of obsolete segments/checkpoints coordinated via a CAS-protected plan and gated by leases.
- Orphan recovery: adopt contiguous durable-but-unpublished segments on reopen, advancing HEAD via CAS.
- Portable backends: in-memory for tests, S3 for production; no local FS backend in core.
- Runtime integration: bounded retries use a `fusio::executor::Timer`; callers can provide runtime-specific executors (implementing both `Executor` and `Timer`) via `ManifestContext::with_executor`.
- Optional blob cache: callers can attach an `Arc<dyn BlobCache>` to `ManifestContext` so checkpoint/segment payloads are memoised when backends expose revision tags (ETags).

## Goals

- One portable manifest/WAL abstraction with the same semantics across backends.
- Clear durability/ack semantics mapped to each backend’s barrier (fs sync or S3 MPU commit).
- Serializable MVCC KV surface with a session-oriented API (`session_write/session_read`, staged `put/delete`, snapshot `get/scan`).
- Efficient recovery via checkpoints and bounded replay.
- Safe, auditable GC tied to active reader/writer leases.

## Non-Goals

- Distributed consensus or cross-shard transactions.
- Background threads/tasks inside the library; the host decides scheduling.
- Local filesystem backend in the core crate (use S3 or in-memory; FS may live in a contrib crate).

## Architecture Overview

Components (traits):
- `HeadStore`: loads and conditionally publishes a small JSON HEAD object using CAS.
- `SegmentIo`: writes immutable segment objects (`put_next(seq, txn_id, payload)`), lists by `seq`, reads.
- `CheckpointStore`: writes/reads immutable checkpoints capturing folded KV state at a transaction id.
- `LeaseStore`: issues short-lived leases to track active snapshots for safe GC cutoffs.
- `GcPlanStore`: CAS-protected GC plan for multi-actor coordination.

Data flow:
1) Writers stage KV operations and encode a segment (records carry `key`, `op`, `value?`). The segment header stores the session’s transaction id (`txn_id`).
2) Segment is durably written (e.g., S3 MPU commit). Writer then CAS-publishes a new HEAD that advances `last_txn_id` and `last_segment_seq`. Successful CAS defines the serial order.
3) Readers snapshot (`{tag, txn_id, last_segment_seq, checkpoint?}`), then serve `get/scan` by scanning segments newer than the checkpoint and, if needed, the checkpoint payload.
4) A compactor folds state up to a snapshot, writes a checkpoint, and CAS-publishes its id in HEAD.
5) GC uses a CAS-protected plan: (phase 1) compute from HEAD + leases; (phase 2) apply by ensuring HEAD references a sufficient checkpoint; (phase 3) delete segments/checkpoints after `not_before` and reset the plan.
6) Recovery on open: adopt contiguous orphan segments immediately after current HEAD (or from seq 1 when no HEAD), validating monotonic txn_id advance and advancing HEAD via CAS.

Storage layout (prefix owned by the embedding application):
- `segments/seg-<seq>.{json|bin}` — immutable segment objects (backend-defined content type) storing `{ txn_id, records }`.
- `checkpoints/ckpt-<txn>.meta.json` + payload `.json|.bin` — immutable checkpoint payload and metadata keyed by the checkpoint’s transaction id.
- `HEAD.json` — JSON `{ version, checkpoint_id: Option<CheckpointId>, last_segment_seq: Option<u64>, last_txn_id: u64 }`.
- `gc/GARBAGE` — CAS-protected GC plan document.

## Durability and Acknowledgement

- Writers acknowledge only after HEAD CAS succeeds (ManifestPublished semantics). Segment durability is currently fixed to S3's commit barrier; callers configure lower-level policies through Fusio itself rather than a manifest-specific knob.
- Backend mapping (from backend families):
  - Object store (S3): only `DurabilityLevel::Commit` is supported; a conditional PUT is used for visibility, and lower levels return `Unimplemented`.
  - In-memory (tests): durability is immediate; CAS simulated.

## Serializable MVCC KV

Types:
- `Snapshot { head_tag, txn_id, last_segment_seq, checkpoint_seq?, checkpoint_id? }`.
- `ReadOptions { as_of?, start?, end? }`.

Rules:
- `Manifest::session_write()` captures a `Snapshot` and registers a short-lived lease for the next transaction id (`snapshot.txn_id + 1`) so orphan recovery defers to the in-flight writer.
- `Manifest::session_read()` pins the latest HEAD under a lease, ensuring GC honours active readers without manual snapshot plumbing.
- `session_at(snapshot)` recreates a lease when resuming from a persisted snapshot so GC honors the in-use view (unpinned sessions were dropped to avoid GC surprises).
- `ReadSession::start_lease_keeper()` / `WriteSession::start_lease_keeper()` use the configured executor to spawn a background heartbeat loop; callers receive a `LeaseKeeper` handle that can be shutdown explicitly when the snapshot is no longer needed. See `examples/step4_lease_keeper.rs` for a minimal S3-backed walkthrough.
- `ReadSession` and `WriteSession` are both `#[must_use]`; dropping without calling `commit().await` or `end().await` triggers a debug assertion to catch leaked leases during development.
- `commit()` encodes and writes a segment tagged with the next monotonic `txn_id`, then CAS-publishes HEAD; conflicts return `PreconditionFailed`. The result is `Result<()>` — all sequencing metadata remains internal to the manifest.
- `get()` fast-path scans segments from newest to oldest within the snapshot bounds, then falls back
  to the checkpoint payload if present; `scan()` folds forward within bounds.

Checkpoint-aware reads:
- When a checkpoint is published in HEAD, reads only scan segments with `seq > checkpoint_seq`, then consult the checkpoint payload on misses.

## Compaction and GC

Compaction (`Compactor::compact_once`):
1) Capture a consistent `Snapshot`.
2) Fold segments up to the snapshot txn_id into a `BTreeMap<K,V>`.
3) Write a checkpoint payload with meta `{ txn_id, key_count, byte_size, created_at_ms, format, last_segment_seq_at_ckpt }`.
4) CAS-publish the checkpoint id in HEAD.

GC (`Compactor::compact_and_gc`):
- Compute the active lease watermark (min `snapshot_txn_id` among live leases). If watermark > `checkpoint.txn_id`, delete segments `<= last_segment_seq_at_ckpt` (best-effort, idempotent).
- Checkpoint retention: keep newest, keep the floor `<= watermark`, keep the last `N` newest, and honor a minimum TTL; delete others.

GC plan (multi-actor):
- Plan: `{ against_head_tag, not_before, delete_segments, delete_checkpoints, make_checkpoints }` where `delete_segments` is a list of inclusive ranges `{ start, end }` and `not_before` is serialized as milliseconds.
- Phase 1 (compute): derive delete sets; CAS-install plan if none present.
- Phase 2 (apply): ensure HEAD references a checkpoint whose `last_segment_seq_at_ckpt` is ≥ the maximum range `end` in `delete_segments`; else CAS-reset the plan to empty to force recompute.
- Phase 3 (delete+reset): after `not_before`, best-effort deletes and CAS-reset the plan to empty.

## Backends

- In-memory: `FsHeadStore<InMemoryFs>`, `FsSegmentStore<InMemoryFs>`, `FsCheckpointStore<InMemoryFs>`, `FsLeaseStore<InMemoryFs>` (tests, local).
- S3: Provided via `s3::{Config, Builder, S3Manifest, S3Compactor}` over Fusio's `Fs`/`FsCas` traits—segments and checkpoints stream through the generic FS handle while HEAD/lease/GC metadata rely on `put_conditional` + `load_with_tag` for CAS semantics.

## Runtime Integration

- `fusio-manifest` remains runtime-agnostic; callers own scheduling and can run on block-on executors or native async runtimes.
- `ManifestContext` stores the executor/timer by value (`E: Executor + Timer + Clone`); applications can call `ctx.timer().clone()` when they need a concrete timer handle (e.g., backoff loops) and swap in runtime-specific executors via `with_executor`.
- The same `ManifestContext` holds an optional blob cache (`ctx.cache`). When unset the manifest reaches through to the underlying stores on every read; when set (e.g., `Some(Arc::new(MemoryBlobCache::new(...)))`) manifests reuse checkpoint/segment blobs keyed by `(object key, ETag)`. `s3::Builder::new` seeds a 256&nbsp;MiB in-memory cache, but applications can override it before constructing a manifest.

### Customising the blob cache

```rust
use fusio_manifest::{s3, ManifestContext, MemoryBlobCache};
use std::sync::Arc;

let mut ctx = ManifestContext::default();
ctx.set_cache(Some(Arc::new(MemoryBlobCache::new(64 * 1024 * 1024))));

let manifest: s3::S3Manifest<_, _> = s3::Builder::new("my-bucket")
    .with_context(ctx) // overrides the default 256 MiB cache
    .prefix("manifests")
    .build()
    .into();

// disable caching completely
let mut ctx = ManifestContext::default();
ctx.set_cache(None);
```

## Crate: fusio-manifest (API Surface)

Top-level type:
- `Manifest<K, V, HS, SS, CS, LS, E = BlockingExecutor, R = DefaultRetention>`
  - Constructors:
    - `Manifest::new(head: HS, seg: SS, ckpt: CS, leases: LS)`
    - `Manifest::new_with_context(..., ctx: Arc<ManifestContext<R, E>>)`
  - Methods:
    - `session_write() -> WriteSession<…>` — opens a pinned writer after orphan recovery and leases the next txn id.
    - `session_read() -> ReadSession<…>` — pins the latest snapshot under a lease.
    - `session_at(Snapshot) -> ReadSession<…>` — rehydrates a persisted snapshot and acquires a fresh lease.
    - `snapshot() -> Snapshot` — returns snapshot metadata without retaining a lease.
    - `get_latest(&K) -> Option<V>` / `scan_latest(Option<ScanRange<K>>) -> Vec<(K, V)>` — one-shot reads scoped to the latest HEAD.
    - `recover_orphans() -> usize` — adopts durable segments that were written but not yet published.
    - `compactor() -> Compactor<K, V, HS, SS, CS, LS, E, R>` — clones the shared store/options for orchestration without re-cloning each backend handle.

- `ReadSession<K, V, HS, SS, CS, LS, E, R>`
  - Methods: `get(&K)`, `scan()`, `scan_range(ScanRange<K>)`, `heartbeat()`, `start_lease_keeper()`, `end()`.

- `WriteSession<K, V, HS, SS, CS, LS, E, R>`
  - Methods: `put(K, V)`, `delete(K)`, `get(&K)`, `get_local(&K)`, `scan()`, `scan_local(Option<ScanRange<K>>)` plus `heartbeat()`, `start_lease_keeper()`, `commit()`, `end()`.

- `Compactor<K, V, HS, SS, CS, LS, E, R>`
  - Methods: `compact_once() -> (CheckpointId, HeadTag)`, `compact_and_gc() -> (CheckpointId, HeadTag)`, `run_once() -> Result<()>`, `gc_compute(&impl GcPlanStore) -> Option<GcTag>`, `gc_apply(&impl GcPlanStore) -> Result<()>`, `gc_delete_and_reset(&impl GcPlanStore) -> Result<()>`.

- S3 convenience layer (`s3` module): `Config<R, E>` / `Builder<R, E>` construct `AmazonS3`; `S3Manifest<K, V, E, R>` wraps the generic manifest and retains the same session API plus `compactor() -> S3Compactor<K, V, E, R>`; `S3Compactor<K, V, E, R>` wires the GC plan store and forwards all compaction/GC calls.

Example (targeting S3 or LocalStack via the convenience wrapper):
```rust
use std::sync::Arc;

use fusio::executor::tokio::TokioExecutor;
use fusio_manifest::{
    context::ManifestContext,
    s3,
    types::Result,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Configure runtime + manifest context (retention, backoff, timers).
    let ctx = Arc::new(ManifestContext::new(TokioExecutor::default()));

    // Builder picks up AWS credentials from the environment; add `endpoint(...)`
    // for LocalStack/MinIO.
    let cfg = s3::Builder::new("my-bucket")
        .prefix("manifests/demo")
        .region("us-east-1")
        .sign_payload(true)
        .with_context(ctx.clone())
        .build();
    let manifest: s3::S3Manifest<String, String, TokioExecutor> = cfg.into();

    let mut write = manifest.session_write().await?;
    write.put("user:1".into(), "alice".into());
    write.put("user:2".into(), "bob".into());
    write.commit().await?;

    let snap = manifest.snapshot().await?;
    let mut reader = manifest.session_at(snap).await?;
    let v = reader.get(&"user:1".to_string()).await?;
    reader.end().await?;
    assert_eq!(v.as_deref(), Some("alice"));

    let latest = manifest.get_latest(&"user:2".to_string()).await?;
    assert_eq!(latest.as_deref(), Some("bob"));
    Ok(())
}
```

`S3Manifest::compactor()` returns an `S3Compactor` that bundles the GC plan store; long-running jobs can call `compact_and_gc()` without wiring stores manually.

Feature flags:
- Default: `std`, `mem`.
- S3 / Fusio FS backends are always available; additional runtime-specific knobs now live in `fusio` (e.g., choose `tokio-http` vs. `wasm-http`).

## Observability

Recommended metrics: commit and publish latencies, CAS conflicts, bytes/records per segment, recovery time, compaction time, GC counts. Optional integrity: segment-level CRC and/or a global setsum for audits (follow-up).

## Security

For S3, use role-based credentials; consider SSE-S3/KMS. Configure lifecycle rules (e.g., abort incomplete MPUs) and retention policies consistent with GC.

## Testing and Rollout

- Unit tests run over in-memory backends by default (fast, deterministic).
- Optional S3 integration tests can run against LocalStack/MinIO; provide AWS-compatible credentials before invoking the ignored tests.
- Rollout focus: mem + S3; local FS backend is intentionally not part of the core crate.

## Compatibility and Migration Notes

- This unified RFD replaces and consolidates RFC-0001 through RFC-0006.
- Where they differ, this RFD follows the current `fusio-manifest` implementation:
  - HEAD JSON now uses `checkpoint_id: Option<CheckpointId>` to record the canonical checkpoint.
- Segment payloads are JSON-framed KV records; a v2 binary framing with CRC is a follow-up.
- Compaction preserves append order (txn/log order) rather than imposing a persistent key sort; introducing key ordering would require an LSM-style index outside this RFD's scope.
  - Leases and the CAS-protected GC plan are part of the public API.
  - Sharded segment keys are deferred; current focus is flat layout plus CAS durability.

## Known Gaps / Blockers (as of 2025-10-07)

- **Durability policy is fixed to commit:** manifest layers always request the strongest S3 barrier; offering additional options is deferred until we have meaningful backend distinctions to expose.
- **Snapshot/GC scaling gaps:** `Manifest::snapshot` re-downloads whole checkpoint payloads on every call, and GC plans materialise `Vec<u64>` of every segment slated for deletion—both will choke on large manifests during chaos exercises.
- **Observability & validation gaps:** Tracking checklist items for lease heartbeat docs, GC metrics/batch deletes, and a real-S3 orphan adoption test remain open, leaving no visibility when chaos scenarios fail.

## Future Work

- Framing v2: MAGIC/VER/LEN/CRC for batches; torn-tail handling and faster validation.
- Backoff examples (`commit_with_backoff`) and richer observability counters/spans for retries.
- Global setsum and optional scrub tool; content-addressable checkpoints.
- Integrity guard on GC plans (record setsum of segments/snapshots we intend to drop and verify before applying).
- Rate-limit CAS-heavy metadata paths (leases, gc plan) with per-process semaphores so S3 412 storms are less likely.
- Cursor store (independent of HEAD) for downstream readers; include in GC cutoffs.
- Sharded segment keys were removed from the initial milestone to simplify correctness validation; consider reintroducing them only with clear perf data and upgrade guidance.

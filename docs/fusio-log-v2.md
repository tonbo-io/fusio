# fusio-log v2 Design: Log Is The Database

## Goals
- Crash-safe, append-only log with early verification and safe recovery.
- Explicit durability controls across local FS, S3, and OPFS/WASM.
- Portable across runtimes/backends via `fusio` traits, with minimal overhead.

## Scope
- Breaking change: new on-disk format (v2). v1 compatibility is optional via a separate converter.
- Non-goals: full KV store, secondary indexes by default, multi-writer concurrency.

## On-Disk Format v2
- File header: `MAGIC(8) | VERSION(2) | FLAGS(2) | CHECKSUM_ALGO(1) | COMPRESSION(1)`.
- Physical blocks: fixed 32 KiB with zero padding.
- Record framing (RocksDB-style): `[CRC32C u32][LEN u16][TYPE u8][PAYLOAD]`, types: `FULL/FIRST/MIDDLE/LAST`.
- Batch payload (inside PAYLOAD): `payload_len u32 | record_count u32 | records…` (all lengths use `u32`).
- Checksum: standardize on CRC32C; CRC covers `TYPE + PAYLOAD` (uncompressed bytes if compression disabled, otherwise compressed bytes).
- Optional compression: per-batch `lz4`/`zstd` via `Options`.

## Writer Semantics
- Durability: `Options::durability = { Flush | Fdatasync | Fsync }`.
- Pacing: `Options::bytes_per_sync` (best-effort periodic sync), `fsync_on_every_batch` toggle.
- Segments: roll by size/time; file pattern `basename.000001.log`, monotonically increasing.
- Local FS: ensure `close()` performs a final sync if configured.
- S3: one segment per object; write multipart to a temp key and publish via manifest (no rename). `flush()` only buffers; `close()` completes MPU and publishes.
- OPFS/WASM: use available sync APIs; document durability caveats.

## Reader & Recovery
- Early verification: validate CRC before any decode; remove unchecked UTF-8.
- Torn-write tolerance: skip trailing zeros; allow partial fragments at EOF; stop at last valid record.
- Policy: on corruption or partial batch, stop at last valid boundary; `Options::truncate_on_recover` to shrink file to the last good offset (local FS only).
- Strict versioning: reject unknown versions; checksum/compression negotiated via header.

## Lifecycle & Metadata
- Manifest log: append-only metadata log listing committed segments with `(seq, name, size, checksum/etag, time)`.
- Pointer: `basename.CURRENT` points to active manifest for fast discovery.
- Retention: size/time/segment-count policies; local segment recycling.

## Index & Snapshots (Optional)
- Sparse index: every N records/bytes, write `(logical_offset -> segment, block, rec)` to `basename.index`.
- Snapshots: helper API to persist a checkpoint; optional compaction utility to drop pre-snapshot segments.

## Public API Changes
- Appends: `append(&T) -> Result<Offset, LogError>`, `append_batch<I: IntoIterator<Item=&T>>() -> Result<Range<Offset>, LogError>`.
- Offsets: logical per-record offsets; `recover_from(offset)` and streaming variants.
- Options: `compression`, `checksum_algo`, `segment_bytes`, `roll_strategy`, `bytes_per_sync`, `durability`, `truncate_on_recover`.
- Errors: propagate all encode/IO errors (no `unwrap` in write path).

## Concurrency & Safety
- Single-writer lock (lockfile or OS file lock). Multi-reader safe across segments.
- Document process semantics; no multi-writer support in v2.

## Observability & Testing
- Metrics: bytes written, fsyncs, segment rolls, recover errors, S3 MPU parts.
- Tracing: spans for append/recover/roll/publish.
- Tests: crash injection around block boundaries, fuzz framed parser, property tests for recovery invariants, WASM/S3 integration, perf benches.

## Phased Delivery
- Phase 1: v2 framing (blocks/header/CRC32C), safe decode, durability knobs, local segments.
- Phase 2: recovery policies, truncate-on-recover, rolling/retention; optional sparse index.
- Phase 3: S3 segments + manifest publish, OPFS durability notes, metrics/tracing.
- Phase 4: compression options, snapshot helpers, performance tuning and docs.

## Migration
- v1 reading is not guaranteed. Provide a separate one-off converter tool if needed.
- New files always write v2; `VERSION` in header enables future evolution.

# Repository Guidelines

## Project Structure & Module Organization
- Rust workspace managed by Cargo. Core crates live at `fusio/`, `fusio-core/`, `fusio-parquet/`, `fusio-log/`, `fusio-opendal/`, `fusio-dispatch/`, and `fusio-object-store/`.
- Examples in `examples/` (see `examples/src/*.rs`) and a WASM demo in `examples/opfs/`.
- Benchmarks in `benches/` (e.g., `benches/tokio.rs`, `benches/monoio.rs`).
- Integration tests in crate-level `tests/` (e.g., `fusio/tests/`, `fusio-parquet/tests/`).
- CI workflows in `.github/workflows/`.

## Build, Test, and Development Commands
- Build workspace: `cargo build --workspace`.
- Build a crate with features (examples):
  - `cargo build -p fusio --features tokio,aws,tokio-http`
  - `cargo build -p fusio --features monoio`
  - `cargo build -p fusio --features tokio-uring` (Linux)
- Test patterns:
  - `cargo test -p fusio --features tokio,aws,tokio-http`
  - `cargo test -p fusio-parquet --features tokio`
  - `cargo test -p fusio-log --no-default-features --features aws,bytes,monoio,monoio-http`
- Lint/format: `cargo fmt --all` and `cargo clippy --workspace --all-features -D warnings`.
- WASM tests (optional): `wasm-pack test --chrome --headless fusio --features aws,opfs,wasm-http` and `wasm-pack test --chrome --headless fusio-parquet --features web`.

## Coding Style & Naming Conventions
- rustfmt (2021 edition) with max width 100, grouped imports; run `cargo fmt --all` before committing.
- Prefer idiomatic Rust naming: modules/files `snake_case`, types/traits `CamelCase`, functions `snake_case`, constants `SCREAMING_SNAKE_CASE`.
- Keep clippy clean; use `allow` sparingly and justify in code when unavoidable.

## Testing Guidelines
- Use unit tests alongside code and integration tests under `tests/`.
- When adding backends or features, test across runtimes: e.g., `--features tokio` and `--features monoio`. For S3-related code, include an `aws` feature test run.
- Add doc tests where helpful for public APIs.
- WASM: keep tests headless-browser friendly; gate with feature flags like `opfs`/`web`.
- For fusio-manifest: we support in-memory and S3 backends only. For local end-to-end S3 tests, use LocalStack or MinIO. Example with LocalStack:
  - `docker run -p 4566:4566 -e SERVICES=s3 localstack/localstack`
  - export AWS credentials/region, then run `cargo test -p fusio-manifest --features aws-tokio`.

## Commit & Pull Request Guidelines
- Follow Conventional Commits (observed in history): `feat:`, `fix:`, `docs:`, `chore:`, `refactor:` with optional scope, e.g., `feat(parquet): ...` and reference PRs/issues `(#123)`.
- PRs: include a clear description, rationale, affected crates, feature flags tested, and test results. Update README/examples when behavior changes.
- Pre-push checklist: `cargo fmt --all`, `cargo clippy --workspace --all-features -D warnings`, and appropriate `cargo test` runs.

## Security & Configuration Tips
- Do not commit credentials. For S3 tests/examples, export `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` locally; avoid checking these into code.
- Prefer `--no-default-features` when you need a minimal surface and enable only the features you require.

## Tooling: ast-grep (AST-aware search)

We use ast-grep to quickly search Rust sources by tokens or simple AST templates. It’s available in the dev environment as `ast-grep`.

Basic usage
- Search the whole workspace for a token/pattern (Rust only):
  - `ast-grep run -l rust -p 'ManifestDb' -r .`
- JSON output for piping/inspection:
  - `ast-grep run -l rust -p 'KvDb' -r . --json=stream`
- Limit to a crate/path:
  - `ast-grep run -l rust -p 'Manifest' fusio-manifest/`
- Show surrounding context (like grep’s context):
  - `ast-grep run -l rust -p 'CheckpointStore' -r . -A 2 -B 2`

Common queries in this repo
- Find all uses of the new `Manifest` type:
  - `ast-grep run -l rust -p 'Manifest' -r .`
- Locate trait impl sites (by token):
  - `ast-grep run -l rust -p 'impl HeadStore' -r .`
  - `ast-grep run -l rust -p 'impl SegmentIo' -r .`
- Track functions quickly:
  - `ast-grep run -l rust -p 'compact_and_gc' -r .`
  - `ast-grep run -l rust -p 'compact_once' -r .`

Notes and tips
- ast-grep patterns are AST-based; for complex Rust syntax (generics, trait bounds), prefer token searches (e.g., `'Manifest<'` or `'impl TraitName'`) for reliability.
- The CLI does not support `-n` (line numbers) like grep; use `--json=stream` and pipe to tools for custom formatting if needed.
- Use `--globs` to include or exclude paths; example: `--globs '!target/**'` (ignored by default).
- If you see “Pattern contains an ERROR node,” simplify the pattern (use token search) or try the playground to refine it: https://ast-grep.github.io/playground.html

# IMPORTAT
We could break any backward compatibility, just make implementation as neat as possibe.

# Agents Handbook

## Workspace Map
- Rust workspace; primary crates live in `fusio*/` directories.
- Examples in `examples/`; WASM demo under `examples/opfs/`.
- Benchmarks in `benches/`; integration tests live in each crate's `tests/` folder.
- CI configuration sits in `.github/workflows/`.

## Build & Test
- Default build: `cargo build --workspace`.
- Feature builds: `cargo build -p fusio --features tokio,aws,tokio-http` | `monoio` | `tokio-uring` (Linux).
- Common test runs:
  - `cargo test -p fusio --features tokio,aws,tokio-http`
  - `cargo test -p fusio-parquet --features tokio`
  - `cargo test -p fusio-log --no-default-features --features aws,bytes,monoio,monoio-http`
- Cache variants: `cargo check -p fusio-manifest --no-default-features --features std` and `cargo test -p fusio-manifest --lib --no-default-features --features std`
- Optional WASM checks: `wasm-pack test --chrome --headless fusio[ -parquet ]` with appropriate features.

## Linting & Formatting
- Format before commits: `cargo +nightly fmt --all`
- **IMPORTANT:** `--workspace --all-features` does NOT work due to mutually exclusive runtime features (`tokio` vs `monoio`/`tokio-uring`/`opfs` which enable `no-send`).
- Run clippy per-package with appropriate feature combinations:
  ```bash
  cargo clippy -p fusio-core --all-features -- -D warnings
  cargo clippy -p fusio --features tokio,aws,tokio-http -- -D warnings
  cargo clippy -p fusio --features monoio,aws,monoio-http -- -D warnings
  cargo clippy -p fusio-manifest -- -D warnings
  cargo clippy -p fusio-parquet --features tokio -- -D warnings
  cargo clippy -p fusio-opendal --all-features -- -D warnings
  cargo clippy -p fusio-object-store --all-features -- -D warnings
  cargo clippy -p examples --features tokio -- -D warnings
  ```
- For CI or pre-commit, test the runtime you're actively developing against.

## Pre-commit Hook
- Enable tracked hooks once per clone: `git config core.hooksPath .githooks`.
- Hook runs the full local suite and requires the nightly toolchain (`rustup toolchain install nightly`): `cargo +nightly fmt --all -- --check`, `cargo check --workspace --all-targets`, `cargo check -p fusio-manifest --no-default-features --features std`, the clippy matrix (`fusio-core`, `fusio` with tokio + monoio features, `fusio-manifest` with default features *and* `--no-default-features --features std`, `fusio-parquet`, `fusio-opendal`, `fusio-object-store`), and `cargo test -p fusio --features tokio,aws,tokio-http`, `cargo test -p fusio-parquet --features tokio`, plus `cargo test -p fusio-manifest --lib --no-default-features --features std`.
- If rustfmt rewrites files, re-stage and re-commit after running `cargo fmt`; clippy is lint-only.
- Add a wasm sanity check: `cargo build --target wasm32-unknown-unknown -p fusio --no-default-features --features "aws,opfs,wasm-http"`; the hook will install `wasm32-unknown-unknown` via rustup if it's missing.

## Style Expectations
- Rust 2021 with rustfmt (max width 100); grouped imports.
- Idiomatic naming: modules/functions snake_case, types CamelCase, constants SCREAMING_SNAKE_CASE.
- Minimize `#[allow]`; justify any unavoidable allowances inline.

## Testing Discipline
- Pair unit tests with code; use crate `tests/` for integration coverage.
- Exercise new backends across runtimes (`tokio`, `monoio`) and include `aws` feature for S3 logic.
- Gate browser/WASM behavior behind `opfs`/`web` features.
- `fusio-manifest` supports in-memory + S3.

## Change Management
- Conventional commits (`feat:`, `fix:`, etc.) with optional scope and PR references.
- PRs should explain rationale, touched crates, feature flags tested, and test results. Update docs/examples when behavior shifts.
- Pre-push checklist: fmt, clippy, relevant `cargo test` invocations.

## Security & Config
- Never commit credentials; export AWS keys/region locally when needed.
- Prefer `--no-default-features` plus explicit flags for minimal surfaces.
- `AmazonS3Builder::new` understands access point ARNs and switches to virtual-host mode automatically.

## Tooling Quick Reference
- Use `ast-grep` for Rust-aware search (e.g. `ast-grep run -l rust -p 'Manifest' -r .`).
- Add `--json=stream` for scripted consumption or `-A/-B` for context.
- `--globs` helps include/exclude paths; simplify patterns if you see "Pattern contains an ERROR node".

## Final Note
- Backward compatibility is flexible—prioritize clean, well-explained implementations.

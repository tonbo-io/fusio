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

## Commit & Pull Request Guidelines
- Follow Conventional Commits (observed in history): `feat:`, `fix:`, `docs:`, `chore:`, `refactor:` with optional scope, e.g., `feat(parquet): ...` and reference PRs/issues `(#123)`.
- PRs: include a clear description, rationale, affected crates, feature flags tested, and test results. Update README/examples when behavior changes.
- Pre-push checklist: `cargo fmt --all`, `cargo clippy --workspace --all-features -D warnings`, and appropriate `cargo test` runs.

## Security & Configuration Tips
- Do not commit credentials. For S3 tests/examples, export `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` locally; avoid checking these into code.
- Prefer `--no-default-features` when you need a minimal surface and enable only the features you require.


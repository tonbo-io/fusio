# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fusio is a Rust library providing a unified, lean, and extensible I/O abstraction layer for file operations across multiple storage backends (local disk, S3, WASM) and async runtimes (tokio, monoio, tokio-uring). It's designed for embedded use in databases and other libraries requiring efficient, runtime-agnostic file I/O.

## Common Development Commands

### Build Commands

```bash
# Build specific package with runtime (choose one)
cargo build -p fusio --features tokio
cargo build -p fusio --features monoio        # Linux/macOS only
cargo build -p fusio --features tokio-uring   # Linux only

# Build with S3 support
cargo build -p fusio --features "tokio aws"

# Build for WebAssembly
cargo build -p fusio --target wasm32-unknown-unknown --features "opfs aws"
```

### Test Commands

```bash
# Test specific runtime
cargo test -p fusio --features tokio
cargo test -p fusio --features monoio        # Linux/macOS only
cargo test -p fusio --features tokio-uring   # Linux only

# Test with S3 (requires AWS credentials)
export BUCKET_NAME=fusio-test
export AWS_REGION=ap-southeast-1
cargo test -p fusio --features "tokio aws"

# Run WASM tests
wasm-pack test --chrome --headless fusio --features "aws opfs wasm-http"
```

### Development Commands

```bash
# Format code
cargo fmt

# Run linter
cargo clippy

# Run benchmarks
cargo bench

# Check specific package
cargo check -p fusio-parquet --features tokio
```

## High-Level Architecture

### Core Concepts

1. **Buffer Traits**: Uses `IoBuf`/`IoBufMut` instead of `bytes::Bytes` for zero-cost abstraction
2. **Ownership-based I/O**: Methods take buffer ownership, returning `(Result, Buffer)` tuples
3. **Runtime Agnostic**: Compile-time feature flags switch between runtimes with no overhead
4. **Path Abstraction**: Custom `Path` type works across local/remote filesystems

### Workspace Structure

- **fusio-core**: Core traits (`Read`, `Write`, `IoBuf`) and error types
- **fusio**: Main crate with filesystem implementations
- **fusio-dispatch**: Runtime dispatch helper
- **fusio-log**: Append-only log for WAL/metadata
- **fusio-parquet**: Parquet file support
- **fusio-object-store**: Integration with Apache Arrow's object_store
- **fusio-opendal**: OpenDAL integration

### Key Traits

- **`Read`**: Random access reads with `read_exact_at(buf, pos)`
- **`Write`**: Sequential writes with ownership-based API
- **`Fs`**: File system operations (open, create, list, remove)
- **Dynamic versions**: `DynRead`, `DynWrite`, `DynFs` for object-safe dispatch

### Implementation Locations

- **Local disk**: `fusio/src/impls/disk/` (tokio, monoio, tokio-uring variants)
- **S3**: `fusio/src/impls/remotes/aws/`
- **HTTP clients**: `fusio/src/impls/remotes/http/`
- **WASM/OPFS**: `fusio/src/impls/disk/opfs/`

### Adding New Features

1. **New storage backend**: Implement traits in `fusio/src/impls/`
2. **New runtime**: Add module in `fusio/src/impls/disk/`
3. **Tests**: Add integration tests showing usage patterns
4. **Features**: Update Cargo.toml with appropriate feature flags

### Important Patterns

- Use `cfg_if!` for platform/feature-specific code
- All async operations must be cancellation-safe
- Preserve buffer ownership semantics in all APIs
- Follow existing error handling patterns with `fusio::Error`
- Match code style of surrounding modules (no comments unless essential)
# Fusio

Fusio provides [Read](https://github.com/tonbo-io/fusio/blob/main/fusio/src/lib.rs#L81) and [Write](https://github.com/tonbo-io/fusio/blob/main/fusio/src/lib.rs#L63) traits to operate on multiple storage backends (e.g., local disk, Amazon S3) across various asynchronous runtimes—both poll-based ([tokio](https://github.com/tokio-rs/tokio)) and completion-based ([tokio-uring](https://github.com/tokio-rs/tokio-uring), [monoio](https://github.com/bytedance/monoio))—with:

- Lean: Binary size is at least 14× smaller than others.
- Minimal-cost abstraction: Compared to bare storage backends, trait definitions allow dispatching file operations without extra overhead.
- Extensible: Exposes traits to support implementing storage backends as third-party crates.

> **Fusio is now at preview version, please join our [community](https://discord.gg/j27XVFVmJM) to attend its development and semantic / behavior discussion.**

## Why do we need Fusio?

Since we started integrating object storage into [Tonbo](https://github.com/tonbo-io/tonbo), we realized the need for file and file system abstractions to dispatch read and write operations to multiple storage backends: memory, local disk, remote object storage, and so on. We found that existing solutions have the following limitations:
- Accessing local or remote file systems is not usable across various kinds of asynchronous runtimes (not only completion-based runtimes but also Python / JavaScript event loops).
- Most VFS implementations are designed for backend server scenarios. As an embedded database, Tonbo requires a lean implementation suitable for embedding, along with a set of traits that allow extending asynchronous file and file system approaches as third-party crates.

For more context, please check [apache/arrow-rs#6051](https://github.com/apache/arrow-rs/issues/6051).

## How to use it?

### Installation
```toml
fusio = { version = "*", features = ["tokio"] }
```

### Examples

#### [Runtime agnostic](https://github.com/tonbo-io/fusio/blob/main/examples/src/multi_runtime.rs)

`fusio` supports switching the async runtime at compile time. Middleware libraries can build runtime-agnostic implementations, allowing the top-level application to choose the runtime.

#### [Object safety](https://github.com/tonbo-io/fusio/blob/main/examples/src/object.rs)

`fusio` pprovides two sets of traits:
- `Read` / `Write` / `Seek` / `Fs` are not object-safe.
- `DynRead` / `DynWrite` / `DynSeek` / `DynFs` are object-safe.

You can freely transmute between them.

#### [File system traits](https://github.com/tonbo-io/fusio/blob/main/examples/src/fs.rs)

`fusio` has an optional Fs trait (use `default-features = false` to disable it). It dispatches common file system operations (open, remove, list, etc.) to specific storage backends (local disk, Amazon S3).

#### [S3 support](https://github.com/tonbo-io/fusio/blob/main/examples/src/s3.rs)

`fusio` has optional Amazon S3 support (enable it with `features = ["tokio-http", "aws"]`); the behavior of S3 operations and credentials does not depend on `tokio`.

## When to choose fusio?

 Overall, `fusio` carefully selects a subset of semantics and behaviors from multiple storage backends and async runtimes to ensure native performance in most scenarios. For example, `fusio` adopts a completion-based API (inspired by [monoio](https://docs.rs/monoio/latest/monoio/io/trait.AsyncReadRent.html)) so that file operations on `tokio` and `tokio-uring`  perform the same as they would without `fusio`.

### compare with `object_store`

`object_store` is locked to tokio and also depends on `bytes`. `fusio` uses `IoBuf` / `IoBufMut` to allow `&[u8]` and `Vec<u8>` to avoid potential runtime costs. If you do not need to consider other async runtimes, try `object_store`; as the official implementation, it integrates well with arrow and parquet.

### compare with `opendal`

`fusio` does not aim to be a full data access layer like `opendal`. `fusio` keeps features lean, and you are able to enable features and their dependencies one by one. The default binary size of `fusio` is 245KB, which is much smaller than `opendal` (8.9MB). If you need a full ecosystem of DAL (tracing, cache, etc.), try opendal.

Also, compared with `opendal::Operator`, fusio exposes core traits and allows them to be implemented in third-party crates.

## Roadmap
- abstractions
  - [x] file operations
  - [x] (partial) file system operations
- storage backend implementations
  - disk
    - [x] tokio
    - [x] tokio-uring
    - [x] monoio
  - [x] network
    - [x] HTTP client trait wi
    - [x] network storage runtime support
      - [x] tokio (over reqwest)
      - [ ] monoio (over hyper-tls)
      - [ ] tokio-uring (over hyper-tls)
    - [x] Amazon S3
    - [ ] Azure Blob Storage
    - [ ] Cloudflare R2
  - [ ] in-memory
- [ ] [conditional operations](https://aws.amazon.com/cn/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)
- extensions
  - [x] parquet support
  - [x] object_store support

## Credits
- `monoio`: all core traits—buffer, read, and write—are highly inspired by it
- `futures`: its design of abstractions and organization of several crates (core, util, etc.) to avoid coupling have influenced `fusio`'s design
- `opendal`: Compile-time poll-based/completion-based runtime switching inspires `fusio`
- `object_store`: `fusio` adopts S3 credential and path behaviors from it

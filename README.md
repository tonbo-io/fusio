<p style="text-align: center;">
  <a href="https://crates.io/crates/fusio">
    <img alt="fusio" src="https://github.com/user-attachments/assets/52e680dc-9c03-4dca-ae07-5d57ba452af8">
  </a>
</p>

<p style="text-align: center;">
  <a href="https://crates.io/crates/fusio">
    <img alt="crates.io" src="https://img.shields.io/crates/v/fusio">
  </a>

  <a href="https://docs.rs/fusio/latest/fusio/">
    <img alt="docs.rs" src="https://img.shields.io/docsrs/fusio">
  </a>
</p>

`fusio` is a library that provides [random read](https://docs.rs/fusio/latest/fusio/trait.Read.html) and [sequential write](https://docs.rs/fusio/latest/fusio/trait.Write.html) traits to operate on multiple storage backends (e.g., local disk, Amazon S3) across various asynchronous runtimes—both poll-based ([tokio](https://github.com/tokio-rs/tokio)) and completion-based ([tokio-uring](https://github.com/tokio-rs/tokio-uring), [monoio](https://github.com/bytedance/monoio)). The main goals are:
- lean: binary size is at least 14× smaller than other alternatives.
- minimal-cost abstraction: compared to bare storage backends, trait definitions allow dispatching file operations without extra overhead.
- extensibility: exposes traits to support implementing storage backends as third-party crates.

> **`fusio` is now at preview version, please join our [community](https://discord.gg/j27XVFVmJM) to attend its development and semantics / behaviors discussion.**

## Why do we need `fusio`?
While developing [Tonbo](https://github.com/tonbo-io/tonbo) (an embedded database ideal for data-intensive application), we needed a flexible and efficient way to handle file and file system operations across multiple storage backends—such as memory, local disk, and remote object storage. We also required compatibility with various asynchronous runtimes, including both completion-based runtimes and event loops in languages like Python and JavaScript.

`fusio` addresses these needs by providing:
- traits that allow dispatch of file and file system operations to multiple storage backends.
- different async runtimes, not only disk but also network I/O.
- compact form: ideal for embedded libs like Tonbo.
- extensibility via third-party crates, enabling custom asynchronous file and file system implementations.

For more context, please check this request to `arrow-rs` [apache/arrow-rs#6051](https://github.com/apache/arrow-rs/issues/6051) that presents the motivation behind `fusio`.

## How to use it?

### Installation
```toml
fusio = { version = "*", features = ["tokio"] }
```

### Examples

#### [Runtime agnostic](https://github.com/tonbo-io/fusio/blob/main/examples/src/multi_runtime.rs)

`fusio` supports switching the async runtime at compile time. Middleware libraries can build runtime-agnostic implementations, allowing the top-level application to choose the runtime.

#### [Object safety](https://github.com/tonbo-io/fusio/blob/main/examples/src/object.rs)

`fusio` provides two sets of traits:
- `Read` / `Write` / `Seek` / `Fs` are not object-safe.
- `DynRead` / `DynWrite` / `DynSeek` / `DynFs` are object-safe.

You can freely transmute between them.

#### [File system traits](https://github.com/tonbo-io/fusio/blob/main/examples/src/fs.rs)

`fusio` has an optional Fs trait (use `default-features = false` to disable it). It dispatches common file system operations (open, remove, list, etc.) to specific storage backends (local disk, Amazon S3).

#### [S3 support](https://github.com/tonbo-io/fusio/blob/main/examples/src/s3.rs)

`fusio` has optional Amazon S3 support (enable it with `features = ["tokio-http", "aws"]`); the behavior of S3 operations and credentials does not depend on `tokio`.

##### S3 append semantics

Opening an object with `OpenOptions::write(true).truncate(false)` now keeps the request fully server-side: `fusio` starts a multipart upload, uses `UploadPartCopy` to splice the existing object into the new upload, and preserves all metadata/SSE headers from the original object. New bytes are buffered while the copy finishes and then streamed as additional parts. Downstream consumers such as [`lotus`](../lotus) can remove their reopen-or-404 append workarounds once they depend on this release.

### WASM executors

- `executor-web`: browser/web executor for `wasm32` (no OPFS dependency). `JoinHandle::join` always returns an error because `spawn_local` tasks are not awaitable; use other signaling for completion.
- `opfs`: back-compat alias of the same executor when OPFS is enabled. OPFS and S3/`wasm-http` are typically used separately.

Feature combos:

- `cargo build -p fusio --target wasm32-unknown-unknown --no-default-features --features "executor-web,aws,wasm-http"`
- `cargo build -p fusio --target wasm32-unknown-unknown --no-default-features --features "executor-web,opfs,wasm-http"`

See `examples/opfs` for OPFS usage.

## When to choose `fusio`?

When you need a combination of: flexibility, abstraction on different storage backends without a performance penalty and a small binary. Overall, `fusio` carefully selects a subset of semantics and behaviors from multiple storage backends and async runtimes to ensure native performance in most scenarios. For example, `fusio` adopts a completion-based API (inspired by [monoio](https://docs.rs/monoio/latest/monoio/io/trait.AsyncReadRent.html)) so that file operations on `tokio` and `tokio-uring`  have the same performance as they would without `fusio`.

### compare with `object_store`

`object_store` is locked to tokio and also depends on `bytes`. `fusio` uses `IoBuf` / `IoBufMut` to allow `&[u8]` and `Vec<u8>` to avoid potential runtime costs. If you do not need to consider other async runtimes, try `object_store`; as the official implementation, it integrates well with Apache Arrow and Parquet.

### compare with `opendal`

`fusio` does not aim to be a full data access layer like `opendal`. `fusio` keeps features lean, and you are able to enable features and their dependencies one by one. The default binary size of `fusio` is 16KB, which is smaller than `opendal` (439KB). If you need a full ecosystem of DAL (tracing, logging, metrics, retry, etc.), try opendal.

Also, compared with `opendal::Operator`, fusio exposes core traits and allows them to be implemented in third-party crates.

## Roadmap
- abstractions
  - [x] file operations
  - [x] (partial) file system operations
- storage backend implementations
  - [x] disk
    - [x] tokio
    - [x] tokio-uring
    - [x] monoio
  - [ ] network
    - [x] HTTP client trait
    - [x] network storage runtime support
      - [x] tokio (over reqwest)
      - [x] monoio (over monoio-http-client)
      - [ ] tokio-uring (over hyper-tls)
    - [x] Amazon S3
    - [ ] Azure Blob Storage
    - [ ] Cloudflare R2
  - [x] in-memory
- [ ] [conditional operations](https://aws.amazon.com/cn/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)
- extensions
  - [x] parquet support
  - [x] object_store support

## Credits
- `monoio`: all core traits—buffer, read, and write—are highly inspired by it.
- `futures`: its design of abstractions and organization of several crates (core, util, etc.) to avoid coupling have influenced `fusio`'s design.
- `opendal`: Compile-time poll-based/completion-based runtime switching inspires `fusio`.
- `object_store`: `fusio` adopts S3 credential and path behaviors from it.

## Changelog

- Unreleased: native S3 append path now relies on multipart `UploadPartCopy`, preserving existing metadata and SSE headers. This allows downstream clients (e.g., [`lotus`](../lotus)) to delete their manual append/404 recovery code once they upgrade to this version.

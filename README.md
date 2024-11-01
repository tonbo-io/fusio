# Fusio

<p align="center">
  <a href="https://crates.io/crates/fusio">
    <img alt="crates.io" src="https://img.shields.io/crates/v/fusio">
  </a>

  <a href="https://docs.rs/fusio/latest/fusio/">
    <img alt="docs.rs" src="https://img.shields.io/docsrs/fusio">
  </a>
</p>

`fusio` provides [Read](https://docs.rs/fusio/latest/fusio/trait.Read.html) and [Write](https://docs.rs/fusio/latest/fusio/trait.Write.html) traits to operate on multiple storage backends (e.g., local disk, Amazon S3) across various asynchronous runtimes—both poll-based ([tokio](https://github.com/tokio-rs/tokio)) and completion-based ([tokio-uring](https://github.com/tokio-rs/tokio-uring), [monoio](https://github.com/bytedance/monoio))—with:
- lean: binary size is at least 14× smaller than others.
- minimal-cost abstraction: compared to bare storage backends, trait definitions allow dispatching file operations without extra overhead.
- extensible: exposes traits to support implementing storage backends as third-party crates.

> **`fusio` is now at preview version, please join our [community](https://discord.gg/j27XVFVmJM) to attend its development and semantics / behaviors discussion.**

## Why do we need `fusio`?
In developing [Tonbo](https://github.com/tonbo-io/tonbo), we needed a flexible and efficient way to handle file and file system operations across multiple storage backends—such as memory, local disk, and remote object storage. We also required compatibility with various asynchronous runtimes, including both completion-based runtimes and event loops in languages like Python and JavaScript.

`fusio` addresses these needs by providing:
- offers traits that allow dispatch of file and file system operations to multiple storage backends.
- usable in diverse async runtimes, not only disk but also network I/O.
- ideal for embedded libs like Tonbo.
- can be extended via third-party crates, enabling custom asynchronous file and file system implementations.

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

`fusio` provides two sets of traits:
- `Read` / `Write` / `Seek` / `Fs` are not object-safe.
- `DynRead` / `DynWrite` / `DynSeek` / `DynFs` are object-safe.

You can freely transmute between them.

#### [File system traits](https://github.com/tonbo-io/fusio/blob/main/examples/src/fs.rs)

`fusio` has an optional Fs trait (use `default-features = false` to disable it). It dispatches common file system operations (open, remove, list, etc.) to specific storage backends (local disk, Amazon S3).

#### [S3 support](https://github.com/tonbo-io/fusio/blob/main/examples/src/s3.rs)

`fusio` has optional Amazon S3 support (enable it with `features = ["tokio-http", "aws"]`); the behavior of S3 operations and credentials does not depend on `tokio`.

## When to choose `fusio`?

 Overall, `fusio` carefully selects a subset of semantics and behaviors from multiple storage backends and async runtimes to ensure native performance in most scenarios. For example, `fusio` adopts a completion-based API (inspired by [monoio](https://docs.rs/monoio/latest/monoio/io/trait.AsyncReadRent.html)) so that file operations on `tokio` and `tokio-uring`  have the same performance as they would without `fusio`.

### compare with `object_store`

`object_store` is locked to tokio and also depends on `bytes`. `fusio` uses `IoBuf` / `IoBufMut` to allow `&[u8]` and `Vec<u8>` to avoid potential runtime costs. If you do not need to consider other async runtimes, try `object_store`; as the official implementation, it integrates well with Apache Arrow and Parquet.

### compare with `opendal`

`fusio` does not aim to be a full data access layer like `opendal`. `fusio` keeps features lean, and you are able to enable features and their dependencies one by one. The default binary size of `fusio` is 245KB, which is smaller than `opendal` (439KB). If you need a full ecosystem of DAL (tracing, cache, etc.), try opendal.

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
- `monoio`: all core traits—buffer, read, and write—are highly inspired by it.
- `futures`: its design of abstractions and organization of several crates (core, util, etc.) to avoid coupling have influenced `fusio`'s design.
- `opendal`: Compile-time poll-based/completion-based runtime switching inspires `fusio`.
- `object_store`: `fusio` adopts S3 credential and path behaviors from it.

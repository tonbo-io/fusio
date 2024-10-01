# Fusio

Fusio provides [Read](https://github.com/tonbo-io/fusio/blob/main/fusio/src/lib.rs#L81) / [Write](https://github.com/tonbo-io/fusio/blob/main/fusio/src/lib.rs#L63) trait to operate multiple storage (local disk, Amazon S3) on multiple poll-based ([tokio](https://github.com/tokio-rs/tokio)) / completion-based async runtime ([tokio-uring](https://github.com/tokio-rs/tokio-uring), [monoio](https://github.com/bytedance/monoio)) with:
- lean: binary size is at least 14x smaller than others
- minimal cost abstraction: compare with bare storage backend, trait definitions promise dispatching file operations without extra costs
- extensible: expose traits to support implementing storage backend as [third-party crates](https://github.com/tonbo-io/fusio/tree/main/fusio-object-store)

> **Fusio is now at preview version, please join our [community](https://discord.gg/j27XVFVmJM) to attend its development and semantic / behavior discussion.**

## Why need Fusio?

Since we start to integrate object storage in [Tonbo](https://github.com/tonbo-io/tonbo), we need file and file system abstractions to dispatch read and write operations to multiple storage backend: memory, local disk, remote object storage and so on. We found that the exist solution has limitations as below:
- local or remote file system accessing is not able to be usable in kinds of async runtimes (not only completion-based runime, but also Python / JS event loop)
- most of VFS implementations are designed for backend server scenarios. As an embedded database, Tonbo needs a lean implementation for embedded, and also a set of traits, allows to extend asynchronous file / file system approach as third-party crates.

For more context, please check [apache/arrow-rs#6051](https://github.com/apache/arrow-rs/issues/6051).

## How to use it?

Because it is not possible to make poll-based async runtime compatible with completion-based at runtime, `fusio` supports switch runtime at compile time

### Installation
```toml
fusio = { version = "*", features = ["tokio"] }
```

### Examples
-

## When choose fusio?

Targets of fusio is different with [object_store](https://github.com/apache/arrow-rs/tree/master/object_store) or [opendal](https://github.com/apache/opendal).

### compare with `object_store`

`object_store` is locked on [tokio](https://github.com/tokio-rs/tokio) runtime in the current, and also [bytes](https://github.com/apache/arrow-rs/blob/master/object_store/src/payload.rs#L23). `fusio` chooses completion-based like API (inspired by [monoio](https://docs.rs/monoio/latest/monoio/io/trait.AsyncReadRent.html)) to get the minimal cost abstraction in all kinds of async runtimes.

`fusio` also uses [IoBuf](https://github.com/tonbo-io/fusio/blob/main/fusio/src/lib.rs#L53) / [IoBufMut](https://github.com/tonbo-io/fusio/blob/main/fusio/src/lib.rs#L64) to allow `&[u8]`, `Vec<u8>` avoid potential runtime costs. If you are not aware of vendor lock-in, try `object_store`, as the official implementation, it integrates well with `arrow` and `parquet`.

### compare with `opendal`

`fusio` does not aim to be a full data access layer like `opendal`. `fusio` is able to enable features and their dependencies on by one. The default binary size of `fusio` is 245KB, which is much more smaller than `opendal` (8.9MB). If you need a full ecosystem of DAL (tracing, cache, etc.) try `opendal`.

Also,

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
      - [x] tokio (base on reqwest)
      - [ ] monoio (base on hyper-tls)
      - [ ] tokio-uring (base on hyper-tls)
    - [x] Amazon S3
    - [ ] Azure Blob Storage
    - [ ] Cloudflare R2
  - [ ] in-memory
- [ ] [conditional operations](https://aws.amazon.com/cn/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)
- extensions
  - [x] parquet support
  - [x] object_store support

## Credits
- `monoio`: all core traits: buffer, read and write are highly inspire by it
- `futures`: how it designs abstractions and organizes several crates (core, util, etc.) to avoid coupling impact `fusio`'s design
- `opendal`: compile-time poll-based / completion-based runtime switch insipres `fusio`
- `object_store`: `fusio` copies S3 credential and path behaviors from it

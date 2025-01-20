
# Fusio Log
`fusio-log` is an append-only log library for Rust. Leveraging [fusio](https://github.com/tonbo-io/fusio), `fusio-log` supports various backends like disk, wasm, and S3.

For those unfamiliar with the database, the key difference between `fusio-log` and other "logging" crates is that `fusio-log` not only supports writing logs through the `write` and `write_batch` methods, but also allows reading logs via the `recover` method.

This crate is designed to build database components like [write-ahead logging](https://en.wikipedia.org/wiki/Write-ahead_logging) or metadata storage, `fusio-log` supports transactional write/read via `write_batch`, ensuring that logs in the same batch are recovered together.

For further more, please read https://datatechnologytoday.wordpress.com/2014/02/10/the-log-is-the-database/.


## Usage

1. Define data structure.
```rust
struct User {
    id: u64,
    name: String,
    email: Option<String>,
}
```

2. Implement `Encode` and `Decode` trait for it.
3. Start to use fusio-log.

```rust
#[tokio::main]
async fn main() {
    let temp_dir = TempDir::new().unwrap();
    let path = Path::from_filesystem_path(temp_dir.path())
        .unwrap()
        .child("log");

    let mut logger = Options::new(path).build::<User>().await.unwrap();
    logger.write(&User {id: 1, name: "fusio-log".to_string(), None}).await.unwrap();
    logger.write_batch([
        User {id: 2, name: "fusio".to_string(), "contact@tonbo.io"}
        User {id: 3, name: "tonbo".to_string(), None}
    ].into_iter()).await.unwrap();

    logger.flush().await.unwrap();
    logger.close().await.unwrap();

}
```

Recover from log file:
```rust
let stream = Options::new(path)
    .recover::<User>()
    .await
    .unwrap();
while let Ok(users) = stream.try_next().await {
    match users {
        Some(users) => {
            for user in users {
                println("{}" user.id)
            }
        };
        None => println();
    }
}
```

### Use with S3
```rust

let path = Path::from_url_path("log").unwrap();
let option = Options::new(path).fs(FsOptions::S3 {
    bucket: "data".to_string(),
    credential: Some(fusio::remotes::aws::AwsCredential {
        key_id: "key_id".to_string(),
        secret_key: "secret_key".to_string(),
        token: None,
    }),
    endpoint: None,
    region: Some("region".to_string()),
    sign_payload: None,
    checksum: None,
});

let mut logger = option.build::<User>().await.unwrap();
logger
    .write(&User {
        id: 100,
        name: "Name".to_string(),
        email: None,
    })
    .await
    .unwrap();
```

### Use in Wasm

Please make sure disable default features and enable `web` feature.

```toml
fusio-log = {git = "https://github.com/tonbo-io/fusio-log", default-features = false, features = ["bytes", "web"]}
```

Then, use `Path::from_opfs_path` to replace `Path::from_filesystem_path`
```rust
let temp_dir = TempDir::new().unwrap();
let path = Path::from_opfs_path(temp_dir.path())
    .unwrap()
    .child("log");

let mut logger = Options::new(path).build::<User>().await.unwrap();
logger.write(&User {id: 1, name: "fusio-log".to_string(), None}).await.unwrap();
```

## Build
### Build in Rust
```sh
cargo build
```

### Build in Wasm

Build with [wasm-pack](https://github.com/rustwasm/wasm-pack)

```sh
wasm-pack build --no-default-features --features web,bytes
```
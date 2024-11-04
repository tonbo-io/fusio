use fusio::{fs::OpenOptions, path::Path, Read, Write};
use fusio_dispatch::FsOptions;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub async fn write_to_opfs() {
    let fs_options = FsOptions::Local;
    let fs = fs_options.parse().unwrap();
    let mut file = fs
        .open_options(
            &Path::from_opfs_path("foo").unwrap(),
            OpenOptions::default().create(true),
        )
        .await
        .unwrap();

    let write_buf = "hello, fusio".as_bytes();

    let (result, _) = file.write_all(write_buf).await;
    result.unwrap();

    file.close().await.unwrap();
}

#[wasm_bindgen]
pub async fn read_from_opfs() {
    let fs_options = FsOptions::Local;
    let fs = fs_options.parse().unwrap();
    let mut file = fs
        .open(&Path::from_opfs_path("foo").unwrap())
        .await
        .unwrap();

    let (result, _read_buf) = file.read_to_end_at(vec![], 0).await;
    result.unwrap();
}

#[cfg(test)]
#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub(crate) mod tests {

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    use fusio::{disk::OPFS, fs::OpenOptions, path::Path, DynFs, Read, Write};
    use futures_util::StreamExt;
    use wasm_bindgen_test::wasm_bindgen_test;

    async fn remove_all(fs: &OPFS, pathes: &[&str]) {
        for path in pathes {
            fs.remove(&(*path).into()).await.unwrap();
        }
    }

    #[wasm_bindgen_test]
    async fn test_opfs_list() {
        let fs = OPFS;
        let path = "test_opfs_dir".to_string();
        fs.create_dir_all(&path.into()).await.unwrap();
        let _ = fs
            .open_options(&"file1".into(), OpenOptions::default().create(true))
            .await
            .unwrap();
        let _ = fs
            .open_options(&"file2".into(), OpenOptions::default().create(true))
            .await;

        let base_path = Path::from_opfs_path("/".to_string()).unwrap();
        let mut stream = fs.list(&base_path).await.unwrap();
        let mut result_len = 0;
        let expected = ["test_opfs_dir", "file1", "file2"];
        while let Some(Ok(meta)) = stream.next().await {
            assert!(expected.contains(&meta.path.as_ref()));
            result_len += 1;
        }
        assert_eq!(result_len, 3);

        remove_all(&fs, &["test_opfs_dir", "file1", "file2"]).await;
    }

    #[wasm_bindgen_test]
    async fn test_create_nested_entry() {
        let fs = OPFS;
        let path = Path::from_opfs_path("test_opfs_dir/sub_dir".to_string()).unwrap();
        fs.create_dir_all(&path).await.unwrap();
        let _ = fs
            .open_options(
                &Path::from_opfs_path("test_opfs_dir/file").unwrap(),
                OpenOptions::default().create(true),
            )
            .await
            .unwrap();
        let _ = fs
            .open_options(
                &Path::from_opfs_path("test_opfs_dir/sub_dir/sub_file").unwrap(),
                OpenOptions::default().create(true),
            )
            .await
            .unwrap();

        let base_path = Path::from_opfs_path("test_opfs_dir".to_string()).unwrap();
        let mut stream = fs.list(&base_path).await.unwrap();
        let expected = ["test_opfs_dir/sub_dir", "test_opfs_dir/file"];
        let mut result_len = 0;
        while let Some(Ok(meta)) = stream.next().await {
            assert!(expected.contains(&meta.path.as_ref()));
            result_len += 1;
        }
        assert_eq!(result_len, 2);

        fs.remove(&Path::from_opfs_path("test_opfs_dir/file").unwrap())
            .await
            .unwrap();

        let expected = ["test_opfs_dir/sub_dir"];
        let mut result_len = 0;
        let mut stream = fs.list(&base_path).await.unwrap();
        while let Some(Ok(meta)) = stream.next().await {
            assert!(expected.contains(&meta.path.as_ref()));
            result_len += 1;
        }
        assert_eq!(result_len, 1);

        remove_all(&fs, &["test_opfs_dir"]).await;
    }

    #[wasm_bindgen_test]
    async fn test_opfs_read_write() {
        let fs = OPFS;
        let mut file = fs
            .open_options(&"file_rw".into(), OpenOptions::default().create(true).truncate(true))
            .await
            .unwrap();
        let (result, _) = file.write_all([1, 2, 3, 4].as_mut()).await;
        result.unwrap();
        let (result, _) = file.write_all([11, 23, 34, 47].as_mut()).await;
        result.unwrap();
        let (result, _) = file.write_all([121, 93, 94, 97].as_mut()).await;
        result.unwrap();
        file.close().await.unwrap();

        let mut file = fs
            .open_options(&"file_rw".into(), OpenOptions::default().create(true))
            .await
            .unwrap();
        let expected = [1_u8, 2, 3, 4, 11, 23, 34, 47, 121, 93, 94, 97];
        for i in 0..12 {
            let (result, data) = file.read_to_end_at(vec![], i).await;
            result.unwrap();
            assert_eq!(data.as_slice(), &expected[i as usize..]);
        }

        let mut buf = [0; 7];
        let (result, data) = file.read_exact_at(buf.as_mut(), 3).await;
        result.unwrap();
        assert_eq!(data, [4, 11, 23, 34, 47, 121, 93]);
        remove_all(&fs, &["file_rw"]).await;
    }

    #[wasm_bindgen_test]
    async fn test_opfs_read_write_utf16() {
        let fs = OPFS;
        let mut file = fs
            .open_options(&"file_utf16".into(), OpenOptions::default().create(true).truncate(true))
            .await
            .unwrap();
        let utf16_bytes: &[u8] = &[
            0x00, 0x48, 0x20, 0xAC, 0x00, 0x6c, 0x00, 0x6c, 0x00, 0x6f, 0x00, 0x20, 0x00, 0x77,
            0x00, 0x6f, 0x00, 0x72, 0x00, 0x6c, 0x00, 0x64, 0x00, 0x21,
        ];

        let (result, _) = file.write_all(utf16_bytes).await;
        result.unwrap();
        file.close().await.unwrap();

        let mut file = fs
            .open_options(&"file_utf16".into(), OpenOptions::default().write(true))
            .await
            .unwrap();
        let (result, data) = file.read_to_end_at(vec![], 0).await;
        result.unwrap();
        assert_eq!(
            data,
            [
                0x00, 0x48, 0x20, 0xAC, 0x00, 0x6c, 0x00, 0x6c, 0x00, 0x6f, 0x00, 0x20, 0x00, 0x77,
                0x00, 0x6f, 0x00, 0x72, 0x00, 0x6c, 0x00, 0x64, 0x00, 0x21,
            ]
        );

        remove_all(&fs, &["file_utf16"]).await;
    }

    #[wasm_bindgen_test]
    async fn test_opfs_read_eof() {
        let fs = OPFS;
        let mut file = fs
            .open_options(&"file_eof".into(), OpenOptions::default().create(true).truncate(true))
            .await
            .unwrap();

        let mut buf = [0; 1];
        let (result, _) = file.read_exact_at(buf.as_mut(), 0).await;
        assert!(result.is_err());

        let (result, _) = file.write_all([1, 2, 3, 4].as_mut()).await;
        result.unwrap();
        file.close().await.unwrap();

        let mut file = fs
            .open_options(&"file_eof".into(), OpenOptions::default().write(true))
            .await
            .unwrap();

        let mut buf = [0; 1];
        let (result, data) = file.read_exact_at(buf.as_mut(), 5).await;
        assert!(result.is_err());
        assert_eq!(data, [0]);
        remove_all(&fs, &["file_eof"]).await;
    }
}

// Integration examples showing how to use Fusio durability semantics on local disk.

#[cfg(test)]
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
mod tokio_example {
    use fusio::{
        durability::apply_on_close,
        fs::{Fs, OpenOptions},
        impls::buffered::BufWriter,
        path::Path,
        DurabilityLevel, Read, Write,
    };
    use tempfile::TempDir;

    #[tokio::test(flavor = "multi_thread")]
    async fn durability_usage_tokio() {
        // Prepare a temp file path
        let temp = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp.path())
            .unwrap()
            .child("durable.txt");

        // Local filesystem (Tokio runtime)
        let fs = fusio::disk::TokioFs;

        // Create the file and wrap it in a buffered writer
        let file = fs
            .open_options(
                &path,
                OpenOptions::default()
                    .create(true)
                    .write(true)
                    .truncate(true),
            )
            .await
            .unwrap();
        let mut writer = BufWriter::new(file, 8 * 1024);

        // Write some data
        let (res, _) = writer.write_all("hello, fusio".as_bytes()).await;
        res.unwrap();

        // Request durability policy on close (persist content + metadata)
        apply_on_close(&mut writer, Some(DurabilityLevel::All))
            .await
            .unwrap();
        writer.close().await.unwrap();

        // Read back to verify content
        let mut reader = fs
            .open_options(&path, OpenOptions::default())
            .await
            .unwrap();
        let (res, buf) = reader.read_to_end_at(Vec::new(), 0).await;
        res.unwrap();
        assert_eq!(buf, b"hello, fusio");
    }
}

// Additional examples for on-demand sync, directory sync, and capability-gated commit fallback.
#[cfg(test)]
#[cfg(all(feature = "tokio", not(target_arch = "wasm32")))]
mod tokio_more_examples {
    use fusio::{
        durability::{apply_on_close, DirSync},
        fs::{Fs, OpenOptions},
        impls::buffered::BufWriter,
        path::Path,
        DurabilityLevel, DurabilityOp, FileSync, Read, SupportsDurability, Write,
    };
    use tempfile::TempDir;

    #[tokio::test(flavor = "multi_thread")]
    async fn on_demand_sync_and_dirsync() {
        let temp = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp.path())
            .unwrap()
            .child("file.txt");
        let fs = fusio::disk::TokioFs;

        let file = fs
            .open_options(
                &path,
                OpenOptions::default()
                    .create(true)
                    .write(true)
                    .truncate(true),
            )
            .await
            .unwrap();
        let mut writer = BufWriter::new(file, 4 * 1024);

        // write and persist content (data) and metadata explicitly
        writer.write_all(&b"abc"[..]).await.0.unwrap();
        writer.sync_data().await.unwrap(); // content only
        writer.sync_all().await.unwrap(); // content + metadata

        // Persist parent directory entry when creating/renaming/deleting.
        // Assert platform-appropriate behavior instead of swallowing errors.
        let dirsync_res = fs.sync_parent(&path).await;
        #[cfg(target_os = "windows")]
        {
            // Explicitly unsupported on Windows in fusio; expect Unsupported.
            match dirsync_res {
                Err(fusio::error::Error::Unsupported { .. }) => {}
                other => panic!("unexpected DirSync result on Windows: {other:?}"),
            }
        }
        #[cfg(not(target_os = "windows"))]
        {
            dirsync_res.unwrap();
        }

        apply_on_close(&mut writer, Some(DurabilityLevel::Data))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let mut r = fs
            .open_options(&path, OpenOptions::default())
            .await
            .unwrap();
        let (res, buf) = r.read_to_end_at(Vec::new(), 0).await;
        res.unwrap();
        assert_eq!(buf, b"abc");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn capability_checks_and_commit_fallback() {
        let temp = TempDir::new().unwrap();
        let path = Path::from_filesystem_path(temp.path())
            .unwrap()
            .child("cap.txt");
        let fs = fusio::disk::TokioFs;

        // TokioFs advertises DirSync capability
        assert!(fs.supports(DurabilityOp::DirSync));

        let file = fs
            .open_options(
                &path,
                OpenOptions::default()
                    .create(true)
                    .write(true)
                    .truncate(true),
            )
            .await
            .unwrap();
        let mut writer = BufWriter::new(file, 4 * 1024);

        // Local files support DataSync and Fsync, but not Commit
        assert!(writer.supports(DurabilityOp::DataSync));
        assert!(writer.supports(DurabilityOp::Fsync));
        assert!(!writer.supports(DurabilityOp::Commit));

        writer.write_all(&b"abc"[..]).await.0.unwrap();

        // Commit fallback pattern: commit if available, otherwise sync_all
        if writer.supports(DurabilityOp::Commit) {
            // If this branch ever triggers on local, something changed.
            #[allow(unreachable_code)]
            {
                use fusio::durability::Commit;
                writer.commit().await.unwrap();
            }
        } else {
            writer.sync_all().await.unwrap();
        }

        writer.close().await.unwrap();

        let mut r = fs
            .open_options(&path, OpenOptions::default())
            .await
            .unwrap();
        let (res, buf) = r.read_to_end_at(Vec::new(), 0).await;
        res.unwrap();
        assert_eq!(buf, b"abc");
    }
}

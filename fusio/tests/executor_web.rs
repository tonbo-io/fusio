#[cfg(all(feature = "executor-web", target_arch = "wasm32", test))]
pub(crate) mod tests {
    use std::time::Duration;

    use fusio::executor::{web::WebExecutor, Executor, JoinHandle, Timer};
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn spawn_and_sleep_progresses() {
        let exec = WebExecutor::new();
        let value = WebExecutor::rw_lock(0);

        let handle = exec.spawn({
            let value = value.clone();
            async move {
                let mut guard = value.write().await;
                *guard = 1;
            }
        });

        assert!(handle.join().await.is_err());

        exec.sleep(Duration::from_millis(5)).await;
        assert_eq!(*value.read().await, 1);
    }

    #[cfg(feature = "fs")]
    #[wasm_bindgen_test]
    async fn in_memory_fs_roundtrip() {
        use fusio::{
            fs::{Fs, OpenOptions},
            impls::mem::fs::InMemoryFs,
            Read, Write,
        };

        let fs = InMemoryFs::new();
        let mut file = fs
            .open_options(
                &"web/roundtrip.txt".into(),
                OpenOptions::default()
                    .write(true)
                    .create(true)
                    .truncate(true),
            )
            .await
            .unwrap();

        let (result, _) = file.write_all(&b"fusio-web"[..]).await;
        result.unwrap();
        let (result, buf) = file.read_to_end_at(Vec::new(), 0).await;
        result.unwrap();
        assert_eq!(buf, b"fusio-web");
        file.close().await.unwrap();
    }

    use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

    #[wasm_bindgen(module = "/tests/worker_scope_shim.js")]
    extern "C" {
        #[wasm_bindgen(catch)]
        fn install_fake_worker_scope() -> Result<bool, JsValue>;
    }

    #[wasm_bindgen_test]
    async fn sleep_in_fake_worker_scope() {
        // If we cannot install a fake worker scope in this host (e.g., prototype
        // changes are blocked), skip the check.
        let Ok(Ok(installed)) = std::panic::catch_unwind(|| unsafe { install_fake_worker_scope() })
        else {
            return;
        };
        if !installed {
            return;
        }
        let exec = WebExecutor::new();
        exec.sleep(Duration::from_millis(1)).await;
    }
}

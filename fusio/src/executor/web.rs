//! Web executor for `wasm32` targets.
//!
//! Tasks are scheduled via `wasm_bindgen_futures::spawn_local` and wrapped in
//! a oneshot so join handles can await completion. Synchronization relies on
//! `async_lock::RwLock` with `MaybeSend`/`MaybeSync` bounds suited for
//! single-threaded WASM.

use std::{error::Error, future::Future, sync::Arc, time::SystemTime};

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use futures_channel::oneshot;
use wasm_bindgen::{prelude::*, JsCast};

use super::{Executor, JoinHandle, RwLock, Timer};

#[wasm_bindgen]
#[derive(Clone, Copy)]
pub struct WebExecutor;

impl Default for WebExecutor {
    fn default() -> Self {
        Self
    }
}

#[wasm_bindgen]
impl WebExecutor {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self
    }
}

pub struct WebJoinHandle<R> {
    receiver: oneshot::Receiver<Result<R, Box<dyn Error>>>,
}

impl<R> JoinHandle<R> for WebJoinHandle<R>
where
    R: MaybeSend,
{
    async fn join(self) -> Result<R, Box<dyn Error>> {
        match self.receiver.await {
            Ok(result) => result,
            Err(_canceled) => Err("Spawned task was canceled before completion".into()),
        }
    }
}

impl<T> RwLock<T> for Arc<async_lock::RwLock<T>>
where
    T: MaybeSend + MaybeSync,
{
    type ReadGuard<'a>
        = async_lock::RwLockReadGuard<'a, T>
    where
        Self: 'a;

    type WriteGuard<'a>
        = async_lock::RwLockWriteGuard<'a, T>
    where
        Self: 'a;

    async fn read(&self) -> Self::ReadGuard<'_> {
        async_lock::RwLock::read(self).await
    }

    async fn write(&self) -> Self::WriteGuard<'_> {
        async_lock::RwLock::write(self).await
    }
}

impl Executor for WebExecutor {
    type JoinHandle<R>
        = WebJoinHandle<R>
    where
        R: MaybeSend;

    type RwLock<T>
        = Arc<async_lock::RwLock<T>>
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        let (sender, receiver) = oneshot::channel::<Result<F::Output, Box<dyn Error>>>();

        wasm_bindgen_futures::spawn_local(async move {
            let result = future.await;
            let _ = sender.send(Ok(result));
        });

        WebJoinHandle { receiver }
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync,
    {
        Arc::new(async_lock::RwLock::new(value))
    }
}

impl Timer for WebExecutor {
    fn sleep(
        &self,
        dur: core::time::Duration,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move {
            let ms: i32 = dur.as_millis() as i32;
            let promise = js_sys::Promise::new(&mut |resolve, _reject| {
                // Support both Window and WorkerGlobalScope to work in main-thread
                // browsers and worker/edge runtimes.
                let global = js_sys::global();
                if let Some(window) = global.dyn_ref::<web_sys::Window>() {
                    let cb = Closure::once_into_js(move || {
                        let _ = resolve.call0(&wasm_bindgen::JsValue::UNDEFINED);
                    });
                    let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                        cb.as_ref().unchecked_ref(),
                        ms,
                    );
                    return;
                }

                if let Some(worker) = global.dyn_ref::<web_sys::WorkerGlobalScope>() {
                    let cb = Closure::once_into_js(move || {
                        let _ = resolve.call0(&wasm_bindgen::JsValue::UNDEFINED);
                    });
                    let _ = worker.set_timeout_with_callback_and_timeout_and_arguments_0(
                        cb.as_ref().unchecked_ref(),
                        ms,
                    );
                    return;
                }

                // Fallback: if a setTimeout-like function exists on globalThis
                // (e.g., Node-style shims), use it; otherwise resolve immediately.
                if let Some(set_timeout) =
                    js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
                        .ok()
                        .and_then(|v| v.dyn_into::<js_sys::Function>().ok())
                {
                    let cb = Closure::once_into_js(move || {
                        let _ = resolve.call0(&wasm_bindgen::JsValue::UNDEFINED);
                    });
                    let _ = set_timeout.call2(&global, cb.as_ref(), &JsValue::from_f64(ms as f64));
                    return;
                }

                let _ = resolve.call0(&wasm_bindgen::JsValue::UNDEFINED);
            });
            let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
        })
    }

    fn now(&self) -> SystemTime {
        super::now()
    }
}

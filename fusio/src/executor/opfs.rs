use std::{error::Error, future::Future, sync::Arc};

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use wasm_bindgen::{prelude::*, JsCast};

use super::{Executor, JoinHandle, RwLock, Sleeper};

#[wasm_bindgen]
pub struct OpfsExecutor;

impl Default for OpfsExecutor {
    fn default() -> Self {
        Self
    }
}

impl OpfsExecutor {
    pub fn new() -> Self {
        Self
    }
}

pub struct OpfsJoinHandle<R> {
    _phantom: core::marker::PhantomData<R>,
}

impl<R> JoinHandle<R> for OpfsJoinHandle<R>
where
    R: MaybeSend,
{
    async fn join(self) -> Result<R, Box<dyn Error>> {
        // In WASM spawn_local has no join handle; document limitation.
        Err("Cannot join spawned tasks in WASM".into())
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

impl Executor for OpfsExecutor {
    type JoinHandle<R>
        = OpfsJoinHandle<R>
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
        wasm_bindgen_futures::spawn_local(async move {
            let _ = future.await;
        });
        OpfsJoinHandle {
            _phantom: core::marker::PhantomData,
        }
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync,
    {
        Arc::new(async_lock::RwLock::new(value))
    }
}

impl Sleeper for OpfsExecutor {
    fn sleep(
        &self,
        dur: core::time::Duration,
    ) -> core::pin::Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move {
            // Create a JS Promise that resolves after setTimeout, then await it.
            let ms: i32 = dur.as_millis() as i32;
            let promise = js_sys::Promise::new(&mut |resolve, _reject| {
                let window = web_sys::window().expect("window");
                let cb = Closure::once_into_js(move || {
                    let _ = resolve.call0(&wasm_bindgen::JsValue::UNDEFINED);
                });
                let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                    cb.as_ref().unchecked_ref(),
                    ms,
                );
            });
            let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
        })
    }
}

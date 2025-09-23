use core::{pin::Pin, time::Duration};
use std::{
    error::Error,
    future::Future,
    ops::{Deref, DerefMut},
    time::SystemTime,
};

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};

pub trait JoinHandle<R> {
    fn join(self) -> impl Future<Output = Result<R, Box<dyn Error>>> + MaybeSend;
}

pub trait RwLock<T> {
    type ReadGuard<'a>: Deref<Target = T> + MaybeSend + 'a
    where
        Self: 'a;

    type WriteGuard<'a>: DerefMut<Target = T> + MaybeSend + 'a
    where
        Self: 'a;

    fn read(&self) -> impl Future<Output = Self::ReadGuard<'_>> + MaybeSend;

    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + MaybeSend;
}

pub trait Executor: 'static {
    type JoinHandle<R>: JoinHandle<R>
    where
        R: MaybeSend;

    type RwLock<T>: RwLock<T> + MaybeSend + MaybeSync
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend;

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync;
}

/// Minimal timer abstraction to decouple libraries from concrete runtimes.
pub trait Timer: MaybeSend + MaybeSync {
    /// Sleep for the given duration and yield back to the runtime.
    fn sleep(&self, dur: Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>>;

    /// Return the current wall-clock time according to the executor.
    fn now(&self) -> SystemTime;
}

/// A blocking fallback for environments without an async runtime.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Default, Clone, Copy)]
pub struct BlockingSleeper;

#[cfg(not(target_arch = "wasm32"))]
impl Timer for BlockingSleeper {
    fn sleep(&self, dur: Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move { std::thread::sleep(dur) })
    }

    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub mod opfs;

#[cfg(feature = "executor-tokio")]
pub mod tokio;

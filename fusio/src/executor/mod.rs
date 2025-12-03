use core::{pin::Pin, time::Duration};
use std::{
    error::Error,
    future::Future,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::SystemTime,
};

#[cfg(not(target_arch = "wasm32"))]
use async_lock::{RwLock as AsyncRwLock, RwLockReadGuard, RwLockWriteGuard};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
#[cfg(not(target_arch = "wasm32"))]
use futures_executor::block_on;
#[cfg(all(target_arch = "wasm32", feature = "executor-web"))]
use js_sys::Date;

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

pub trait Executor: MaybeSend + MaybeSync + 'static {
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
pub trait Timer: MaybeSend + MaybeSync + 'static {
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

#[cfg(all(feature = "executor-web", target_arch = "wasm32"))]
pub mod web;

#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub mod opfs;

#[cfg(feature = "executor-tokio")]
pub mod tokio;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct BlockingJoinHandle<R>(Option<R>);

#[cfg(not(target_arch = "wasm32"))]
impl<R> JoinHandle<R> for BlockingJoinHandle<R>
where
    R: MaybeSend,
{
    fn join(self) -> impl Future<Output = Result<R, Box<dyn Error>>> + MaybeSend {
        let mut value = self.0;
        async move {
            let out = value.take().expect("blocking join handle already taken");
            Ok(out)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct BlockingRwLock<T>(AsyncRwLock<T>);

#[cfg(not(target_arch = "wasm32"))]
impl<T> RwLock<T> for BlockingRwLock<T>
where
    T: MaybeSend + MaybeSync,
{
    type ReadGuard<'a>
        = RwLockReadGuard<'a, T>
    where
        T: 'a,
        Self: 'a;

    type WriteGuard<'a>
        = RwLockWriteGuard<'a, T>
    where
        T: 'a,
        Self: 'a;

    fn read(&self) -> impl Future<Output = Self::ReadGuard<'_>> + MaybeSend {
        self.0.read()
    }

    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + MaybeSend {
        self.0.write()
    }
}

/// Executes futures synchronously on the current thread and offers blocking timers.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Default, Copy)]
pub struct BlockingExecutor;

#[cfg(not(target_arch = "wasm32"))]
impl Executor for BlockingExecutor {
    type JoinHandle<R>
        = BlockingJoinHandle<R>
    where
        R: MaybeSend;

    type RwLock<T>
        = BlockingRwLock<T>
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        BlockingJoinHandle(Some(block_on(future)))
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync,
    {
        BlockingRwLock(AsyncRwLock::new(value))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Timer for BlockingExecutor {
    fn sleep(&self, dur: Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move { std::thread::sleep(dur) })
    }

    fn now(&self) -> SystemTime {
        now()
    }
}

impl<T> Timer for Arc<T>
where
    T: Timer + ?Sized,
{
    fn sleep(&self, dur: Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        (**self).sleep(dur)
    }

    fn now(&self) -> SystemTime {
        (**self).now()
    }
}

/// Timer that never sleeps and always reports the Unix epoch.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopTimer;

impl Timer for NoopTimer {
    fn sleep(&self, _dur: Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move {})
    }

    fn now(&self) -> SystemTime {
        now()
    }
}

/// Executor that runs tasks synchronously and uses `NoopTimer` for scheduling.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Default)]
pub struct NoopExecutor {
    inner: BlockingExecutor,
}

#[cfg(not(target_arch = "wasm32"))]
impl Executor for NoopExecutor {
    type JoinHandle<R>
        = <BlockingExecutor as Executor>::JoinHandle<R>
    where
        R: MaybeSend;

    type RwLock<T>
        = <BlockingExecutor as Executor>::RwLock<T>
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        self.inner.spawn(future)
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync,
    {
        BlockingExecutor::rw_lock(value)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Timer for NoopExecutor {
    fn sleep(&self, _dur: Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move {})
    }

    fn now(&self) -> SystemTime {
        now()
    }
}

#[inline]
pub(super) fn now() -> SystemTime {
    #[cfg(all(target_arch = "wasm32", feature = "executor-web"))]
    {
        SystemTime::UNIX_EPOCH + Duration::from_millis(Date::now() as u64)
    }
    #[cfg(not(all(target_arch = "wasm32", feature = "executor-web")))]
    {
        SystemTime::now()
    }
}

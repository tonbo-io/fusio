use core::{pin::Pin, time::Duration};
use std::{
    error::Error,
    future::Future,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::SystemTime,
};

use async_lock::{
    Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, RwLock as AsyncRwLock, RwLockReadGuard,
    RwLockWriteGuard,
};
use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
#[cfg(not(target_arch = "wasm32"))]
use futures_executor::block_on;
#[cfg(all(target_arch = "wasm32", feature = "executor-web"))]
use js_sys::Date;

pub trait JoinHandle<R> {
    fn join(self) -> impl Future<Output = Result<R, Box<dyn Error + Send + Sync>>> + MaybeSend;
}

pub trait Mutex<T> {
    type Guard<'a>: DerefMut<Target = T> + MaybeSend + 'a
    where
        Self: 'a;

    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + MaybeSend;
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
    type JoinHandle<R>: JoinHandle<R> + MaybeSend
    where
        R: MaybeSend;

    type Mutex<T>: Mutex<T> + MaybeSend + MaybeSync
    where
        T: MaybeSend + MaybeSync;

    type RwLock<T>: RwLock<T> + MaybeSend + MaybeSync
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend;

    fn mutex<T>(value: T) -> Self::Mutex<T>
    where
        T: MaybeSend + MaybeSync;

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

#[cfg(all(feature = "executor-web", target_arch = "wasm32"))]
pub mod web;

#[cfg(all(feature = "opfs", target_arch = "wasm32"))]
pub mod opfs;

#[cfg(feature = "executor-tokio")]
pub mod tokio;

#[cfg(feature = "monoio")]
pub mod monoio;

/// A join handle that holds a pre-computed result.
#[derive(Debug, Clone)]
pub struct NoopJoinHandle<R>(Option<R>);

impl<R> JoinHandle<R> for NoopJoinHandle<R>
where
    R: MaybeSend,
{
    fn join(self) -> impl Future<Output = Result<R, Box<dyn Error + Send + Sync>>> + MaybeSend {
        let mut value = self.0;
        async move {
            let out = value.take().expect("noop join handle already taken");
            Ok(out)
        }
    }
}

/// A Mutex implementation using async_lock, available on all platforms.
#[derive(Debug)]
pub struct NoopMutex<T>(AsyncMutex<T>);

impl<T> NoopMutex<T> {
    pub fn new(value: T) -> Self {
        Self(AsyncMutex::new(value))
    }
}

impl<T> Mutex<T> for NoopMutex<T>
where
    T: MaybeSend + MaybeSync,
{
    type Guard<'a>
        = AsyncMutexGuard<'a, T>
    where
        T: 'a,
        Self: 'a;

    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + MaybeSend {
        self.0.lock()
    }
}

/// An RwLock implementation using async_lock, available on all platforms.
#[derive(Debug)]
pub struct NoopRwLock<T>(AsyncRwLock<T>);

impl<T> NoopRwLock<T> {
    pub fn new(value: T) -> Self {
        Self(AsyncRwLock::new(value))
    }
}

impl<T> RwLock<T> for NoopRwLock<T>
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

/// A minimal executor that runs futures synchronously (on non-WASM) or provides
/// no-op timer functionality. Available on all platforms.
///
/// On non-WASM platforms, `spawn` executes futures synchronously using `block_on`.
/// On WASM platforms, `spawn` will panic - use `WebExecutor` instead for actual
/// task spawning.
#[derive(Debug, Clone, Default, Copy)]
pub struct NoopExecutor;

impl Executor for NoopExecutor {
    type JoinHandle<R>
        = NoopJoinHandle<R>
    where
        R: MaybeSend;

    type Mutex<T>
        = NoopMutex<T>
    where
        T: MaybeSend + MaybeSync;

    type RwLock<T>
        = NoopRwLock<T>
    where
        T: MaybeSend + MaybeSync;

    #[cfg(not(target_arch = "wasm32"))]
    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        NoopJoinHandle(Some(block_on(future)))
    }

    #[cfg(target_arch = "wasm32")]
    fn spawn<F>(&self, _future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        panic!("NoopExecutor::spawn is not supported on WASM. Use WebExecutor instead.")
    }

    fn mutex<T>(value: T) -> Self::Mutex<T>
    where
        T: MaybeSend + MaybeSync,
    {
        NoopMutex::new(value)
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync,
    {
        NoopRwLock::new(value)
    }
}

impl Timer for NoopExecutor {
    fn sleep(&self, _dur: Duration) -> Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move {})
    }

    fn now(&self) -> SystemTime {
        now()
    }
}

/// Backward-compatible alias for `NoopExecutor`.
#[cfg(not(target_arch = "wasm32"))]
#[deprecated(since = "0.5.0", note = "Use NoopExecutor instead")]
pub type BlockingExecutor = NoopExecutor;

/// Backward-compatible alias for `NoopJoinHandle`.
#[cfg(not(target_arch = "wasm32"))]
#[deprecated(since = "0.5.0", note = "Use NoopJoinHandle instead")]
pub type BlockingJoinHandle<R> = NoopJoinHandle<R>;

/// Backward-compatible alias for `NoopRwLock`.
#[cfg(not(target_arch = "wasm32"))]
#[deprecated(since = "0.5.0", note = "Use NoopRwLock instead")]
pub type BlockingRwLock<T> = NoopRwLock<T>;

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

/// Timer that never sleeps and always reports current time.
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

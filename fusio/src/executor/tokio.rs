use std::{error::Error, future::Future};

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};
use tokio::runtime::Handle;

use super::{Executor, JoinHandle, RwLock, Timer};

impl<R: MaybeSend> JoinHandle<R> for tokio::task::JoinHandle<R> {
    async fn join(self) -> Result<R, Box<dyn Error>> {
        self.await.map_err(|e| Box::new(e) as Box<dyn Error>)
    }
}

impl<T: MaybeSend + MaybeSync> RwLock<T> for tokio::sync::RwLock<T> {
    type ReadGuard<'a>
        = tokio::sync::RwLockReadGuard<'a, T>
    where
        T: 'a;
    type WriteGuard<'a>
        = tokio::sync::RwLockWriteGuard<'a, T>
    where
        T: 'a;

    async fn read(&self) -> Self::ReadGuard<'_> {
        ::tokio::sync::RwLock::read(self).await
    }

    async fn write(&self) -> Self::WriteGuard<'_> {
        ::tokio::sync::RwLock::write(self).await
    }
}

#[derive(Clone)]
pub struct TokioExecutor {
    handle: Handle,
}

impl Default for TokioExecutor {
    fn default() -> Self {
        Self {
            handle: Handle::current(),
        }
    }
}

impl Executor for TokioExecutor {
    type JoinHandle<R>
        = tokio::task::JoinHandle<R>
    where
        R: MaybeSend;

    type RwLock<T>
        = tokio::sync::RwLock<T>
    where
        T: MaybeSend + MaybeSync;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        self.handle.spawn(future)
    }

    fn rw_lock<T>(value: T) -> Self::RwLock<T>
    where
        T: MaybeSend + MaybeSync,
    {
        tokio::sync::RwLock::new(value)
    }
}

impl Timer for TokioExecutor {
    fn sleep(
        &self,
        dur: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move {
            tokio::time::sleep(dur).await;
        })
    }

    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
}

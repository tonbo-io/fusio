use std::{error::Error, future::Future};

use fusio_core::{MaybeSend, MaybeSendFuture, MaybeSync};

use super::{Executor, JoinHandle, NoopMutex, NoopRwLock, Timer};

pub struct MonoioJoinHandle<R>(monoio::task::JoinHandle<R>);

impl<R> JoinHandle<R> for MonoioJoinHandle<R>
where
    R: MaybeSend,
{
    async fn join(self) -> Result<R, Box<dyn Error + Send + Sync>> {
        Ok(self.0.await)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MonoioExecutor;

impl Executor for MonoioExecutor {
    type JoinHandle<R>
        = MonoioJoinHandle<R>
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

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + MaybeSend + 'static,
        F::Output: MaybeSend,
    {
        MonoioJoinHandle(monoio::spawn(future))
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

impl Timer for MonoioExecutor {
    fn sleep(
        &self,
        dur: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn MaybeSendFuture<Output = ()>>> {
        Box::pin(async move {
            monoio::time::sleep(dur).await;
        })
    }

    fn now(&self) -> std::time::Instant {
        std::time::Instant::now()
    }

    fn system_time(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
}

use core::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, time::Duration};

use fusio::executor::{Executor, Timer};
use futures_channel::oneshot;

use super::{LeaseHandle, LeaseStore};
use crate::types::{Error, Result};

/// Keeps a lease alive by issuing periodic heartbeats on a background task.
#[derive(Debug)]
pub struct LeaseKeeper {
    stop: Arc<AtomicBool>,
    done: Option<oneshot::Receiver<()>>,
}

impl LeaseKeeper {
    pub(crate) fn spawn<E, LS>(
        executor: Arc<E>,
        timer: Arc<dyn Timer + Send + Sync>,
        leases: LS,
        lease: LeaseHandle,
        ttl: Duration,
    ) -> Result<Self>
    where
        LS: LeaseStore + Clone + 'static,
        E: Executor + Timer + Send + Sync + 'static,
    {
        if ttl == Duration::from_millis(0) {
            return Err(Error::Unimplemented("lease keeper requires positive ttl"));
        }

        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = oneshot::channel();
        let worker = LeaseKeeperWorker {
            stop: stop.clone(),
            timer,
            leases,
            lease,
            ttl,
            done: Some(tx),
        };

        let handle = executor.spawn(async move {
            worker.run().await;
        });
        drop(handle);

        Ok(Self {
            stop,
            done: Some(rx),
        })
    }

    /// Signal the background heartbeat to stop and wait for it to release the lease.
    pub async fn shutdown(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(done) = self.done.take() {
            let _ = done.await;
        }
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

struct LeaseKeeperWorker<LS>
where
    LS: LeaseStore + Clone + 'static,
{
    stop: Arc<AtomicBool>,
    timer: Arc<dyn Timer + Send + Sync>,
    leases: LS,
    lease: LeaseHandle,
    ttl: Duration,
    done: Option<oneshot::Sender<()>>,
}

impl<LS> LeaseKeeperWorker<LS>
where
    LS: LeaseStore + Clone + 'static,
{
    async fn run(mut self) {
        let sleep_interval = heartbeat_interval(self.ttl);
        while !self.stop.load(Ordering::Relaxed) {
            let sleep = self.timer.sleep(sleep_interval);
            futures_util::pin_mut!(sleep);
            sleep.await;
            if self.stop.load(Ordering::Relaxed) {
                break;
            }

            match self.leases.heartbeat(&self.lease, self.ttl).await {
                Ok(()) => {}
                Err(Error::PreconditionFailed) => {
                    // Lease lost; stop heartbeating.
                    break;
                }
                Err(_) => {
                    // Best-effort: continue after next interval.
                    // TODO: add metrics/logging.
                }
            }
        }

        let _ = self.leases.release(self.lease).await;
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}

fn heartbeat_interval(ttl: Duration) -> Duration {
    let half = ttl / 2;
    if half.is_zero() {
        Duration::from_millis(100)
    } else {
        half.max(Duration::from_millis(100))
    }
}

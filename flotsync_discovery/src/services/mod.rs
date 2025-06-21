use std::time::Duration;

use crate::{Port, errors::*};
use async_trait::async_trait;
use snafu::prelude::*;
use tokio::sync::watch;

mod peer_announcement;
pub use peer_announcement::PeerAnnouncementService;

#[cfg(feature = "zeroconf")]
mod mdns_announcement;
pub use mdns_announcement::MdnsAnnouncementService;

#[derive(Debug)]
pub struct ServiceHandle {
    /// Identifies the service that this handle belongs to.
    pub label: &'static str,
    shutdown: watch::Sender<bool>,
    join: tokio::task::JoinHandle<Result<()>>,
}
impl ServiceHandle {
    pub async fn shutdown(self) -> Result<()> {
        if self.shutdown.send(true).is_err() {
            log::warn!(
                "The {} service referenced by this service handle was already dropped.",
                self.label
            );
            Ok(())
        } else {
            self.join.await.context(JoinSnafu)?
        }
    }
}

// struct ShutdownChannel {
//     channel: oneshot::Receiver<oneshot::Sender<Result<()>>>,
// }
// impl ShutdownChannel {
//     // async fn execute_shutdown<F>(self, thunk: F)
//     // where
//     //     F: FnOnce() -> Result<()>,
//     // {
//     //     let result = thunk();
//     //     if self.channel.send(result).is_err() {
//     //         log::warn!(
//     //             "Could not send shutdown result, because the service handle was already dropped."
//     //         );
//     //     }
//     // }
// }

// fn create_handle(label: &'static str) -> (ServiceHandle, ShutdownChannel) {
//      let (tx, rx) = watch::channel(false);
//     let handle = ServiceHandle {
//         label,
//         shutdown: tx,
//     };
//     let channel = ShutdownChannel { channel: rx };
//     (handle, channel)
// }

pub async fn start_service<Fut, F, S>(constructor: F, options: S::Options) -> Result<ServiceHandle>
where
    Fut: Future<Output = Result<S>> + Send,
    F: FnOnce(S::Options) -> Fut,
    S: Service + Send + 'static,
{
    let service_name = std::any::type_name::<S>();
    let mut service = constructor(options).await?;
    let (tx, mut rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = rx.changed() => {
                    if *rx.borrow() {
                        if let Err(e) = service.shutdown().await {
                            log::error!("Error during service '{service_name}' shutdown: {e}");
                        }
                        break;
                    }
                }
                res = service.run() => {
                    if let Err(e) = res {
                        log::error!("Error during service '{service_name}' execution: {e}");
                    }
                    // continue
                }
            }
        }
        Ok(())
    });
    Ok(ServiceHandle {
        label: service_name,
        shutdown: tx,
        join,
    })
}

/// A generic interface for a background kind of service.
#[async_trait]
pub trait Service {
    type Options;

    async fn run(&mut self) -> Result<()>;

    async fn shutdown(self) -> Result<()>;
}

/// A way to hold on to some arbitrary value until the service handles decides to shutdown.
pub struct ServiceWrapper<T> {
    inner: T,
}

impl<T> ServiceWrapper<T>
where
    T: Send + 'static,
{
    pub async fn setup(service: T) -> Result<Self> {
        Ok(Self { inner: service })
    }
}

#[async_trait]
impl<T> Service for ServiceWrapper<T>
where
    T: Send + 'static,
{
    type Options = T;

    async fn run(&mut self) -> Result<()> {
        tokio::time::interval(Duration::from_secs(60)).tick().await;
        Ok(())
    }

    async fn shutdown(self) -> Result<()> {
        drop(self.inner);
        Ok(())
    }
}

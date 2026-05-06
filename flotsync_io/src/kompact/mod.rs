//! Kompact-facing adapter layer for the raw `flotsync_io` driver.
//!
//! The Kompact surface intentionally differs by transport:
//! - UDP is exposed as a shared typed port capability.
//! - TCP is exposed as a manager/session actor model.

mod bridge;
mod driver_component;
mod listener;
mod session;
mod types;

use crate::errors::{Error, Result};
use ::kompact::{config_keys::system, prelude::*};
use std::{sync::mpsc, thread, time::Duration};

pub use bridge::{IoBridge, IoBridgeHandle};
pub use driver_component::IoDriverComponent;
pub use types::{
    ConfigureFailureReason,
    OpenFailureReason,
    OpenTcpListener,
    OpenTcpSession,
    OpenedTcpListener,
    OpenedTcpSession,
    PendingTcpSession,
    TcpListenerEvent,
    TcpListenerRef,
    TcpListenerRequest,
    TcpSessionEvent,
    TcpSessionEventTarget,
    TcpSessionRef,
    TcpSessionRequest,
    UdpIndication,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
    UdpSendResult,
    tagged_tcp_session_event_target,
};

#[cfg(test)]
mod tests;

/// Resolves one Kompact future into the crate-local result shape.
///
/// The Kompact `KFuture` can fail if the promise side disappears before replying. Inside this
/// adapter that always means the driver-side actor infrastructure became unavailable, so the
/// helper normalises that case into [`Error::DriverUnavailable`].
pub(super) async fn resolve_kfuture<T: Send>(future: KFuture<Result<T>>) -> Result<T> {
    match future.await {
        Ok(reply) => reply,
        Err(_) => Err(Error::DriverUnavailable),
    }
}

/// Shut one Kompact system down with a bounded wait.
///
/// When `force_kill` is `false`, the helper performs graceful shutdown via
/// [`KompactSystem::shutdown`]. When `force_kill` is `true`, it uses
/// [`KompactSystem::kill_system`] instead. In both cases the helper logs the
/// start and successful completion.
///
/// # Panics
///
/// Panics if shutdown fails, if the shutdown worker disconnects, or if shutdown does not finish
/// within `timeout`.
pub fn shutdown_system_bounded(system: KompactSystem, timeout: Duration, force_kill: bool) {
    if thread::panicking() {
        let _ = if force_kill {
            system.kill_system()
        } else {
            system.shutdown()
        };
        return;
    }

    let system_label = system
        .config()
        .read_or_default(&system::LABEL)
        .unwrap_or_else(|_| String::from("<unlabelled-kompact-system>"));
    let shutdown_kind = if force_kill {
        "forced kill"
    } else {
        "graceful shutdown"
    };
    log::debug!(
        "Starting {shutdown_kind} for Kompact system '{system_label}' with timeout {timeout:?}."
    );

    let (shutdown_tx, shutdown_rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let shutdown_result = if force_kill {
            system.kill_system()
        } else {
            system.shutdown()
        };
        let _ = shutdown_tx.send(shutdown_result);
    });

    match shutdown_rx.recv_timeout(timeout) {
        Ok(Ok(())) => {
            log::debug!("Completed {shutdown_kind} for Kompact system '{system_label}'.");
        }
        Ok(Err(error)) => {
            log::error!("Failed {shutdown_kind} for Kompact system '{system_label}': {error}");
            panic!("failed {shutdown_kind} for Kompact system '{system_label}': {error}");
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            log::error!(
                "Timed out during {shutdown_kind} for Kompact system '{system_label}' after {timeout:?}."
            );
            panic!(
                "timed out during {shutdown_kind} for Kompact system '{system_label}' after {timeout:?}"
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            log::error!(
                "Shutdown thread disconnected unexpectedly during {shutdown_kind} for Kompact system '{system_label}'."
            );
            panic!(
                "shutdown thread disconnected unexpectedly during {shutdown_kind} for Kompact system '{system_label}'"
            );
        }
    }
}

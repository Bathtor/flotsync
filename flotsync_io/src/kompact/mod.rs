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
use ::kompact::prelude::KFuture;

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
    TcpSessionRef,
    TcpSessionRequest,
    UdpIndication,
    UdpOpenRequestId,
    UdpPort,
    UdpRequest,
    UdpSendResult,
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

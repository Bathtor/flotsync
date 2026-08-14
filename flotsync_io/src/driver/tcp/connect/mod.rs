//! Outbound TCP connection initiation and completion monitoring.
//!
//! Normal connections use Mio readiness from initiation onwards. Windows connections with an
//! explicit local bind remain outside Mio until Winsock reports that the pending connect may have
//! completed; the shared TCP state machine then validates completion and registers the established
//! stream with Mio.

use crate::api::ConnectionId;
use mio::net::TcpStream as MioTcpStream;
use std::{
    io::{self, ErrorKind},
    net::SocketAddr,
    time::{Duration, Instant},
};

#[cfg(not(windows))]
mod portable;
#[cfg(windows)]
mod windows;

#[cfg(not(windows))]
use portable as platform;
#[cfg(windows)]
use windows as platform;

pub(super) use platform::ConnectCompletionMonitor;

/// How the driver should learn that one newly-started connection may have completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConnectCompletion {
    /// The connect syscall completed synchronously.
    Complete,
    /// Mio readiness will report possible completion.
    MioReadiness,
    /// The platform monitor must report possible completion before the stream enters Mio.
    Monitored,
}

/// Result of initiating one non-blocking outbound TCP connection.
#[derive(Debug)]
pub(super) struct StartedTcpConnect {
    pub(super) stream: MioTcpStream,
    pub(super) completion: ConnectCompletion,
}

/// Authoritative state observed after a completion source signalled possible progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TcpConnectStatus {
    /// The connection has not completed yet and must keep its existing completion source.
    Pending,
    /// The connection completed and the supplied address is the peer reported by the socket.
    Connected { peer_addr: SocketAddr },
    /// The connection or completion inspection failed.
    Failed { error_kind: ErrorKind },
}

/// Advisory result from the platform monitor before authoritative socket inspection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MonitoredConnectSignal {
    /// The platform has not reported completion yet.
    Pending,
    /// The platform reported possible success or failure; inspect the socket to decide which.
    Ready,
    /// The completion mechanism itself failed for this socket.
    Failed { error_kind: ErrorKind },
}

/// Starts a non-blocking TCP connection, optionally binding its exact local address first.
pub(super) fn start_tcp_connect(
    local_addr: Option<SocketAddr>,
    remote_addr: SocketAddr,
) -> io::Result<StartedTcpConnect> {
    let Some(local_addr) = local_addr else {
        let stream = MioTcpStream::connect(remote_addr)?;
        return Ok(StartedTcpConnect {
            stream,
            completion: ConnectCompletion::MioReadiness,
        });
    };

    platform::start_bound_tcp_connect(local_addr, remote_addr)
}

/// Inspects non-blocking TCP connect completion according to Mio's documented sequence.
///
/// Readiness is only advisory. A connection is established only after `take_error()` reports no
/// error and `peer_addr()` returns an address. Platform-specific in-progress errors from
/// `peer_addr()` leave the connection pending.
pub(super) fn inspect_tcp_connect(stream: &MioTcpStream) -> TcpConnectStatus {
    match stream.take_error() {
        Ok(Some(error)) | Err(error) => {
            return TcpConnectStatus::Failed {
                error_kind: error.kind(),
            };
        }
        Ok(None) => {}
    }

    match stream.peer_addr() {
        Ok(peer_addr) => TcpConnectStatus::Connected { peer_addr },
        Err(error) if platform::is_connect_pending_error(&error) => TcpConnectStatus::Pending,
        Err(error) => TcpConnectStatus::Failed {
            error_kind: error.kind(),
        },
    }
}

/// Common interface implemented by the target-specific pending-connect monitor.
pub(super) trait PendingConnectMonitor {
    /// Starts tracking one connection at `now`.
    fn track(&mut self, connection_id: ConnectionId, now: Instant);

    /// Stops tracking one connection. Missing ids are harmless no-ops.
    fn remove(&mut self, connection_id: ConnectionId);

    /// Restricts the next Mio poll to the monitor's next scheduled check when necessary.
    fn constrain_poll_timeout(
        &self,
        configured: Option<Duration>,
        now: Instant,
    ) -> Option<Duration>;

    /// Returns tracked ids whose sockets should be checked now.
    ///
    /// Calling this before the next deadline returns an empty list without rescheduling it.
    fn take_due(&mut self, now: Instant) -> Vec<ConnectionId>;

    /// Checks whether one monitored socket may have completed its connection attempt.
    fn probe(stream: &MioTcpStream) -> MonitoredConnectSignal;
}

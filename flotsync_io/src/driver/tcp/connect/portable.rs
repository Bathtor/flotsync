//! Portable explicitly-bound connection initiation.

use super::{ConnectCompletion, MonitoredConnectSignal, PendingConnectMonitor, StartedTcpConnect};
use crate::{api::ConnectionId, socket_support::socket_domain};
use mio::net::TcpStream as MioTcpStream;
use socket2::{Protocol, SockAddr, Socket, Type};
use std::{
    io::{self, ErrorKind},
    net::{SocketAddr, TcpStream as StdTcpStream},
    time::{Duration, Instant},
};

/// No-op monitor used where explicitly-bound pending connects are Mio-readable.
#[derive(Debug, Default)]
pub(in crate::driver::tcp) struct ConnectCompletionMonitor;

pub(super) fn start_bound_tcp_connect(
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
) -> io::Result<StartedTcpConnect> {
    let socket = Socket::new(
        socket_domain(remote_addr),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(local_addr))?;

    let completion = match socket.connect(&SockAddr::from(remote_addr)) {
        Ok(()) => ConnectCompletion::Complete,
        Err(error) if is_connect_pending_error(&error) => ConnectCompletion::MioReadiness,
        Err(error) => return Err(error),
    };
    let stream: StdTcpStream = socket.into();
    let stream = MioTcpStream::from_std(stream);
    Ok(StartedTcpConnect { stream, completion })
}

pub(super) fn is_connect_pending_error(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        ErrorKind::WouldBlock | ErrorKind::NotConnected
    ) {
        return true;
    }

    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::EINPROGRESS)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

impl PendingConnectMonitor for ConnectCompletionMonitor {
    fn track(&mut self, _connection_id: ConnectionId, _now: Instant) {}

    fn remove(&mut self, _connection_id: ConnectionId) {}

    fn constrain_poll_timeout(
        &self,
        configured: Option<Duration>,
        _now: Instant,
    ) -> Option<Duration> {
        configured
    }

    fn take_due(&mut self, _now: Instant) -> Vec<ConnectionId> {
        Vec::new()
    }

    fn probe(_stream: &MioTcpStream) -> MonitoredConnectSignal {
        MonitoredConnectSignal::Pending
    }
}

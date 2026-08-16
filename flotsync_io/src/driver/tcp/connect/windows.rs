//! Windows explicitly-bound connection initiation and completion monitoring.

use super::{ConnectCompletion, MonitoredConnectSignal, PendingConnectMonitor, StartedTcpConnect};
use crate::{api::ConnectionId, socket_support::socket_domain};
use mio::net::TcpStream as MioTcpStream;
use socket2::{Protocol, SockAddr, Socket, Type};
use std::{
    collections::BTreeSet,
    io::{self, ErrorKind},
    net::{SocketAddr, TcpStream as StdTcpStream},
    os::windows::io::AsRawSocket,
    time::{Duration, Instant},
};
use windows_sys::Win32::Networking::WinSock::{
    POLLWRNORM,
    SOCKET,
    SOCKET_ERROR,
    WSAEINPROGRESS,
    WSAEWOULDBLOCK,
    WSAGetLastError,
    WSAPOLLFD,
    WSAPoll,
};

/// Interval between completion checks while explicitly-bound Windows connects are pending.
const CONNECT_PROBE_INTERVAL: Duration = Duration::from_millis(10);

/// Tracks explicitly-bound Windows connections that have not entered Mio yet.
#[derive(Debug)]
pub(in crate::driver::tcp) struct ConnectCompletionMonitor {
    pending: BTreeSet<ConnectionId>,
    next_probe_at: Option<Instant>,
    probe_interval: Duration,
}

impl Default for ConnectCompletionMonitor {
    fn default() -> Self {
        Self {
            pending: BTreeSet::new(),
            next_probe_at: None,
            probe_interval: CONNECT_PROBE_INTERVAL,
        }
    }
}

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
        Err(error) if is_connect_pending_error(&error) => ConnectCompletion::Monitored,
        Err(error) => return Err(error),
    };
    let stream: StdTcpStream = socket.into();
    let stream = MioTcpStream::from_std(stream);
    Ok(StartedTcpConnect { stream, completion })
}

pub(super) fn is_connect_pending_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::WouldBlock | ErrorKind::NotConnected
    ) || matches!(error.raw_os_error(), Some(WSAEINPROGRESS | WSAEWOULDBLOCK))
}

impl PendingConnectMonitor for ConnectCompletionMonitor {
    fn track(&mut self, connection_id: ConnectionId, now: Instant) {
        if self.pending.insert(connection_id) && self.next_probe_at.is_none() {
            self.next_probe_at = Some(now);
        }
    }

    fn remove(&mut self, connection_id: ConnectionId) {
        self.pending.remove(&connection_id);
        if self.pending.is_empty() {
            self.next_probe_at = None;
        }
    }

    fn constrain_poll_timeout(
        &self,
        configured: Option<Duration>,
        now: Instant,
    ) -> Option<Duration> {
        let Some(deadline) = self.next_probe_at else {
            return configured;
        };
        let until_probe = deadline.saturating_duration_since(now);
        Some(configured.map_or(until_probe, |timeout| timeout.min(until_probe)))
    }

    fn take_due(&mut self, now: Instant) -> Vec<ConnectionId> {
        let Some(deadline) = self.next_probe_at else {
            return Vec::new();
        };
        if now < deadline {
            return Vec::new();
        }

        self.next_probe_at = Some(now + self.probe_interval);
        self.pending.iter().copied().collect()
    }

    fn probe(stream: &MioTcpStream) -> MonitoredConnectSignal {
        let socket = SOCKET::try_from(stream.as_raw_socket())
            .expect("Windows raw socket handle does not fit Winsock SOCKET");
        let mut poll_fd = WSAPOLLFD {
            fd: socket,
            events: POLLWRNORM,
            revents: 0,
        };
        // SAFETY: `poll_fd` describes one live socket and remains writable for the duration of the
        // zero-timeout status query.
        let result = unsafe { WSAPoll(&raw mut poll_fd, 1, 0) };
        if result == SOCKET_ERROR {
            return MonitoredConnectSignal::Failed {
                error_kind: last_winsock_error().kind(),
            };
        }
        if result == 0 || poll_fd.revents == 0 {
            MonitoredConnectSignal::Pending
        } else {
            MonitoredConnectSignal::Ready
        }
    }
}

/// Returns the most recent Winsock error for the current thread.
fn last_winsock_error() -> io::Error {
    // SAFETY: `WSAGetLastError` has no preconditions and returns thread-local Winsock state.
    let error_code = unsafe { WSAGetLastError() };
    io::Error::from_raw_os_error(error_code)
}

/// Windows connect-monitor scheduling tests.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_preserves_configured_timeout_without_pending_connects() {
        let monitor = ConnectCompletionMonitor::default();
        let now = Instant::now();
        let configured = Duration::from_millis(25);

        assert_eq!(
            monitor.constrain_poll_timeout(Some(configured), now),
            Some(configured)
        );
        assert_eq!(monitor.constrain_poll_timeout(None, now), None);
    }

    #[test]
    fn monitor_deadline_survives_early_checks_and_clears_after_removal() {
        let mut monitor = ConnectCompletionMonitor::default();
        let connection_id = ConnectionId(3);
        let now = Instant::now();
        monitor.track(connection_id, now);

        assert_eq!(
            monitor.constrain_poll_timeout(None, now),
            Some(Duration::ZERO)
        );
        assert_eq!(monitor.take_due(now), vec![connection_id]);

        let early = now + Duration::from_millis(4);
        assert_eq!(monitor.take_due(early), Vec::new());
        assert_eq!(
            monitor.constrain_poll_timeout(None, early),
            Some(Duration::from_millis(6))
        );

        let due = now + CONNECT_PROBE_INTERVAL;
        assert_eq!(monitor.take_due(due), vec![connection_id]);

        monitor.remove(connection_id);
        let configured = Duration::from_millis(25);
        assert_eq!(
            monitor.constrain_poll_timeout(Some(configured), due),
            Some(configured)
        );
    }

    #[test]
    fn monitor_keeps_earlier_configured_timeout_and_tracks_until_last_removal() {
        let mut monitor = ConnectCompletionMonitor::default();
        let first = ConnectionId(2);
        let second = ConnectionId(7);
        let now = Instant::now();
        monitor.track(first, now);
        monitor.track(second, now);
        monitor.take_due(now);

        let configured = Duration::from_millis(3);
        assert_eq!(
            monitor.constrain_poll_timeout(Some(configured), now),
            Some(configured)
        );

        monitor.remove(first);
        assert_eq!(
            monitor.constrain_poll_timeout(None, now),
            Some(CONNECT_PROBE_INTERVAL)
        );
        monitor.remove(second);
        assert_eq!(monitor.constrain_poll_timeout(None, now), None);
    }
}

//! Mailbox messages for the shared Kompact driver component.

use super::*;

/// Internal mailbox for the shared Kompact driver component.
#[doc(hidden)]
#[derive(Debug)]
pub enum IoDriverComponentMessage {
    ReserveUdpSocket(Ask<ActorRefStrong<IoBridgeMessage>, Result<SocketId>>),
    ReleaseUdpSocket(Ask<SocketId, Result<()>>),
    OpenTcpListener(Ask<OpenTcpListenerRegistration, Result<ListenerId>>),
    ReleaseTcpListener(Ask<ListenerId, Result<()>>),
    OpenTcpSession(Ask<OpenTcpSessionRegistration, Result<ConnectionId>>),
    ReleaseTcpSession(Ask<ConnectionId, Result<()>>),
    AdoptAcceptedTcpSession(Ask<AdoptAcceptedTcpSessionRegistration, Result<()>>),
    RejectPendingTcpSession(Ask<ConnectionId, Result<()>>),
    DispatchUdp(UdpCommand),
    DispatchTcp(TcpCommand),
    DriverEvent(DriverEvent),
}

/// Registration payload used while opening one inbound TCP listener.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenTcpListenerRegistration {
    pub(crate) listener: ActorRefStrong<TcpListenerMessage>,
    pub(crate) local_addr: SocketAddr,
}

/// Registration payload used while opening one outbound TCP session.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenTcpSessionRegistration {
    pub(crate) session: ActorRefStrong<TcpSessionMessage>,
    pub(crate) remote_addr: SocketAddr,
    pub(crate) local_addr: Option<SocketAddr>,
}

/// Registration payload used while adopting one previously accepted inbound TCP connection.
#[doc(hidden)]
#[derive(Debug)]
pub struct AdoptAcceptedTcpSessionRegistration {
    pub(crate) connection_id: ConnectionId,
    pub(crate) session: ActorRefStrong<TcpSessionMessage>,
}

//! Raw transport command and event surfaces shared by the driver core and adapter layers.

use crate::pool::IoLease;
use bytes::Bytes;
use std::{
    fmt,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
};

/// Driver-local handle for a listening TCP socket.
///
/// This wraps a runtime-owned `usize` identifier. It is valid only inside the local
/// `KompactSystem` and must not be treated as a stable wire or persistence identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ListenerId(pub usize);

/// Driver-local handle for a TCP connection.
///
/// This wraps a runtime-owned `usize` identifier. It is valid only inside the local
/// `KompactSystem` and must not be treated as a stable wire or persistence identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConnectionId(pub usize);

/// Driver-local handle for a UDP socket.
///
/// This wraps a runtime-owned `usize` identifier. It is valid only inside the local
/// `KompactSystem` and must not be treated as a stable wire or persistence identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SocketId(pub usize);

/// Driver-local handle used to correlate a write request with its eventual outcome.
///
/// This wraps a runtime-owned `usize` identifier. It is valid only inside the local
/// `KompactSystem` and must not be treated as a stable wire or persistence identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TransmissionId(pub usize);

macro_rules! impl_local_id_display {
    ($id_type:ident, $label:literal) => {
        impl fmt::Display for $id_type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($label, "#{:x}"), self.0)
            }
        }
    };
}

impl_local_id_display!(ListenerId, "listener");
impl_local_id_display!(ConnectionId, "connection");
impl_local_id_display!(SocketId, "socket");
impl_local_id_display!(TransmissionId, "tx");

/// Conservative UDP payload budget used by the current driver and default pool sizing.
///
/// `1472` bytes fits within a typical Ethernet MTU of `1500` without IPv4 fragmentation
/// (`1500 - 20 byte IPv4 header - 8 byte UDP header`).
pub const MAX_UDP_PAYLOAD_BYTES: usize = 1472;

/// Payload container for freeform I/O operations.
///
/// v1 keeps both a lease-backed fast path and an owned-bytes compatibility path. Lease-backed
/// payloads share their underlying memory across clones and use independent read cursors when
/// consumed.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum IoPayload {
    /// Uses an immutable lease-backed payload for low-copy transport.
    Lease(IoLease),
    /// Uses owned byte storage for compatibility with callers outside the pool-based path.
    Bytes(Bytes),
}

impl IoPayload {
    /// Returns the total readable payload length in bytes.
    pub fn len(&self) -> usize {
        match self {
            Self::Lease(lease) => lease.len(),
            Self::Bytes(bytes) => bytes.len(),
        }
    }

    /// Returns whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Explains why a requested send could not be accepted or completed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendFailureReason {
    /// The driver rejected the send because write-side capacity is currently exhausted.
    Backpressure,
    /// The target socket or connection was already closed when the send was processed.
    Closed,
    /// The target socket or connection is not in a state where the requested send is valid.
    InvalidState,
    /// An unconnected UDP socket was asked to send without an explicit datagram target.
    MissingTargetForUnconnectedSocket,
    /// A connected UDP socket was asked to send with an explicit datagram target.
    UnexpectedTargetForConnectedSocket,
    /// The payload exceeds the maximum datagram size currently supported by the transport path.
    MessageTooLarge,
    /// The operating system rejected the send for a transport-specific reason other than closure.
    IoError,
    /// The shared driver was unavailable, for example during startup failure or shutdown.
    DriverUnavailable,
}

impl From<&io::Error> for SendFailureReason {
    fn from(error: &io::Error) -> Self {
        match error.kind() {
            io::ErrorKind::WouldBlock => Self::Backpressure,
            io::ErrorKind::NotConnected => Self::InvalidState,
            io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset => Self::Closed,
            _ => Self::IoError,
        }
    }
}

/// Explains why a transport endpoint transitioned into a closed state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseReason {
    /// The endpoint completed an orderly close.
    Graceful,
    /// The endpoint was terminated abruptly rather than drained gracefully.
    Aborted,
    /// The endpoint was closed because the owning driver is shutting down.
    DriverShutdown,
}

/// Explains why a UDP socket transitioned into a closed state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UdpCloseReason {
    /// The socket was closed because local code requested closure explicitly.
    Requested,
    /// The connected UDP peer became unreachable and the platform surfaced that via
    /// `ConnectionRefused` on the socket.
    Disconnected,
}

/// Describes how the driver should choose the local bind address for an unconnected UDP socket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UdpLocalBind {
    /// Bind exactly the supplied local socket address.
    Exact(SocketAddr),
    /// Choose a suitable ephemeral local bind address for future traffic to the supplied peer.
    ///
    /// This keeps the socket unconnected, but lets the driver apply transport-specific policy
    /// when selecting the local address family and interface affinity. The resulting concrete
    /// local address is still reported through [`UdpEvent::Bound`].
    ForPeer(SocketAddr),
}

impl UdpLocalBind {
    /// Resolves this bind policy into the concrete local address the raw driver will attempt.
    ///
    /// This is primarily useful for callers that need to predict the local bind address reported
    /// by failure paths such as [`UdpEvent::BindFailed`] before the socket is actually opened.
    ///
    /// The resolution is platform-sensitive. In particular, loopback peers resolve to loopback
    /// ephemeral local addresses, while other peers resolve to wildcard ephemeral local addresses
    /// in the same address family.
    pub fn resolve_local_addr(self) -> SocketAddr {
        match self {
            Self::Exact(local_addr) => local_addr,
            Self::ForPeer(peer_addr) => {
                if peer_addr.ip().is_loopback() {
                    match peer_addr {
                        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
                    }
                } else {
                    match peer_addr {
                        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
                        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
                    }
                }
            }
        }
    }
}

/// Commands issued against the TCP freeform I/O surface.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TcpCommand {
    /// Initiates an outbound TCP connection associated with the provided local handle.
    Connect {
        connection_id: ConnectionId,
        local_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
    },
    /// Binds and starts listening on a local TCP socket associated with the provided listener handle.
    Listen {
        listener_id: ListenerId,
        local_addr: SocketAddr,
    },
    /// Activates a previously accepted inbound TCP connection after its owning adapter installed
    /// the session routing needed to receive future events.
    ///
    /// Accepted connections start in a paused state so inbound bytes cannot race ahead of their
    /// eventual Kompact session owner.
    AdoptAccepted { connection_id: ConnectionId },
    /// Rejects a previously accepted inbound TCP connection before any session adopts it.
    ///
    /// This closes the pending connection and releases its local driver-owned resources without
    /// transitioning it into a normal session event stream.
    RejectAccepted { connection_id: ConnectionId },
    /// Requests transmission of raw payload bytes on an established TCP connection.
    Send {
        connection_id: ConnectionId,
        transmission_id: TransmissionId,
        payload: IoPayload,
    },
    /// Closes a TCP listener and releases its driver-owned resources.
    CloseListener { listener_id: ListenerId },
    /// Closes a TCP connection; `abort = true` requests an abortive close, `false` a graceful close.
    Close {
        connection_id: ConnectionId,
        abort: bool,
    },
}

/// Events emitted from the TCP freeform I/O surface.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TcpEvent {
    /// Reports that an outbound TCP connection became established.
    Connected {
        connection_id: ConnectionId,
        peer_addr: SocketAddr,
    },
    /// Reports that an outbound TCP connection attempt failed before the stream became usable.
    ConnectFailed {
        connection_id: ConnectionId,
        remote_addr: SocketAddr,
        error_kind: io::ErrorKind,
    },
    /// Reports that a requested listener bind/listen step failed before the listening socket became
    /// usable.
    ListenFailed {
        listener_id: ListenerId,
        local_addr: SocketAddr,
        error_kind: io::ErrorKind,
    },
    /// Reports that a requested TCP listener is now bound and accepting connections.
    Listening {
        listener_id: ListenerId,
        local_addr: SocketAddr,
    },
    /// Reports that a listener accepted a new inbound TCP connection.
    ///
    /// The accepted connection is still paused at this point. Adapters must decide whether to
    /// [`TcpCommand::AdoptAccepted`] or [`TcpCommand::RejectAccepted`] it before any session I/O
    /// begins.
    Accepted {
        listener_id: ListenerId,
        connection_id: ConnectionId,
        peer_addr: SocketAddr,
    },
    /// Delivers inbound payload bytes received on a TCP connection.
    Received {
        connection_id: ConnectionId,
        payload: IoPayload,
    },
    /// Confirms that a previously requested send on the identified TCP connection completed.
    SendAck {
        connection_id: ConnectionId,
        transmission_id: TransmissionId,
    },
    /// Reports that a previously requested send could not be accepted or completed.
    SendNack {
        connection_id: ConnectionId,
        transmission_id: TransmissionId,
        reason: SendFailureReason,
    },
    /// Reports that read interest was disabled because inbound buffer capacity was exhausted.
    ReadSuspended { connection_id: ConnectionId },
    /// Reports that read interest was re-enabled after inbound buffer capacity returned.
    ReadResumed { connection_id: ConnectionId },
    /// Reports that write-side progress is suspended because the connection cannot currently accept sends.
    WriteSuspended { connection_id: ConnectionId },
    /// Reports that write-side progress resumed and sends may be attempted again.
    WriteResumed { connection_id: ConnectionId },
    /// Reports that the listener closed and released its driver-owned state.
    ListenerClosed { listener_id: ListenerId },
    /// Reports that the TCP connection closed and explains the close mode.
    Closed {
        connection_id: ConnectionId,
        reason: CloseReason,
    },
}

/// Shared UDP socket-configuration options.
///
/// Shared socket-configuration changes are modeled explicitly via [`UdpCommand::Configure`]
/// because they affect the capability exposed by one local socket rather than one particular
/// datagram send operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UdpSocketOption {
    /// Enables or disables IPv4 broadcast transmission on the socket.
    ///
    /// This is required before sending to IPv4 broadcast destinations such as
    /// `255.255.255.255` or per-interface subnet broadcast addresses.
    Broadcast(bool),
    /// Enables or disables IPv4 multicast loopback.
    ///
    /// When enabled, datagrams sent by this socket to an IPv4 multicast group may also be
    /// received back locally if the socket joined that group.
    MulticastLoopV4(bool),
    /// Enables or disables IPv6 multicast loopback.
    ///
    /// When enabled, datagrams sent by this socket to an IPv6 multicast group may also be
    /// received back locally if the socket joined that group.
    MulticastLoopV6(bool),
    /// Sets the IPv4 multicast TTL used for future outbound multicast datagrams.
    ///
    /// This affects only IPv4 multicast sends, not ordinary unicast UDP traffic.
    MulticastTtlV4(u32),
    /// Sets the IPv6 multicast hop limit used for future outbound multicast datagrams.
    ///
    /// This affects only IPv6 multicast sends, not ordinary unicast UDP traffic.
    MulticastHopsV6(u32),
    /// Selects the IPv4 interface used for future outbound multicast datagrams.
    ///
    /// The supplied address must identify a local IPv4 interface on the current host.
    MulticastInterfaceV4(Ipv4Addr),
    /// Selects the IPv6 interface used for future outbound multicast datagrams.
    ///
    /// The supplied value is the operating-system interface index used by IPv6 multicast socket
    /// APIs.
    MulticastInterfaceV6(u32),
    /// Joins one IPv4 multicast group on the specified local IPv4 interface.
    JoinMulticastV4 {
        group: Ipv4Addr,
        interface: Ipv4Addr,
    },
    /// Leaves one IPv4 multicast group on the specified local IPv4 interface.
    LeaveMulticastV4 {
        group: Ipv4Addr,
        interface: Ipv4Addr,
    },
    /// Joins one IPv6 multicast group on the specified local interface index.
    JoinMulticastV6 { group: Ipv6Addr, interface: u32 },
    /// Leaves one IPv6 multicast group on the specified local interface index.
    LeaveMulticastV6 { group: Ipv6Addr, interface: u32 },
}

/// Commands issued against the UDP freeform I/O surface.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum UdpCommand {
    /// Binds a UDP socket using the provided local socket handle.
    Bind {
        socket_id: SocketId,
        bind: UdpLocalBind,
    },
    /// Creates a connected UDP socket, optionally binding it first, and associates it with the
    /// provided local socket handle.
    Connect {
        socket_id: SocketId,
        remote_addr: SocketAddr,
        bind: UdpLocalBind,
    },
    /// Requests transmission of a UDP datagram, optionally to an explicit target for unconnected sockets.
    Send {
        socket_id: SocketId,
        transmission_id: TransmissionId,
        payload: IoPayload,
        target: Option<SocketAddr>,
    },
    /// Applies one shared socket-configuration change to an already open UDP socket.
    ///
    /// The requested configuration affects the socket capability itself. Success or failure is
    /// therefore reported through the shared UDP event stream rather than through a private reply
    /// path.
    Configure {
        socket_id: SocketId,
        option: UdpSocketOption,
    },
    /// Closes the UDP socket and releases its driver-owned resources.
    Close { socket_id: SocketId },
}

/// Events emitted from the UDP freeform I/O surface.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum UdpEvent {
    /// Reports that a requested UDP socket bind completed successfully.
    Bound {
        socket_id: SocketId,
        local_addr: SocketAddr,
    },
    /// Reports that a requested UDP socket bind failed before the socket became usable.
    BindFailed {
        socket_id: SocketId,
        local_addr: SocketAddr,
        error_kind: io::ErrorKind,
    },
    /// Reports that a requested UDP socket connect completed successfully.
    Connected {
        socket_id: SocketId,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    },
    /// Reports that a requested UDP socket connect failed before the socket became usable.
    ConnectFailed {
        socket_id: SocketId,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        error_kind: io::ErrorKind,
    },
    /// Delivers an inbound UDP datagram together with its source address.
    Received {
        socket_id: SocketId,
        source: SocketAddr,
        payload: IoPayload,
    },
    /// Confirms that a previously requested datagram send on the identified UDP socket completed.
    SendAck {
        socket_id: SocketId,
        transmission_id: TransmissionId,
    },
    /// Reports that a previously requested datagram send could not be accepted or completed.
    SendNack {
        socket_id: SocketId,
        transmission_id: TransmissionId,
        reason: SendFailureReason,
    },
    /// Reports that one shared UDP socket option was applied successfully.
    Configured {
        socket_id: SocketId,
        option: UdpSocketOption,
    },
    /// Reports that a requested shared UDP socket option could not be applied.
    ConfigureFailed {
        socket_id: SocketId,
        option: UdpSocketOption,
        error_kind: io::ErrorKind,
    },
    /// Reports that read interest was disabled because inbound buffer capacity was exhausted.
    ReadSuspended { socket_id: SocketId },
    /// Reports that read interest was re-enabled after inbound buffer capacity returned.
    ReadResumed { socket_id: SocketId },
    /// Reports that write-side progress is suspended because the socket cannot currently accept sends.
    WriteSuspended { socket_id: SocketId },
    /// Reports that write-side progress resumed and sends may be attempted again.
    WriteResumed { socket_id: SocketId },
    /// Reports that the UDP socket closed and released its driver-owned state.
    Closed {
        socket_id: SocketId,
        remote_addr: Option<SocketAddr>,
        reason: UdpCloseReason,
    },
}

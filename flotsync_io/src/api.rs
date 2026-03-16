//! Typed command and event surfaces shared between the driver core and Kompact bridges.

use crate::pool::IoLease;
use ::kompact::prelude::Port;
use bytes::Bytes;
use std::{fmt, io, net::SocketAddr};

/// Typed Kompact port for TCP-oriented freeform I/O.
#[derive(Clone, Copy, Debug)]
pub struct TcpPort;

impl Port for TcpPort {
    type Request = TcpCommand;
    type Indication = TcpEvent;
}

/// Typed Kompact port for UDP-oriented freeform I/O.
#[derive(Clone, Copy, Debug)]
pub struct UdpPort;

impl Port for UdpPort {
    type Request = UdpCommand;
    type Indication = UdpEvent;
}

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

/// Commands issued against the TCP freeform I/O surface.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TcpCommand {
    /// Initiates an outbound TCP connection associated with the provided local handle.
    Connect {
        connection_id: ConnectionId,
        remote_addr: SocketAddr,
    },
    /// Binds and starts listening on a local TCP socket associated with the provided listener handle.
    Listen {
        listener_id: ListenerId,
        local_addr: SocketAddr,
    },
    /// Requests transmission of raw payload bytes on an established TCP connection.
    Send {
        connection_id: ConnectionId,
        transmission_id: TransmissionId,
        payload: IoPayload,
    },
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
    /// Reports that a requested TCP listener is now bound and accepting connections.
    Listening {
        listener_id: ListenerId,
        local_addr: SocketAddr,
    },
    /// Reports that a listener accepted a new inbound TCP connection that it now owns.
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
    /// Confirms that a previously requested send was accepted and completed.
    SendAck { transmission_id: TransmissionId },
    /// Reports that a previously requested send could not be accepted or completed.
    SendNack {
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
    /// Reports that the TCP connection closed and explains the close mode.
    Closed {
        connection_id: ConnectionId,
        reason: CloseReason,
    },
}

/// Commands issued against the UDP freeform I/O surface.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum UdpCommand {
    /// Binds a UDP socket to a local address using the provided local socket handle.
    Bind {
        socket_id: SocketId,
        local_addr: SocketAddr,
    },
    /// Creates a connected UDP socket, optionally binding it first, and associates it with the
    /// provided local socket handle.
    Connect {
        socket_id: SocketId,
        remote_addr: SocketAddr,
        local_addr: Option<SocketAddr>,
    },
    /// Requests transmission of a UDP datagram, optionally to an explicit target for unconnected sockets.
    Send {
        socket_id: SocketId,
        transmission_id: TransmissionId,
        payload: IoPayload,
        target: Option<SocketAddr>,
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
        local_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
        error_kind: io::ErrorKind,
    },
    /// Delivers an inbound UDP datagram together with its source address.
    Received {
        socket_id: SocketId,
        source: SocketAddr,
        payload: IoPayload,
    },
    /// Confirms that a previously requested datagram send was accepted and completed.
    SendAck { transmission_id: TransmissionId },
    /// Reports that a previously requested datagram send could not be accepted or completed.
    SendNack {
        transmission_id: TransmissionId,
        reason: SendFailureReason,
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
    Closed { socket_id: SocketId },
}

//! Typed command and event surfaces shared between the driver core and Kompact bridges.

use ::kompact::prelude::{ChunkLease, Port};
use bytes::Bytes;
use std::net::SocketAddr;

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

/// Payload container for freeform I/O operations.
///
/// v1 keeps both a lease-backed fast path and an owned-bytes compatibility path.
///
/// Cloning a lease-backed payload materialises it into [`bytes::Bytes`] because
/// [`ChunkLease`] itself is uniquely owned while Kompact port traffic must be cloneable.
#[derive(Debug)]
#[non_exhaustive]
pub enum IoPayload {
    /// Uses a Kompact-managed pooled buffer lease for zero-copy or low-copy transport.
    Lease(ChunkLease),
    /// Uses owned byte storage for compatibility with callers outside the pool-based path.
    Bytes(Bytes),
}

impl Clone for IoPayload {
    fn clone(&self) -> Self {
        match self {
            Self::Lease(chunk) => Self::Bytes(chunk.create_byte_clone()),
            Self::Bytes(bytes) => Self::Bytes(bytes.clone()),
        }
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
    /// The shared driver was unavailable, for example during startup failure or shutdown.
    DriverUnavailable,
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

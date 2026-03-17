use super::session::TcpSessionMessage;
use crate::api::{CloseReason, IoPayload, SendFailureReason, SocketId, TransmissionId};
use ::kompact::prelude::{ActorRefStrong, Port, Receiver, Recipient};
use std::{io, net::SocketAddr};

/// Describes why an open-style operation could not make the requested socket or session usable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenFailureReason {
    /// The operating system rejected the operation while touching the real socket.
    Io(io::ErrorKind),
    /// The caller referred to a local handle that is not owned by this bridge or no longer live.
    InvalidHandle,
    /// The shared driver infrastructure was unavailable before the operation could run.
    DriverUnavailable,
}

/// Shared Kompact UDP capability port.
///
/// This port represents a shared local UDP capability rather than an owned session. Multiple
/// local components may connect to the same bridge and observe the same socket lifecycle and
/// receive indications.
#[derive(Clone, Copy, Debug)]
pub struct UdpPort;

impl Port for UdpPort {
    type Request = UdpRequest;
    type Indication = UdpIndication;
}

/// Requests that can be issued against the Kompact-facing UDP bridge.
#[derive(Clone, Debug)]
pub enum UdpRequest {
    /// Binds the identified local UDP socket handle to the provided local address.
    Bind {
        socket_id: SocketId,
        local_addr: SocketAddr,
    },
    /// Creates or reconfigures the identified UDP socket as a connected UDP socket.
    Connect {
        socket_id: SocketId,
        remote_addr: SocketAddr,
        local_addr: Option<SocketAddr>,
    },
    /// Sends one datagram through the identified UDP socket.
    ///
    /// `reply_to` receives the send outcome privately for this send operation only. Send outcomes
    /// are intentionally not broadcast on the shared UDP port, because that would leak unrelated
    /// transmission completion to every local consumer of the same bridge.
    Send {
        socket_id: SocketId,
        transmission_id: TransmissionId,
        payload: IoPayload,
        target: Option<SocketAddr>,
        reply_to: Recipient<UdpSendResult>,
    },
    /// Closes the identified UDP socket and releases its driver-owned resources.
    Close { socket_id: SocketId },
}

/// Indications emitted on the shared Kompact UDP port.
///
/// These indications represent shared socket state and inbound traffic. They are broadcast to all
/// local components connected to the same bridge. Per-send completion is delivered separately via
/// [`UdpSendResult`].
#[derive(Clone, Debug)]
pub enum UdpIndication {
    /// Reports that the socket was bound successfully.
    ///
    /// Bind/connect failures are reported as port indications because they affect the
    /// shared socket capability itself rather than one specific send request.
    Bound {
        socket_id: SocketId,
        local_addr: SocketAddr,
    },
    /// Reports that a requested bind could not make the socket usable.
    BindFailed {
        socket_id: SocketId,
        local_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    /// Reports that the socket was connected successfully.
    Connected {
        socket_id: SocketId,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    },
    /// Reports that a requested connect could not make the socket usable.
    ConnectFailed {
        socket_id: SocketId,
        local_addr: Option<SocketAddr>,
        remote_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    /// Delivers one inbound UDP datagram.
    Received {
        socket_id: SocketId,
        source: SocketAddr,
        payload: IoPayload,
    },
    /// Reports that read interest was disabled because ingress capacity was exhausted.
    ReadSuspended { socket_id: SocketId },
    /// Reports that read interest was re-enabled after ingress capacity returned.
    ReadResumed { socket_id: SocketId },
    /// Reports that write-side progress is currently suspended for the socket.
    WriteSuspended { socket_id: SocketId },
    /// Reports that write-side progress resumed for the socket.
    WriteResumed { socket_id: SocketId },
    /// Reports that the socket closed and released its driver-owned state.
    Closed { socket_id: SocketId },
}

/// Private reply channel outcome for one UDP send operation.
#[derive(Clone, Debug)]
pub enum UdpSendResult {
    /// Confirms that the requested datagram was accepted and sent.
    ///
    /// These results are delivered only to the `reply_to` recipient supplied on the matching
    /// [`UdpRequest::Send`] and are never broadcast on the shared UDP port.
    Ack {
        socket_id: SocketId,
        transmission_id: TransmissionId,
    },
    /// Reports that the requested datagram could not be accepted or completed.
    Nack {
        socket_id: SocketId,
        transmission_id: TransmissionId,
        reason: SendFailureReason,
    },
}

/// Request used with the TCP manager side of an [`IoBridge`](super::IoBridge).
///
/// Sending this request asks the bridge to allocate one outbound TCP session endpoint, start an
/// asynchronous connect attempt, and route future session events to `events_to`.
#[derive(Clone, Debug)]
pub struct OpenTcpSession {
    /// Remote TCP peer address to connect to.
    pub remote_addr: SocketAddr,
    /// Optional local bind address to use before connecting.
    pub local_addr: Option<SocketAddr>,
    /// Recipient that will receive all lifecycle, read, and send-completion events for the session.
    pub events_to: Recipient<TcpSessionEvent>,
}

/// Requests accepted by one Kompact TCP session endpoint.
#[derive(Clone, Debug)]
pub enum TcpSessionRequest {
    /// Sends raw payload bytes on this session.
    Send {
        transmission_id: TransmissionId,
        payload: IoPayload,
    },
    /// Closes the session; `abort = true` requests an abortive close.
    Close { abort: bool },
}

/// Events emitted for one Kompact TCP session.
#[derive(Clone, Debug)]
pub enum TcpSessionEvent {
    /// Reports that the outbound connect attempt completed successfully.
    Connected { peer_addr: SocketAddr },
    /// Reports that the outbound connect attempt failed before the session became usable.
    ConnectFailed {
        remote_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    /// Delivers inbound TCP bytes for this session.
    Received { payload: IoPayload },
    /// Confirms that a previously requested send completed.
    SendAck { transmission_id: TransmissionId },
    /// Reports that a previously requested send could not be accepted or completed.
    SendNack {
        transmission_id: TransmissionId,
        reason: SendFailureReason,
    },
    /// Reports that read interest was disabled because ingress capacity was exhausted.
    ReadSuspended,
    /// Reports that read interest was re-enabled after ingress capacity returned.
    ReadResumed,
    /// Reports that write-side progress is currently suspended.
    WriteSuspended,
    /// Reports that write-side progress resumed.
    WriteResumed,
    /// Reports that the session reached a terminal closed state.
    Closed { reason: CloseReason },
}

/// Strong request endpoint for one Kompact TCP session.
///
/// The bridge returns this handle when opening a TCP session. All requests sent through it are
/// implicitly associated with that session; callers do not have to keep or repeat a
/// `ConnectionId`.
///
/// The handle stores a strong actor reference internally so repeated session sends avoid the extra
/// indirection cost of a [`Recipient`]. This does not, by itself, define session cleanup
/// semantics; session lifetime remains controlled by explicit close and component lifecycle.
#[derive(Clone, Debug)]
pub struct TcpSessionRef {
    actor: ActorRefStrong<TcpSessionMessage>,
}

impl TcpSessionRef {
    pub(crate) fn new(actor: ActorRefStrong<TcpSessionMessage>) -> Self {
        Self { actor }
    }

    /// Sends one request to the underlying TCP session endpoint.
    pub fn tell(&self, request: TcpSessionRequest) {
        self.actor.tell(TcpSessionMessage::Request(request));
    }

    /// Returns a narrowed recipient for this session.
    ///
    /// This is provided for integration points that specifically require a `Recipient`, but it
    /// pays the normal Kompact recipient adapter overhead on each use.
    pub fn recipient(&self) -> Recipient<TcpSessionRequest> {
        self.actor.recipient()
    }
}

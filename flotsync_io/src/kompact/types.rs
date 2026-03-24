use super::{
    listener::{AcceptPendingTcpSession, TcpListenerMessage},
    session::TcpSessionMessage,
};
use crate::{
    api::{
        CloseReason,
        ConnectionId,
        IoPayload,
        SendFailureReason,
        SocketId,
        TransmissionId,
        UdpCloseReason,
        UdpLocalBind,
        UdpSocketOption,
    },
    errors::Result,
    pool::{EgressAsyncWriter, EgressPool},
};
use ::kompact::prelude::{
    ActorRef,
    ActorRefFactory,
    ActorRefStrong,
    KFuture,
    MessageBounds,
    Port,
    Receiver,
    Recipient,
    promise,
};
use bytes::Bytes;
use std::{io, net::SocketAddr, ops::AsyncFnOnce, sync::Arc};
use uuid::Uuid;

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

/// Describes why a shared UDP socket-configuration request could not be applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigureFailureReason {
    /// The operating system rejected the configuration while touching the real socket.
    Io(io::ErrorKind),
    /// The caller referred to a local socket handle that is not owned by this bridge.
    InvalidHandle,
    /// The shared driver infrastructure was unavailable before the configuration could run.
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
    /// Opens one new unconnected UDP socket according to the provided bind policy.
    ///
    /// The driver assigns the resulting `SocketId` and reports it back on the shared UDP
    /// indication stream. `request_id` exists only so the original requester can match the
    /// eventual success or failure indication to this open attempt.
    Bind {
        request_id: UdpOpenRequestId,
        bind: UdpLocalBind,
    },
    /// Opens one new connected UDP socket.
    ///
    /// The driver assigns the resulting `SocketId` and reports it back on the shared UDP
    /// indication stream. `request_id` exists only so the original requester can match the
    /// eventual success or failure indication to this open attempt.
    Connect {
        request_id: UdpOpenRequestId,
        remote_addr: SocketAddr,
        bind: UdpLocalBind,
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
    /// Applies one shared UDP socket-configuration change.
    ///
    /// Configuration affects the shared socket capability rather than one send attempt, so success
    /// and failure are broadcast as [`UdpIndication`] values to every local consumer connected to
    /// this bridge.
    Configure {
        socket_id: SocketId,
        option: UdpSocketOption,
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
        request_id: UdpOpenRequestId,
        socket_id: SocketId,
        local_addr: SocketAddr,
    },
    /// Reports that a requested bind could not make the socket usable.
    BindFailed {
        request_id: UdpOpenRequestId,
        local_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    /// Reports that the socket was connected successfully.
    Connected {
        request_id: UdpOpenRequestId,
        socket_id: SocketId,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    },
    /// Reports that a requested connect could not make the socket usable.
    ConnectFailed {
        request_id: UdpOpenRequestId,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        reason: OpenFailureReason,
    },
    /// Delivers one inbound UDP datagram.
    Received {
        socket_id: SocketId,
        source: SocketAddr,
        payload: IoPayload,
    },
    /// Reports that one shared UDP socket option was applied successfully.
    Configured {
        socket_id: SocketId,
        option: UdpSocketOption,
    },
    /// Reports that one shared UDP socket option could not be applied.
    ConfigureFailed {
        socket_id: SocketId,
        option: UdpSocketOption,
        reason: ConfigureFailureReason,
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
    Closed {
        socket_id: SocketId,
        remote_addr: Option<SocketAddr>,
        reason: UdpCloseReason,
    },
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

/// Correlates one UDP open request with the later shared indication that reports its outcome.
///
/// UDP open requests are broadcast through a shared port, so Kompact does not preserve which local
/// component originated a given bind/connect operation. The requester therefore supplies one
/// globally unique correlation id and matches it against the echoed indication later on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct UdpOpenRequestId(pub Uuid);

impl UdpOpenRequestId {
    /// Allocates one fresh UDP open-request correlation id.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// Request used with the TCP listener manager side of an [`IoBridge`](super::IoBridge).
///
/// Sending this request asks the bridge to allocate one inbound TCP listener endpoint, start an
/// asynchronous listen attempt, and route future listener events to `incoming_to`.
#[derive(Clone, Debug)]
pub struct OpenTcpListener {
    /// Local TCP socket address to bind and listen on.
    pub local_addr: SocketAddr,
    /// Recipient that will receive listener lifecycle and pending incoming-session events.
    pub incoming_to: Recipient<TcpListenerEvent>,
}

/// Successful outcome of opening one TCP listener.
#[derive(Clone, Debug)]
pub struct OpenedTcpListener {
    /// Strong request endpoint for the newly opened listener.
    pub listener: TcpListenerRef,
    /// Concrete local address the listener is bound to.
    pub local_addr: SocketAddr,
}

/// Requests accepted by one Kompact TCP listener endpoint.
#[derive(Clone, Debug)]
pub enum TcpListenerRequest {
    /// Closes the listener and stops accepting new inbound connections.
    Close,
}

/// Events emitted for one Kompact TCP listener.
#[derive(Debug)]
pub enum TcpListenerEvent {
    /// Reports one newly accepted inbound TCP connection that is waiting for application logic to
    /// decide whether to accept or reject it.
    ///
    /// The contained [`PendingTcpSession`] is deliberately not cloneable. Exactly one decision
    /// owns the accepted socket: accept it into a session, reject it, or drop the handle and let
    /// it auto-reject.
    Incoming {
        peer_addr: SocketAddr,
        pending: PendingTcpSession,
    },
    /// Reports that the listener reached a terminal closed state.
    Closed,
}

/// Strong request endpoint for one Kompact TCP listener.
///
/// The bridge returns this handle when opening a listener. All requests sent through it are
/// implicitly associated with that listener; callers do not have to keep or repeat a
/// `ListenerId`.
#[derive(Clone, Debug)]
pub struct TcpListenerRef {
    actor: ActorRefStrong<TcpListenerMessage>,
}

impl TcpListenerRef {
    pub(crate) fn new(actor: ActorRefStrong<TcpListenerMessage>) -> Self {
        Self { actor }
    }

    /// Sends one request to the underlying TCP listener endpoint.
    pub fn tell(&self, request: TcpListenerRequest) {
        self.actor.tell(TcpListenerMessage::Request(request));
    }

    /// Returns a narrowed recipient for this listener.
    pub fn recipient(&self) -> Recipient<TcpListenerRequest> {
        self.actor.recipient()
    }
}

/// Pending inbound TCP session that still needs an application-level accept/reject decision.
///
/// The raw driver already accepted the operating-system socket, but it has not yet enabled normal
/// session I/O. Callers must either accept or reject the pending connection. Dropping the value
/// without making a decision triggers a best-effort reject so pending accepted sockets do not
/// linger forever.
#[derive(Debug)]
pub struct PendingTcpSession {
    inner: Option<PendingTcpSessionInner>,
}

#[derive(Debug)]
struct PendingTcpSessionInner {
    listener: ActorRefStrong<TcpListenerMessage>,
    connection_id: ConnectionId,
}

impl PendingTcpSession {
    pub(crate) fn new(
        listener: ActorRefStrong<TcpListenerMessage>,
        connection_id: ConnectionId,
    ) -> Self {
        Self {
            inner: Some(PendingTcpSessionInner {
                listener,
                connection_id,
            }),
        }
    }

    /// Accepts the inbound connection and attaches it to a new TCP session endpoint.
    ///
    /// `events_to` receives all future lifecycle and I/O events for the accepted session. The
    /// returned future resolves once the session actor exists, owns the accepted connection, and
    /// the raw driver has enabled normal session readiness.
    pub fn accept<R>(self, events_to: R) -> KFuture<Result<TcpSessionRef>>
    where
        R: Receiver<TcpSessionEvent>,
    {
        self.accept_target(TcpSessionEventTarget::from_receiver(events_to))
    }

    fn accept_target(mut self, events_to: TcpSessionEventTarget) -> KFuture<Result<TcpSessionRef>> {
        let inner = self.take_inner();
        let request = AcceptPendingTcpSession {
            connection_id: inner.connection_id,
            events_to,
        };
        let (promise, future) = promise::<Result<TcpSessionRef>>();
        inner.listener.tell(TcpListenerMessage::AcceptPending(
            ::kompact::prelude::Ask::new(promise, request),
        ));
        future
    }

    /// Rejects the inbound connection and releases its driver-owned resources.
    pub fn reject(mut self) -> KFuture<Result<()>> {
        let inner = self.take_inner();
        let (promise, future) = promise::<Result<()>>();
        inner.listener.tell(TcpListenerMessage::RejectPending(
            ::kompact::prelude::Ask::new(promise, inner.connection_id),
        ));
        future
    }

    /// Accepts the inbound connection and forwards future session events through a runtime tag.
    pub fn accept_tagged<R, M, Tag>(
        self,
        target: R,
        tag: Tag,
        wrap: fn(Tag, TcpSessionEvent) -> M,
    ) -> KFuture<Result<TcpSessionRef>>
    where
        R: ActorRefFactory<Message = M>,
        M: MessageBounds,
        Tag: Copy + Send + Sync + 'static,
    {
        self.accept_target(tagged_tcp_session_event_target(target, tag, wrap))
    }

    fn take_inner(&mut self) -> PendingTcpSessionInner {
        self.inner.take().unwrap_or_else(|| {
            panic!("pending TCP session decision handle may only be consumed once")
        })
    }
}

impl Drop for PendingTcpSession {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        inner
            .listener
            .tell(TcpListenerMessage::DropPending(inner.connection_id));
    }
}

/// Type-erased session-event delivery target used by the Kompact TCP adapter.
#[derive(Clone)]
pub struct TcpSessionEventTarget {
    inner: Arc<dyn DynTcpSessionEventTarget>,
}

impl std::fmt::Debug for TcpSessionEventTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TcpSessionEventTarget(..)")
    }
}

impl TcpSessionEventTarget {
    fn new(inner: impl DynTcpSessionEventTarget + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Builds one event target from a plain Kompact recipient.
    pub fn from_recipient(recipient: Recipient<TcpSessionEvent>) -> Self {
        Self::new(RecipientTcpSessionEventTarget { recipient })
    }

    /// Builds one event target from any local receiver of `TcpSessionEvent`.
    pub fn from_receiver<R>(receiver: R) -> Self
    where
        R: Receiver<TcpSessionEvent>,
    {
        Self::from_recipient(receiver.recipient())
    }

    pub(crate) fn tell(&self, event: TcpSessionEvent) {
        self.inner.tell(event);
    }
}

trait DynTcpSessionEventTarget: Send + Sync {
    fn tell(&self, event: TcpSessionEvent);
}

#[derive(Clone)]
struct RecipientTcpSessionEventTarget {
    recipient: Recipient<TcpSessionEvent>,
}

impl DynTcpSessionEventTarget for RecipientTcpSessionEventTarget {
    fn tell(&self, event: TcpSessionEvent) {
        self.recipient.tell(event);
    }
}

#[derive(Clone)]
struct TaggedActorRefTcpSessionEventTarget<M, Tag>
where
    M: MessageBounds,
    Tag: Copy + Send + Sync + 'static,
{
    actor: ActorRef<M>,
    tag: Tag,
    wrap: fn(Tag, TcpSessionEvent) -> M,
}

impl<M, Tag> DynTcpSessionEventTarget for TaggedActorRefTcpSessionEventTarget<M, Tag>
where
    M: MessageBounds,
    Tag: Copy + Send + Sync + 'static,
{
    fn tell(&self, event: TcpSessionEvent) {
        self.actor.tell((self.wrap)(self.tag, event));
    }
}

/// Builds one tagged TCP session-event target that wraps an actor ref directly.
///
/// Unlike Kompact's `Recipient`, this helper can carry one small `Copy` tag and combine it with
/// each forwarded `TcpSessionEvent` using the supplied `wrap` function before telling the actor.
pub fn tagged_tcp_session_event_target<A, M, Tag>(
    target: A,
    tag: Tag,
    wrap: fn(Tag, TcpSessionEvent) -> M,
) -> TcpSessionEventTarget
where
    A: ActorRefFactory<Message = M>,
    M: MessageBounds,
    Tag: Copy + Send + Sync + 'static,
{
    TcpSessionEventTarget::new(TaggedActorRefTcpSessionEventTarget {
        actor: target.actor_ref(),
        tag,
        wrap,
    })
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

/// Successful outcome of opening one outbound TCP session.
#[derive(Clone, Debug)]
pub struct OpenedTcpSession {
    /// Strong request endpoint for the newly opened TCP session.
    pub session: TcpSessionRef,
    /// Remote peer address for the established session.
    pub peer_addr: SocketAddr,
}

/// Requests accepted by one Kompact TCP session endpoint.
#[derive(Clone, Debug)]
pub enum TcpSessionRequest {
    /// Sends raw payload bytes on this session.
    Send {
        transmission_id: TransmissionId,
        payload: IoPayload,
    },
    /// Sends raw payload bytes and then closes the session gracefully after the send drains.
    SendAndClose {
        transmission_id: TransmissionId,
        payload: IoPayload,
    },
    /// Closes the session; `abort = true` requests an abortive close.
    Close { abort: bool },
}

/// Events emitted for one Kompact TCP session.
#[derive(Clone, Debug)]
pub enum TcpSessionEvent {
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
    egress_pool: EgressPool,
}

impl TcpSessionRef {
    pub(crate) fn new(actor: ActorRefStrong<TcpSessionMessage>, egress_pool: EgressPool) -> Self {
        Self { actor, egress_pool }
    }

    /// Sends one request to the underlying TCP session endpoint.
    pub fn tell(&self, request: TcpSessionRequest) {
        self.actor.tell(TcpSessionMessage::Request(request));
    }

    /// Returns the shared egress pool owned by the underlying raw driver instance.
    pub fn egress_pool(&self) -> &EgressPool {
        &self.egress_pool
    }

    /// Sends one payload on this session.
    pub fn send(&self, transmission_id: TransmissionId, payload: IoPayload) {
        self.tell(TcpSessionRequest::Send {
            transmission_id,
            payload,
        });
    }

    /// Sends one byte-backed payload on this session.
    pub fn send_bytes(&self, transmission_id: TransmissionId, bytes: Bytes) {
        self.send(transmission_id, IoPayload::Bytes(bytes));
    }

    /// Sends one static byte slice on this session.
    pub fn send_static(&self, transmission_id: TransmissionId, bytes: &'static [u8]) {
        self.send(transmission_id, IoPayload::from_static(bytes));
    }

    /// Serialises one payload through a growable async writer and sends it once the closure
    /// completes successfully.
    pub async fn send_with<T, F>(
        &self,
        transmission_id: TransmissionId,
        hint_bytes: Option<usize>,
        write: F,
    ) -> Result<T>
    where
        F: for<'a> AsyncFnOnce(&'a mut EgressAsyncWriter) -> Result<T>,
    {
        let mut writer = self.egress_pool.writer(hint_bytes);
        let value = write(&mut writer).await?;
        let payload = match writer.finish()? {
            Some(payload) => payload,
            None => IoPayload::Bytes(Bytes::new()),
        };
        self.send(transmission_id, payload);
        Ok(value)
    }

    /// Serialises one payload through a growable async writer, sends it, and then requests a
    /// graceful close once the send drains.
    pub async fn send_and_close_with<T, F>(
        &self,
        transmission_id: TransmissionId,
        hint_bytes: Option<usize>,
        write: F,
    ) -> Result<T>
    where
        F: for<'a> AsyncFnOnce(&'a mut EgressAsyncWriter) -> Result<T>,
    {
        let mut writer = self.egress_pool.writer(hint_bytes);
        let value = write(&mut writer).await?;
        let payload = match writer.finish()? {
            Some(payload) => payload,
            None => IoPayload::Bytes(Bytes::new()),
        };
        self.send_and_close(transmission_id, payload);
        Ok(value)
    }

    /// Sends one payload and requests a graceful close once the send drains.
    pub fn send_and_close(&self, transmission_id: TransmissionId, payload: IoPayload) {
        self.tell(TcpSessionRequest::SendAndClose {
            transmission_id,
            payload,
        });
    }

    /// Closes the session; `abort = true` requests an abortive close.
    pub fn close(&self, abort: bool) {
        self.tell(TcpSessionRequest::Close { abort });
    }

    /// Returns a narrowed recipient for this session.
    ///
    /// This is provided for integration points that specifically require a `Recipient`, but it
    /// pays the normal Kompact recipient adapter overhead on each use.
    pub fn recipient(&self) -> Recipient<TcpSessionRequest> {
        self.actor.recipient()
    }
}

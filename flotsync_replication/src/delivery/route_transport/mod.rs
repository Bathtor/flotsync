//! Route-transport boundary beneath semantic delivery.
//!
//! Discovery publishes route candidates whose concrete route key type is owned
//! by route transport rather than by semantic delivery.
//!
//! Semantic delivery only needs:
//!
//! - a comparable/hashable coverage key
//! - route sharing semantics
//! - an optional preference rank
//!
//! The concrete route key type is therefore generic in the delivery-facing
//! contracts below and concretised here for the current transport-manager
//! sketch.
//!
//! This module intentionally uses two different communication styles:
//!
//! - outbound route-transport submission is actor-based because each logical
//!   send has exactly one correlated result;
//! - inbound delivery is port-based because it is an unsolicited event stream
//!   produced by the network.

pub mod manager;

use super::shared::{RelayIdentity, RouteSendId};
use flotsync_core::MemberIdentity;
use flotsync_io::prelude::{IoPayload, SocketId};
use flotsync_messages::serialisation::FlotsyncSerializable;
use flotsync_utils::{IString, NonOwningPhantomData};
use kompact::{
    Never,
    prelude::{Ask, Port},
};
use snafu::Snafu;
use std::{fmt::Debug, hash::Hash, net::SocketAddr, sync::Arc};

/// Whether semantic delivery may collapse equal candidates into one shared send.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RouteSharingKind {
    /// Sends may not be collapsed. This route only handles a single target.
    Exclusive,
    /// Sends may be collapsed. This route may be able to handle more than one target.
    SharedCoverage,
}

/// Discovery-published route preference.
///
/// Higher valued routes are preferred over lower valued routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoutePreferenceRank(u8);

impl RoutePreferenceRank {
    pub const UNRANKED: Self = Self(0);

    #[must_use]
    pub fn new(value: u8) -> Self {
        Self(value)
    }

    /// No specific preference was provided.
    #[must_use]
    pub fn is_unranked(self) -> bool {
        self.0 == 0
    }
}

/// Datagram-like delivery scope for one concrete UDP route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DatagramRouteScope {
    Unicast,
    Broadcast,
    Multicast,
}

/// Concrete UDP route key owned by route transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UdpRouteKey {
    pub remote_addr: SocketAddr,
    pub scope: DatagramRouteScope,
    pub local_bind: Option<SocketAddr>,
}

/// Concrete TCP route key owned by route transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TcpRouteKey {
    pub remote_addr: SocketAddr,
    pub local_bind: Option<SocketAddr>,
}

/// Concrete route key used by the current transport-manager sketch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TransportRouteKey {
    Udp(UdpRouteKey),
    Tcp(TcpRouteKey),
}

/// Discovery-published route candidate.
///
/// `R` is the concrete coverage key type owned by route transport/discovery.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SendRouteCandidate<R> {
    pub coverage_key: R,
    pub sharing: RouteSharingKind,
    pub preference_rank: RoutePreferenceRank,
}

/// Published discovery update for one peer or relay.
///
/// Each update fully replaces the previously published route set for that
/// peer or relay. An empty `routes` list withdraws all currently usable routes
/// for that identity.
#[derive(Clone, Debug)]
pub enum DiscoveryRouteUpdate<R> {
    PeerRoutes {
        peer: MemberIdentity,
        routes: Vec<SendRouteCandidate<R>>,
    },
    RelayRoutes {
        relay: RelayIdentity,
        routes: Vec<SendRouteCandidate<R>>,
    },
}

/// Discovery port used to publish route updates into dependent components.
#[derive(Clone, Copy, Debug, Default)]
pub struct RouteDiscoveryPort<R>(NonOwningPhantomData<R>);

impl<R> Port for RouteDiscoveryPort<R>
where
    R: Clone + Debug + Eq + Hash + Send + Sync + 'static,
{
    type Request = Never;
    type Indication = DiscoveryRouteUpdate<R>;
}

/// Indications sent from route transport back to discovery about concrete route
/// health.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionInfoIndication<R> {
    ReportRouteFailed {
        route: R,
        reason: ConnectionFailureReason,
    },
}

/// Reasons why a concrete route should be downgraded or dropped by discovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionFailureReason {
    Unreachable,
    Refused,
    TimedOut,
    Closed,
    Other(IString),
}

/// Discovery-provided port used by route transport to report route health.
#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectionInfoPort<R>(NonOwningPhantomData<R>);

impl<R> Port for ConnectionInfoPort<R>
where
    R: Clone + Debug + Eq + Hash + Send + Sync + 'static,
{
    type Request = Never;
    type Indication = ConnectionInfoIndication<R>;
}

/// One logical outbound route-transport send.
#[derive(Clone)]
pub struct RouteTransportSend<R> {
    /// Stable caller-provided correlation id for this logical send attempt.
    pub send_id: RouteSendId,
    /// Discovery-published concrete route candidate to use for the send.
    pub route: SendRouteCandidate<R>,
    /// Logical payload to serialise once route transport has provisioned any
    /// transport-local resources it needs.
    pub payload: Arc<dyn FlotsyncSerializable>,
}

impl<R> std::fmt::Debug for RouteTransportSend<R>
where
    R: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteTransportSend")
            .field("send_id", &self.send_id)
            .field("route", &self.route)
            .field("payload", &"<serializable payload>")
            .finish()
    }
}

/// Abstract reasons why route transport rejected one logical send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteTransportNackReason {
    RouteUnknown,
    RouteUnavailable,
    Backpressure,
    InvalidPayload,
    LocalResourcePressure,
    Other(IString),
}

/// Directed outcome of one outbound route-transport submission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteTransportSubmitResult<R>
where
    R: Clone + Debug + Eq + Hash + Send + Sync + 'static,
{
    /// Route transport accepted the logical send and handed it to the
    /// underlying transport backend.
    ///
    /// This is a transport-layer success only. It does not mean that any
    /// semantic-delivery acknowledgement was observed.
    Sent { coverage_key: R },
    /// Route transport rejected the logical send before or during transport
    /// handoff.
    SendFailed {
        coverage_key: R,
        reason: RouteTransportNackReason,
    },
}

/// Externally owned UDP socket that should carry route-transport traffic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalUdpSocketRegistration {
    /// Driver-local socket id assigned by `flotsync_io`.
    pub socket_id: SocketId,
    /// Concrete local address bound for this socket.
    pub local_addr: SocketAddr,
}

/// Reason why an externally owned UDP socket could not be registered.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum ExternalUdpSocketRegistrationError {
    #[snafu(display("socket id {socket_id:?} is already registered"))]
    SocketIdAlreadyRegistered {
        /// Socket id already associated with another route-transport endpoint.
        socket_id: SocketId,
    },
    #[snafu(display("local address {local_addr} is already registered"))]
    LocalAddrAlreadyRegistered {
        /// Local address already associated with another route-transport endpoint.
        local_addr: SocketAddr,
    },
}

/// Directed actor messages accepted by the route-transport manager.
///
/// Outbound route-transport submission is intentionally actor-based rather than
/// port-based because each send has exactly one correlated result.
#[derive(Debug)]
pub enum RouteTransportActorMessage<R>
where
    R: Clone + Debug + Eq + Hash + Send + Sync + 'static,
{
    /// Submit one logical outbound route-transport send and resolve the
    /// attached `Ask` exactly once with the transport-layer outcome.
    Submit(Ask<RouteTransportSend<R>, RouteTransportSubmitResult<R>>),
    /// Register one externally bound UDP socket as a route-transport endpoint.
    RegisterExternalUdpSocket(
        Ask<ExternalUdpSocketRegistration, Result<(), ExternalUdpSocketRegistrationError>>,
    ),
}

/// Transport-local metadata attached to one fully reassembled inbound payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundTransportMeta<R> {
    /// Best-effort reconstruction of the concrete route key that produced this
    /// logical payload.
    pub route: R,
    /// Concrete remote address observed on the underlying transport when
    /// available.
    pub remote_addr: Option<SocketAddr>,
}

/// One fully reassembled inbound logical payload surfaced by route transport.
#[derive(Clone, Debug)]
pub struct RouteTransportInboundDeliver<R> {
    /// Full transport payload after any reassembly performed by the concrete
    /// transport backend.
    pub payload: IoPayload,
    /// Best-effort transport metadata describing where the payload came from.
    pub transport: InboundTransportMeta<R>,
}

/// Indication-only route-transport Kompact port provided upward to delivery
/// ingress.
///
/// `Request = Never` is intentional: outbound route-transport requests must go
/// through actor messaging with [`RouteTransportActorMessage::Submit`]. This
/// port exists only for unsolicited inbound delivery events produced by the
/// transport backend.
#[derive(Clone, Copy, Debug, Default)]
pub struct RouteTransportPort<R>(NonOwningPhantomData<R>);

impl<R> Port for RouteTransportPort<R>
where
    R: Clone + Debug + Eq + Hash + Send + Sync + 'static,
{
    type Request = Never;
    type Indication = RouteTransportInboundDeliver<R>;
}

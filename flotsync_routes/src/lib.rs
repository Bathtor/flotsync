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

pub use kompact;

mod endpoint_discovery;

pub mod key_material_discovery;
pub mod manager;
pub mod protocol;
pub mod route_establishment;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

/// Kompact configuration keys consumed by route-establishment support.
pub mod config_keys {
    use kompact::{config::DurationValue, kompact_config};
    use std::time::Duration;

    kompact_config! {
        ROUTE_ESTABLISHMENT_PROBE_TIMEOUT,
        key = "flotsync.discovery.route-establishment.probe-timeout",
        type = DurationValue,
        default = Duration::from_secs(2),
        doc = "Maximum wait for a route establishment introduction response before a probe is considered stale.",
        version = "0.1.0"
    }

    kompact_config! {
        ROUTE_ESTABLISHMENT_REACHABLE_LEASE,
        key = "flotsync.discovery.route-establishment.reachable-lease",
        type = DurationValue,
        default = Duration::from_secs(30),
        doc = "Time for which one verified route remains published before refresh is required.",
        version = "0.1.0"
    }

    kompact_config! {
        KEY_MATERIAL_DISCOVERY_SUPPRESSION_LEASE,
        key = "flotsync.discovery.key-material.suppression-lease",
        type = DurationValue,
        default = Duration::from_secs(2),
        doc = "Time for which one in-flight direct key-material lookup suppresses duplicate requests.",
        version = "0.1.0"
    }
}

use flotsync_core::MemberIdentity;
use flotsync_io::prelude::{IoPayload, SocketId, UdpCloseReason};
use flotsync_messages::serialisation::FlotsyncSerializable;
/// Per-socket `UDPour` runtime configuration used by route transport.
pub use flotsync_udpour::UDPourConfig;
use flotsync_utils::{IString, NonOwningPhantomData};
use kompact::{
    Never,
    prelude::{Ask, Port},
};
use snafu::Snafu;
use std::{fmt::Debug, hash::Hash, net::SocketAddr, sync::Arc};
use uuid::Uuid;

/// Temporary relay identity choice.
///
/// Discovery owns identity verification for peers and relays. Until a later
/// task proves otherwise, relay identities can stay on the same underlying type
/// as peer/member identities.
pub type RelayIdentity = MemberIdentity;

/// Stable identifier for one concrete send operation issued against an opaque
/// discovery-provided route.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RouteSendId(
    /// Caller-provided correlation id for this route-transport send.
    pub Uuid,
);

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
    /// Preference value used when discovery did not rank one route.
    pub const UNRANKED: Self = Self(0);

    /// Build a route preference from its raw rank value.
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
    /// Point-to-point UDP route to one remote address.
    Unicast,
    /// UDP broadcast route for one local network.
    Broadcast,
    /// UDP multicast route for one multicast group.
    Multicast,
}

/// Concrete UDP route key owned by route transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UdpRouteKey {
    /// Concrete remote UDP address used for outbound datagrams.
    pub remote_addr: SocketAddr,
    /// Datagram addressing mode for this route.
    pub scope: DatagramRouteScope,
    /// Optional local UDP bind address required for this route.
    pub local_bind: Option<SocketAddr>,
}

/// Concrete TCP route key owned by route transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TcpRouteKey {
    /// Concrete remote TCP address used for connection establishment.
    pub remote_addr: SocketAddr,
    /// Optional local TCP bind address required for this route.
    pub local_bind: Option<SocketAddr>,
}

/// Concrete route key used by the current transport-manager sketch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TransportRouteKey {
    /// UDP-backed route key.
    Udp(UdpRouteKey),
    /// TCP-backed route key.
    Tcp(TcpRouteKey),
}

/// Externally owned route endpoint socket authorised for route traffic.
///
/// This is route-endpoint ownership state, not a raw UDP bind event. The
/// `socket_bound_addr` is the concrete bind address reported by `flotsync_io`
/// for the socket and is used as the transport-local socket key. Concrete
/// addresses advertised to peers are published separately through discovery's
/// endpoint-selection contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteEndpointBinding {
    /// Driver-local socket id assigned by `flotsync_io`.
    pub socket_id: SocketId,
    /// Actual local bind address for this socket.
    pub socket_bound_addr: SocketAddr,
}

/// Reason why one authorised route endpoint became unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteEndpointUnavailableReason {
    /// The underlying UDP socket closed.
    Closed {
        /// UDP close reason observed by the endpoint owner.
        reason: UdpCloseReason,
    },
}

/// Route endpoint lifecycle published as endpoint ownership changes.
///
/// Endpoint owners publish the initial lifecycle. Route transport may
/// republish accepted events after applying them so downstream route users see
/// only endpoints that are already available through transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteEndpointLifecycle {
    /// The endpoint owner authorises this socket for route traffic.
    Available(RouteEndpointBinding),
    /// The endpoint owner no longer authorises this socket for route traffic.
    Unavailable {
        /// Binding that became unavailable.
        binding: RouteEndpointBinding,
        /// Semantic reason for the endpoint becoming unavailable.
        reason: RouteEndpointUnavailableReason,
    },
}

/// Port used to publish route endpoint availability between endpoint owners,
/// route transport, and route users.
#[derive(Clone, Copy, Debug, Default)]
pub struct RouteEndpointLifecyclePort;

impl Port for RouteEndpointLifecyclePort {
    type Request = Never;
    type Indication = RouteEndpointLifecycle;
}

/// Discovery-published route candidate.
///
/// `R` is the concrete coverage key type owned by route transport/discovery.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SendRouteCandidate<R> {
    /// Concrete key used by route transport to identify the candidate.
    pub coverage_key: R,
    /// Whether equal candidates may share one physical send.
    pub sharing: RouteSharingKind,
    /// Discovery-provided preference rank for this candidate.
    pub preference_rank: RoutePreferenceRank,
}

/// Published discovery update for one peer or relay.
///
/// Each update fully replaces the previously published route set for that
/// peer or relay. An empty `routes` list withdraws all currently usable routes
/// for that identity.
#[derive(Clone, Debug)]
pub enum DiscoveryRouteUpdate<R> {
    /// Replace all known routes for one discovered peer.
    PeerRoutes {
        /// Peer whose routes are being replaced.
        peer: MemberIdentity,
        /// Complete replacement route set for the peer.
        routes: Vec<SendRouteCandidate<R>>,
    },
    /// Replace all known routes for one discovered relay.
    RelayRoutes {
        /// Relay whose routes are being replaced.
        relay: RelayIdentity,
        /// Complete replacement route set for the relay.
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
    /// Report that one concrete route failed and should be downgraded or withdrawn.
    ReportRouteFailed {
        /// Concrete route that failed.
        route: R,
        /// Transport-observed failure reason.
        reason: ConnectionFailureReason,
    },
}

/// Reasons why a concrete route should be downgraded or dropped by discovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionFailureReason {
    /// Remote endpoint could not be reached.
    Unreachable,
    /// Remote endpoint refused the transport attempt.
    Refused,
    /// Transport attempt or exchange timed out.
    TimedOut,
    /// The route was closed while still considered usable.
    Closed,
    /// Route failed for a transport-specific reason.
    Other(
        /// Transport-specific failure description.
        IString,
    ),
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
    /// Route transport does not know the submitted concrete route.
    RouteUnknown,
    /// Route transport knows the route, but it cannot currently be used.
    RouteUnavailable,
    /// Route transport rejected the send because local queues were full.
    Backpressure,
    /// Payload serialisation failed before transport handoff.
    InvalidPayload,
    /// Route transport could not allocate a required local resource.
    LocalResourcePressure,
    /// Route transport rejected the send for a transport-specific reason.
    Other(
        /// Transport-specific rejection description.
        IString,
    ),
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
    Sent {
        /// Concrete route key that accepted the outbound payload.
        coverage_key: R,
    },
    /// Route transport rejected the logical send before or during transport
    /// handoff.
    SendFailed {
        /// Concrete route key that rejected the outbound payload.
        coverage_key: R,
        /// Transport-layer rejection reason.
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

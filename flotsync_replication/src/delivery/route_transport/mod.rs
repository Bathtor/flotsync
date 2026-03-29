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
//! contracts below and concretized here for the current transport manager
//! sketch.

pub mod manager;

use super::shared::{ReachabilityClass, RelayIdentity, RouteSendId};
use crate::api::MemberIdentity;
use flotsync_io::prelude::{EgressAsyncWriter, Error as IoError};
use flotsync_utils::{BoxFuture, IString, NonOwningPhantomData};
use kompact::{Never, prelude::Port};
use snafu::prelude::*;
use std::{fmt::Debug, hash::Hash, net::SocketAddr, sync::Arc};

/// Size hint returned by one serializable network message.
///
/// Route transport can use this to choose between bounded sync reservation and
/// fully async/growable encoding paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeHint {
    /// Absolutely no idea about the required size.
    Unknown,
    /// Given value is the exact size required.
    Exact(usize),
    /// Given value is an upper bound on the size required.
    UpperBound(usize),
    /// Given value is an estimate that may be too large or too small.
    Estimate(usize),
}

/// Dyn-safe payload contract for flotsync network messages.
pub trait FlotsyncSerializable: Send + Sync + 'static {
    fn serialized_size_hint(&self) -> SizeHint;

    fn serialize_into<'a>(
        &'a self,
        writer: &'a mut EgressAsyncWriter,
    ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>>;
}

/// Serialization failure at the route-transport boundary.
#[derive(Debug, Snafu)]
pub enum FlotsyncSerializeError {
    #[snafu(display("writer failure while serializing flotsync message"))]
    Io { source: IoError },
    #[snafu(display("message serialization failed: {message}"))]
    Encoding { message: IString },
}

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

    pub fn new(value: u8) -> Self {
        Self(value)
    }

    /// No specific preference was provided.
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
#[derive(Clone, Debug)]
pub enum DiscoveryRouteUpdate<R> {
    PeerRoutes {
        peer: MemberIdentity,
        classification: ReachabilityClass,
        routes: Vec<SendRouteCandidate<R>>,
    },
    RelayRoutes {
        relay: RelayIdentity,
        classification: ReachabilityClass,
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
    pub send_id: RouteSendId,
    pub route: SendRouteCandidate<R>,
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

/// Route-transport Kompact port provided to semantic delivery.
#[derive(Clone, Copy, Debug, Default)]
pub struct RouteTransportPort<R>(NonOwningPhantomData<R>);

impl<R> Port for RouteTransportPort<R>
where
    R: Clone + Debug + Eq + Hash + Send + Sync + 'static,
{
    type Request = RouteTransportSend<R>;
    type Indication = RouteTransportPortIndication<R>;
}

/// Indications emitted by route transport back to semantic delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteTransportPortIndication<R> {
    SendAck {
        send_id: RouteSendId,
        coverage_key: R,
    },
    SendNack {
        send_id: RouteSendId,
        coverage_key: R,
        reason: RouteTransportNackReason,
    },
}

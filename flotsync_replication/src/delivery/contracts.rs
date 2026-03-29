//! Actor-style message contracts and durable storage boundary for delivery.

use super::{
    group_broadcast::{GroupBroadcastDeliver, GroupBroadcastSubmit},
    reliable_delivery::{ReliableDeliveryDeliver, ReliableDeliverySubmit},
    shared::{MessageId, ReachabilityClass, RelayIdentity, RouteSendId, SendRouteId},
};
use crate::api::MemberIdentity;
use flotsync_utils::BoxFuture;
use kompact::prelude::{Port, Recipient};
use snafu::prelude::*;
use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
    time::SystemTime,
};

/// Marker trait for protobuf-backed network messages.
///
/// The concrete serialization method does not belong in this task. The only
/// thing the delivery domain needs here is a shared dyn-safe payload type that
/// discovery-published send routes can accept.
pub trait ProtobufSerializable: Send + Sync + 'static {}

/// Group-broadcast Kompact port.
#[derive(Clone, Copy, Debug)]
pub struct GroupBroadcastPort;

impl Port for GroupBroadcastPort {
    type Request = GroupBroadcastPortRequest;
    type Indication = GroupBroadcastPortIndication;
}

/// Requests sent into the group-broadcast component.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupBroadcastPortRequest {
    Submit(GroupBroadcastSubmit),
}

/// Indications emitted by the group-broadcast component.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupBroadcastPortIndication {
    Deliver(GroupBroadcastDeliver),
}

/// Reliable-delivery Kompact port.
#[derive(Clone, Copy, Debug)]
pub struct ReliableDeliveryPort;

impl Port for ReliableDeliveryPort {
    type Request = ReliableDeliveryPortRequest;
    type Indication = ReliableDeliveryPortIndication;
}

/// Requests sent into the reliable-delivery component.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReliableDeliveryPortRequest {
    Submit(ReliableDeliverySubmit),
}

/// Indications emitted by the reliable-delivery component.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReliableDeliveryPortIndication {
    Deliver(ReliableDeliveryDeliver),
}

/// Discovery-published opaque send route.
///
/// Equality and hashing intentionally ignore the underlying recipient and use
/// only `route_id`. That gives the scheduler exactly the information it needs
/// for route-coverage grouping and nothing more.
#[derive(Clone)]
pub struct ResolvedSendRoute {
    pub route_id: SendRouteId,
    pub send_to: Recipient<RouteSend>,
}

impl fmt::Debug for ResolvedSendRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedSendRoute")
            .field("route_id", &self.route_id)
            .finish()
    }
}

impl PartialEq for ResolvedSendRoute {
    fn eq(&self, other: &Self) -> bool {
        self.route_id == other.route_id
    }
}

impl Eq for ResolvedSendRoute {}

impl Hash for ResolvedSendRoute {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.route_id.hash(state);
    }
}

/// Generic send request issued against one discovery-provided route.
#[derive(Clone)]
pub struct RouteSend {
    pub send_id: RouteSendId,
    pub payload: Arc<dyn ProtobufSerializable>,
    pub reply_to: Recipient<RouteSendResult>,
}

impl fmt::Debug for RouteSend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouteSend")
            .field("send_id", &self.send_id)
            .finish()
    }
}

/// Generic transport-facing response to one `RouteSend`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteSendResult {
    Ack {
        send_id: RouteSendId,
    },
    Nack {
        send_id: RouteSendId,
        reason: RouteSendNackReason,
    },
}

/// Abstract reasons why a discovery-provided route could not accept a send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteSendNackReason {
    RouteUnavailable,
    Backpressure,
    InvalidPayload,
    Other(String),
}

/// Published discovery update for one peer or relay.
#[derive(Clone, Debug)]
pub enum DiscoveryRouteUpdate {
    PeerRoutes {
        peer: MemberIdentity,
        classification: ReachabilityClass,
        routes: Vec<ResolvedSendRoute>,
    },
    RelayRoutes {
        relay: RelayIdentity,
        classification: ReachabilityClass,
        routes: Vec<ResolvedSendRoute>,
    },
}

/// Durable store record for one accepted group-broadcast request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredGroupBroadcastWork {
    pub submit: GroupBroadcastSubmit,
    pub first_submitted_at: SystemTime,
}

/// Durable store record for one accepted reliable-delivery request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredReliableDeliveryWork {
    pub submit: ReliableDeliverySubmit,
    pub first_submitted_at: SystemTime,
}

/// Async storage error type for durable work records.
#[derive(Debug, Snafu)]
pub enum DeliveryWorkStoreError {
    #[snafu(display("Delivery persistence failed: {message}"))]
    Persistence { message: String },
}

/// Async persistence contract for durable work records.
///
/// Routes are intentionally not part of the persisted state. After restart the
/// runtime should reload the durable submit records and rebuild current routes
/// from discovery.
pub trait DeliveryWorkStore: Send + Sync {
    fn load_group_broadcast_work(
        &self,
    ) -> BoxFuture<'_, Result<Vec<StoredGroupBroadcastWork>, DeliveryWorkStoreError>>;

    fn load_reliable_delivery_work(
        &self,
    ) -> BoxFuture<'_, Result<Vec<StoredReliableDeliveryWork>, DeliveryWorkStoreError>>;

    fn store_group_broadcast_work(
        &self,
        work: StoredGroupBroadcastWork,
    ) -> BoxFuture<'_, Result<(), DeliveryWorkStoreError>>;

    fn store_reliable_delivery_work(
        &self,
        work: StoredReliableDeliveryWork,
    ) -> BoxFuture<'_, Result<(), DeliveryWorkStoreError>>;

    fn remove_group_broadcast_work(
        &self,
        message_id: MessageId,
    ) -> BoxFuture<'_, Result<(), DeliveryWorkStoreError>>;

    fn remove_reliable_delivery_work(
        &self,
        message_id: MessageId,
    ) -> BoxFuture<'_, Result<(), DeliveryWorkStoreError>>;
}

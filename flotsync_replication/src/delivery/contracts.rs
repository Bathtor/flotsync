//! Actor-style message contracts and durable storage boundary for delivery.

use super::{
    group_broadcast::{GroupBroadcastDeliver, GroupBroadcastSubmit},
    reliable_delivery::{ReliableDeliveryDeliver, ReliableDeliverySubmit},
    shared::MessageId,
};
use flotsync_utils::BoxFuture;
use kompact::prelude::Port;
use snafu::prelude::*;
use std::time::SystemTime;

pub use flotsync_route_transport::DiscoveryRouteUpdate;

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

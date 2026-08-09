//! Actor-style message contracts and durable storage boundary for delivery.

use super::{
    group_broadcast::{GroupBroadcastDeliver, GroupBroadcastSubmit},
    reliable_delivery::{ReliableDeliveryDeliver, ReliableDeliverySubmit},
    shared::MessageId,
};
use crate::api::StoreError;
use bytes::Bytes;
use flotsync_core::MemberIdentity;
use flotsync_utils::BoxFuture;
use kompact::prelude::Port;
use std::time::SystemTime;

pub use flotsync_routes::DiscoveryRouteUpdate;

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

/// Lightweight persisted sender-work metadata used before loading an envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredReliableDeliveryWorkMetadata {
    /// Stable identity shared by the stored envelope, retry state, and recipient ack.
    pub message_id: MessageId,
    /// Recipient used to gate full-envelope loading on current route availability.
    pub recipient: MemberIdentity,
    /// Wall-clock time when reliable delivery first began processing the submit.
    pub first_submitted_at: SystemTime,
}

/// Complete recipient-confidential envelope retained until semantic acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredReliableDeliveryWork {
    /// Metadata that can be loaded without reading the potentially large envelope.
    pub metadata: StoredReliableDeliveryWorkMetadata,
    /// Canonically encoded reliable-delivery endpoint envelope.
    pub encoded_envelope: Bytes,
}

/// Narrow persistence capability required by reliable delivery.
///
/// Implementations retain each accepted encoded envelope and expose its small
/// scheduling projection separately so startup need not read all envelope bytes.
pub trait ReliableDeliveryStore: Send + Sync {
    /// Load metadata for every unacknowledged outbound item.
    fn load_reliable_delivery_work_metadata(
        &self,
    ) -> BoxFuture<'_, Result<Vec<StoredReliableDeliveryWorkMetadata>, StoreError>>;

    /// Load the complete encoded envelope for `message_id`. Return `Ok(None)`
    /// when no such item exists.
    ///
    /// Reliable delivery calls this after the item becomes route-eligible.
    fn load_reliable_delivery_work(
        &self,
        message_id: MessageId,
    ) -> BoxFuture<'_, Result<Option<StoredReliableDeliveryWork>, StoreError>>;

    /// Idempotently store one newly sealed envelope before transport submission.
    ///
    /// Repeating the same message id replaces the row with the supplied
    /// recipient, timestamp, and encoded envelope so an ambiguous insertion
    /// result can be retried safely.
    fn store_reliable_delivery_work(
        &self,
        work: StoredReliableDeliveryWork,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Remove the encoded envelope and metadata for `message_id`.
    ///
    /// Return `true` when an item was removed and `false` otherwise.
    fn remove_reliable_delivery_work(
        &self,
        message_id: MessageId,
    ) -> BoxFuture<'_, Result<bool, StoreError>>;
}

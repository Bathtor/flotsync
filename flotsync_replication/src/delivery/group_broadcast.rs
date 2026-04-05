//! Group-scoped delivery-domain types.

use super::{
    ingress::InboundDeliveryMeta,
    shared::{
        ActiveRouteRecord,
        DeliveryClass,
        EncryptedPayload,
        LogicalRouteId,
        MessageId,
        SignedEnvelopeFooter,
    },
};
use crate::api::{GroupId, MemberIdentity};
use flotsync_messages::delivery as delivery_proto;
use flotsync_utils::NonOwningPhantomData;
use kompact::{Never, prelude::Port};
use std::time::SystemTime;

/// Plaintext group-scoped envelope header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMessageHeader {
    pub group_id: GroupId,
    pub sender: MemberIdentity,
    pub message_id: MessageId,
}

/// Immutable group-scoped fan-out envelope.
///
/// The delivery class is intentionally not part of the transmitted envelope. It
/// is a local scheduling policy on the submit request, not payload content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMessageEnvelope {
    pub header: GroupMessageHeader,
    pub payload: EncryptedPayload,
    pub footer: SignedEnvelopeFooter,
}

/// Replication-to-broadcast request.
///
/// Route expansion is intentionally not part of this message. Broadcast owns
/// route resolution based on current discovery output and configured relays.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupBroadcastSubmit {
    pub delivery_class: DeliveryClass,
    pub envelope: GroupMessageEnvelope,
}

/// Inbound group message delivered by the network-facing broadcast service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupBroadcastDeliver {
    pub envelope: GroupMessageEnvelope,
}

/// Inbound group-broadcast payload handed to the semantic owner from delivery
/// ingress.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupBroadcastInboundDeliver<R> {
    /// Shared ingress metadata derived before the semantic handoff.
    pub meta: InboundDeliveryMeta<R>,
    /// Fully decoded group-broadcast boundary frame owned by the generated
    /// protobuf types.
    pub frame: delivery_proto::GroupBroadcastFrame,
}

/// Internal ingress port that feeds decoded group-broadcast boundary frames
/// into the group-broadcast service.
#[derive(Clone, Copy, Debug, Default)]
pub struct GroupBroadcastInboundPort<R>(NonOwningPhantomData<R>);

impl<R> Port for GroupBroadcastInboundPort<R>
where
    R: Clone + std::fmt::Debug + Send + Sync + 'static,
{
    /// Delivery ingress is the sole producer for this internal semantic
    /// stream.
    type Request = Never;
    type Indication = GroupBroadcastInboundDeliver<R>;
}

/// Queue-owned in-memory state for one accepted group broadcast request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupBroadcastWorkItem {
    pub submit: GroupBroadcastSubmit,
    pub admitted_at: SystemTime,
    pub member_routes: Vec<ActiveRouteRecord>,
    pub relay_routes: Vec<ActiveRouteRecord>,
}

/// Advisory receipt used only for compaction and observability.
///
/// This is deliberately not part of the correctness boundary. Group broadcast
/// transitions to `Delivered` on accepted direct handoff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryReceipt {
    pub group_id: GroupId,
    pub message_id: MessageId,
    pub route_id: LogicalRouteId,
    pub recipient: MemberIdentity,
}

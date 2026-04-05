//! Recipient-addressed durable delivery types.

use super::{
    ingress::InboundDeliveryMeta,
    shared::{
        ActiveRouteRecord,
        EncryptedPayload,
        MailboxItemId,
        MessageId,
        RelayIdentity,
        SignedEnvelopeFooter,
    },
};
use crate::api::MemberIdentity;
use flotsync_messages::delivery as delivery_proto;
use flotsync_utils::NonOwningPhantomData;
use kompact::{Never, prelude::Port};
use std::time::SystemTime;
use uuid::Uuid;

/// Plaintext recipient-addressed envelope header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableMessageHeader {
    pub sender: MemberIdentity,
    pub recipient: MemberIdentity,
    pub message_id: MessageId,
}

/// Immutable sender-signed envelope used for both direct recipient delivery and
/// relay mailbox storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableMessageEnvelope {
    pub header: ReliableMessageHeader,
    pub payload: EncryptedPayload,
    pub footer: SignedEnvelopeFooter,
}

/// Replication-to-delivery request for one recipient-addressed message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliverySubmit {
    pub envelope: ReliableMessageEnvelope,
}

/// Inbound reliable-delivery message delivered by the network-facing service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliveryDeliver {
    pub envelope: ReliableMessageEnvelope,
}

/// Inbound reliable-delivery payload handed to the semantic owner from
/// delivery ingress.
#[derive(Clone, Debug, PartialEq)]
pub struct ReliableDeliveryInboundDeliver<R> {
    /// Shared ingress metadata derived before the semantic handoff.
    pub meta: InboundDeliveryMeta<R>,
    /// Fully decoded reliable-delivery boundary frame owned by the generated
    /// protobuf types.
    pub frame: delivery_proto::ReliableDeliveryFrame,
}

/// Internal ingress port that feeds decoded reliable-delivery boundary frames
/// into the reliable-delivery service.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReliableDeliveryInboundPort<R>(NonOwningPhantomData<R>);

impl<R> Port for ReliableDeliveryInboundPort<R>
where
    R: Clone + std::fmt::Debug + Send + Sync + 'static,
{
    /// Delivery ingress is the sole producer for this internal semantic
    /// stream.
    type Request = Never;
    type Indication = ReliableDeliveryInboundDeliver<R>;
}

/// Queue-owned in-memory state for one accepted reliable-delivery message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliveryWorkItem {
    pub submit: ReliableDeliverySubmit,
    pub admitted_at: SystemTime,
    pub recipient_route: ActiveRouteRecord,
    pub relay_routes: Vec<ActiveRouteRecord>,
    pub recipient_ack: RecipientAckStatus,
}

/// Sender-side completion tracking for recipient-addressed durable delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecipientAckStatus {
    Pending,
    Observed {
        acknowledged_at: SystemTime,
        ack: RecipientAck,
    },
}

/// Plaintext recipient-ack header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipientAckHeader {
    pub message_id: MessageId,
    pub original_sender: MemberIdentity,
    pub recipient: MemberIdentity,
}

/// Recipient-signed completion signal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipientAck {
    pub header: RecipientAckHeader,
    pub footer: SignedEnvelopeFooter,
}

/// Signed proof used by the recipient when checking in with a relay mailbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdentityProof {
    pub signer: MemberIdentity,
    pub challenge: Uuid,
    pub footer: SignedEnvelopeFooter,
}

/// Recipient-driven mailbox retrieval request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxFetch {
    pub recipient: MemberIdentity,
    pub freshness_token: Uuid,
    pub proof: IdentityProof,
}

/// One mailbox item returned by a relay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxItem {
    pub item_id: MailboxItemId,
    pub envelope: ReliableMessageEnvelope,
}

/// One full fetch response from a relay mailbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxBatch {
    pub relay: RelayIdentity,
    pub recipient: MemberIdentity,
    pub items: Vec<MailboxItem>,
}

/// Handle used when acknowledging mailbox items back to the relay.
///
/// The protocol draft explicitly leaves room for either stable message ids,
/// relay-issued handles, or both.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MailboxAckHandle {
    Message(MessageId),
    Item(MailboxItemId),
}

/// Relay cleanup acknowledgement sent only after the recipient durably accepted
/// the mailbox item locally.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxAck {
    pub relay: RelayIdentity,
    pub recipient: MemberIdentity,
    pub acknowledgements: Vec<MailboxAckHandle>,
}

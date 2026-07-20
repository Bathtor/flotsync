//! Protocol data structures for reliable delivery.

#[allow(
    clippy::wildcard_imports,
    reason = "The private delivery helper shares its parent's local implementation vocabulary."
)]
use super::*;

/// Plaintext recipient-addressed envelope header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableMessageHeader {
    pub sender: MemberIdentity,
    pub recipient: MemberIdentity,
    pub message_id: MessageId,
    pub scope: ReliableMessageScope,
}

/// HPKE-sealed and sender-signed reliable-delivery payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedPayload {
    pub sealed: SealedHPKEPayload,
}

/// Recipient-addressed envelope queued or carried by reliable delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableMessageEnvelope<P> {
    pub header: ReliableMessageHeader,
    pub payload: P,
}

impl ReliableMessageEnvelope<EncryptedPayload> {
    pub(super) fn to_wire_format(&self) -> endpoint_proto::EndpointFrame {
        reliable_envelope_to_wire_format(self)
    }
}

/// Replication-to-delivery request for one recipient-addressed message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliverySubmit {
    pub envelope: ReliableMessageEnvelope<PlaintextPayload>,
}

/// Inbound reliable-delivery message delivered by the network-facing service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliveryDeliver {
    pub envelope: ReliableMessageEnvelope<PlaintextPayload>,
    /// Completing this handle confirms that the recipient processed the
    /// delivered item and may now emit a semantic recipient ack.
    pub processed: KClaimablePromise<()>,
}

/// Inbound reliable-delivery payload handed to the semantic owner from
/// delivery ingress.
#[derive(Clone, Debug, PartialEq)]
pub struct ReliableDeliveryInboundDeliver<R> {
    /// Shared ingress metadata derived before the semantic handoff.
    pub meta: InboundDeliveryMeta<R>,
    /// Fully decoded reliable-delivery endpoint branch owned by the generated
    /// protobuf types.
    pub frame: delivery_proto::ReliableDeliveryFrame,
}

/// Internal ingress port that feeds decoded reliable-delivery endpoint branches
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
    pub recipient_route: ActiveRouteRecord,
    pub relay_routes: Vec<ActiveRouteRecord>,
    pub recipient_ack: RecipientAckStatus,
}

/// Sender-side completion tracking for recipient-addressed reliable delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecipientAckStatus {
    Pending,
    Observed { ack: RecipientAck },
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
    pub signature: DetachedSignature,
}

impl RecipientAck {
    pub(super) fn to_wire_format(&self) -> endpoint_proto::EndpointFrame {
        recipient_ack_to_wire_format(self)
    }
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
    pub envelope: ReliableMessageEnvelope<EncryptedPayload>,
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

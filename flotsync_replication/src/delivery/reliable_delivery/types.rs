//! Protocol data structures for reliable delivery.

#[allow(
    clippy::wildcard_imports,
    reason = "The private delivery helper shares its parent's local implementation vocabulary."
)]
use super::*;
use crate::delivery::contracts::StoredReliableDeliveryWorkMetadata;

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

/// Inbound message delivered reliably by the network-facing service.
///
/// In rare recovery circumstances, such as a crash before acknowledgement or stored-row cleanup
/// finishes, a message may be delivered again. Consumers must process duplicate
/// [`ReliableMessageHeader::message_id`] values safely, but need not optimise that exceptional path.
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

/// In-memory lifecycle state for one persisted reliable-delivery message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReliableDeliveryWorkItem {
    /// Persisted scheduling metadata; the full envelope remains in the store
    /// until a usable route and outbound-attempt slot are both available.
    pub metadata: StoredReliableDeliveryWorkMetadata,
    /// Direct-recipient route state for queue, attempt, and acknowledgement transitions.
    pub recipient_route: ActiveRouteRecord,
    /// Reserved relay-route state; empty until relay delivery is implemented.
    pub relay_routes: Vec<ActiveRouteRecord>,
    /// Whether the first missing-recipient-ack timeout has already been reported.
    pub(super) reported_ack_timeout: bool,
    /// Final cleanup state, present after sending has stopped for this message.
    pub(super) pending_removal: Option<PendingRemoval>,
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

/// Stored-row cleanup state retained as part of the persisted work lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PendingRemoval {
    /// Why active sending stopped.
    pub(super) reason: PendingRemovalReason,
    /// Whether the persisted row still needs to be removed.
    pub(super) stored_row_pending: bool,
    /// Transport result that must still release its reserved capacity slot.
    pub(super) outstanding_send_id: Option<RouteSendId>,
}

/// Why active sending stopped while the stored row awaits removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PendingRemovalReason {
    /// The intended recipient returned a verified semantic acknowledgement.
    RecipientAcknowledged,
    /// The selected stored envelope record was invalid and cannot be sent.
    InvalidStoredRecord,
    /// Route transport permanently rejected the unchanged stored envelope.
    PermanentTransportFailure,
}

impl PendingRemovalReason {
    /// Human-readable context retained across cleanup retries.
    pub(super) const fn description(self) -> &'static str {
        match self {
            Self::RecipientAcknowledged => "verified recipient acknowledgement",
            Self::InvalidStoredRecord => "invalid stored envelope record",
            Self::PermanentTransportFailure => "permanent transport failure",
        }
    }
}

//! Delivery-wire parsing and early local-interest classification.
//!
//! The shallow classifier intentionally parses only the semantic boundary and
//! the relevant `public_header` sub-message before deciding whether the local
//! node cares about the frame. Full protobuf decode is deferred until after
//! that decision so large encrypted payloads and mailbox batches are skipped
//! for irrelevant traffic.
//!
//! The key result semantics in this module are:
//!
//! - `Ok(Some(...))`: the payload was well-formed enough to classify, was
//!   locally relevant, and was then fully decoded;
//! - `Ok(None)`: the payload was well-formed enough to classify, but the
//!   shallow public header showed that the local node should ignore it;
//! - `Err(...)`: the payload was malformed, internally inconsistent, or
//!   otherwise undecodable at the delivery-wire level.

use super::{
    ingress::DeliveryTargetHint,
    shared::{DetachedSignature, MessageId, SignatureScheme},
};
use crate::{
    GroupMemberships,
    api::{GroupId, MemberIdentity},
};
use flotsync_core::member::{IdentifierBuf, IdentifierError};
use flotsync_io::prelude::IoPayload;
use flotsync_messages::{
    buffa::{DecodeError, Message, MessageView},
    delivery as proto,
    discovery as discovery_proto,
};
use proto::delivery_boundary_frame::Boundary;
use snafu::prelude::*;
use std::collections::HashSet;
use uuid::Uuid;

pub(crate) fn member_identity_to_wire_format(
    member: &MemberIdentity,
) -> discovery_proto::Identifier {
    let segments = member
        .segments_iter()
        .map(|segment| segment.as_ref().to_owned())
        .collect();
    discovery_proto::Identifier {
        segments,
        ..discovery_proto::Identifier::default()
    }
}

pub(crate) fn member_identity_from_wire(
    identifier: discovery_proto::Identifier,
    field: &'static str,
) -> Result<MemberIdentity, WireValueDecodeError> {
    let mut buffer = IdentifierBuf::new();
    for segment in identifier.segments {
        buffer
            .push_checked(segment.clone())
            .context(InvalidIdentifierSegmentSnafu { field, segment })?;
    }
    Ok(buffer.into_identifier())
}

pub(crate) fn group_id_from_wire(
    raw: &[u8],
    field: &'static str,
) -> Result<GroupId, WireValueDecodeError> {
    Ok(GroupId(uuid_from_wire(raw, field)?))
}

pub(crate) fn message_id_from_wire(
    raw: &[u8],
    field: &'static str,
) -> Result<MessageId, WireValueDecodeError> {
    Ok(MessageId(uuid_from_wire(raw, field)?))
}

/// Validate and copy a protobuf byte field into a fixed-width protocol array.
pub(crate) fn fixed_bytes_field<const N: usize>(
    field: &'static str,
    bytes: &[u8],
) -> Result<[u8; N], WireValueDecodeError> {
    bytes
        .try_into()
        .map_err(|_| WireValueDecodeError::InvalidByteLength {
            field,
            expected: N,
            actual: bytes.len(),
        })
}

/// Encode a signature-only control-frame authenticator.
pub(crate) fn detached_signature_to_wire_format(
    signature: &DetachedSignature,
) -> proto::DetachedSignature {
    proto::DetachedSignature {
        scheme: flotsync_messages::buffa::EnumValue::from(match signature.scheme {
            SignatureScheme::Ed25519 => proto::KnownSignatureScheme::KNOWN_SIGNATURE_SCHEME_ED25519,
        }),
        signature_bytes: signature.bytes.clone(),
        ..proto::DetachedSignature::default()
    }
}

/// Decode a signature-only control-frame authenticator.
pub(crate) fn detached_signature_from_wire(
    wire: proto::DetachedSignature,
    field: &'static str,
) -> Result<DetachedSignature, WireValueDecodeError> {
    let scheme =
        wire.scheme
            .as_known()
            .ok_or_else(|| WireValueDecodeError::UnknownSignatureScheme {
                field,
                value: wire.scheme.to_i32(),
            })?;
    let scheme = match scheme {
        proto::KnownSignatureScheme::KNOWN_SIGNATURE_SCHEME_ED25519 => SignatureScheme::Ed25519,
        proto::KnownSignatureScheme::KNOWN_SIGNATURE_SCHEME_UNSPECIFIED => {
            return UnspecifiedSignatureSchemeSnafu { field }.fail();
        }
    };
    Ok(DetachedSignature {
        scheme,
        bytes: wire.signature_bytes,
    })
}

/// Parse one delivery boundary frame, dropping it early if the shallow public
/// header shows that the local node has no interest in it.
///
/// This function performs the two-stage ingress strategy used by
/// [`DeliveryIngress`](super::ingress::DeliveryIngress):
///
/// - first derive local interest from the cheap public header only;
/// - then fully decode the protobuf payload only for relevant traffic.
///
/// `Ok(None)` is therefore an expected fast-path outcome for valid but
/// irrelevant traffic.
pub(crate) fn decode_boundary_frame_if_interested(
    payload: &mut IoPayload,
    interest: DeliveryInterestView<'_>,
) -> Result<Option<DecodedDeliveryFrame>, DeliveryWireError> {
    let payload_slice = payload.as_contiguous_slice();

    let Some(classification) = classify_boundary_frame(payload_slice, interest)? else {
        return Ok(None);
    };

    let decoded = decode_full_boundary_frame(payload_slice)?;
    match (classification.owner, decoded) {
        (SemanticOwner::GroupBroadcast, Boundary::GroupBroadcast(frame)) => {
            Ok(Some(DecodedDeliveryFrame::GroupBroadcast {
                target: classification.target,
                frame: *frame,
            }))
        }
        (SemanticOwner::ReliableDelivery, Boundary::ReliableDelivery(frame)) => {
            Ok(Some(DecodedDeliveryFrame::ReliableDelivery {
                target: classification.target,
                frame: *frame,
            }))
        }
        (SemanticOwner::GroupBroadcast, Boundary::ReliableDelivery(_)) => {
            BoundaryOwnerMismatchSnafu {
                shallow_owner: SemanticOwner::GroupBroadcast,
                full_owner: SemanticOwner::ReliableDelivery,
            }
            .fail()
        }
        (SemanticOwner::ReliableDelivery, Boundary::GroupBroadcast(_)) => {
            BoundaryOwnerMismatchSnafu {
                shallow_owner: SemanticOwner::ReliableDelivery,
                full_owner: SemanticOwner::GroupBroadcast,
            }
            .fail()
        }
    }
}

/// One locally relevant delivery frame that has passed the shallow
/// classification step and then been fully decoded as protobuf.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DecodedDeliveryFrame {
    /// Group-owned traffic that should be handed to the group-broadcast
    /// semantic owner.
    GroupBroadcast {
        target: DeliveryTargetHint,
        frame: proto::GroupBroadcastFrame,
    },
    /// Recipient-owned or delivery-control traffic that should be handed to
    /// the reliable-delivery semantic owner.
    ReliableDelivery {
        target: DeliveryTargetHint,
        frame: proto::ReliableDeliveryFrame,
    },
}

/// Borrowed local-interest snapshot used during shallow delivery-wire
/// classification.
///
/// Grouping these sets into one struct makes the call sites less repetitive and
/// makes it explicit that classification is always evaluated against one
/// coherent local-interest view.
#[derive(Clone, Copy, Debug)]
pub struct DeliveryInterestView<'a> {
    pub group_memberships: &'a GroupMemberships,
    pub local_members: &'a HashSet<MemberIdentity>,
    pub hosted_mailboxes: &'a HashSet<MemberIdentity>,
}

impl DeliveryInterestView<'_> {
    /// Keep the classification flowing only when the given group currently
    /// exists in the local membership snapshot.
    fn check_group(self, group_id: &GroupId) -> Option<ShallowClassification> {
        (!self.group_memberships.contains_group(group_id))
            .then_some(ShallowClassification::Irrelevant)
    }

    /// Keep the classification flowing only when the given member identity is
    /// hosted locally and should therefore receive recipient-scoped delivery
    /// traffic.
    fn check_local_member(self, member: &MemberIdentity) -> Option<ShallowClassification> {
        (!self.local_members.contains(member)).then_some(ShallowClassification::Irrelevant)
    }

    /// Keep the classification flowing only when this node currently hosts the
    /// relay mailbox addressed by the given recipient identity.
    fn check_hosted_mailbox(self, recipient: &MemberIdentity) -> Option<ShallowClassification> {
        (!self.hosted_mailboxes.contains(recipient)).then_some(ShallowClassification::Irrelevant)
    }
}

/// Failure modes for delivery-wire classification and full decode.
#[derive(Debug, Snafu)]
pub(crate) enum DeliveryWireError {
    #[snafu(display("Failed to parse delivery wire payload: {source}"))]
    Decode { source: DecodeError },

    #[snafu(display("Protobuf message '{message}' is missing required field '{field}'."))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },

    #[snafu(display("Protobuf oneof '{name}' has no selected value."))]
    MissingOneof { name: &'static str },

    #[snafu(transparent)]
    WireValueDecode { source: WireValueDecodeError },

    #[snafu(display(
        "Shallow delivery classifier selected {shallow_owner}, but full protobuf decode selected {full_owner}."
    ))]
    BoundaryOwnerMismatch {
        shallow_owner: SemanticOwner,
        full_owner: SemanticOwner,
    },
}

/// Perform the shallow classification pass over one boundary frame.
///
/// This pass never constructs the full generated protobuf frame. Instead it
/// reads only enough structure to decide semantic ownership and local
/// relevance.
///
/// `Ok(None)` means the payload was classified successfully and deliberately
/// dropped as irrelevant.
fn classify_boundary_frame(
    payload: &[u8],
    interest: DeliveryInterestView<'_>,
) -> Result<Option<RelevantShallowClassification>, DeliveryWireError> {
    let frame = proto::DeliveryBoundaryFrameView::decode_view(payload).context(DecodeSnafu)?;
    let boundary = frame.boundary.as_ref().context(MissingOneofSnafu {
        name: "DeliveryBoundaryFrame.boundary",
    })?;

    let classification = match boundary {
        proto::delivery_boundary_frame::BoundaryView::GroupBroadcast(frame) => {
            parse_group_broadcast_frame_hint(frame, interest)?
        }
        proto::delivery_boundary_frame::BoundaryView::ReliableDelivery(frame) => {
            parse_reliable_delivery_frame_hint(frame, interest)?
        }
    };

    match classification {
        ShallowClassification::Relevant { owner, target } => {
            Ok(Some(RelevantShallowClassification { owner, target }))
        }
        ShallowClassification::Irrelevant => Ok(None),
    }
}

/// Parse the top-level group-broadcast branch of the delivery boundary.
fn parse_group_broadcast_frame_hint(
    frame: &proto::GroupBroadcastFrameView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let body = frame.body.as_ref().context(MissingOneofSnafu {
        name: "GroupBroadcastFrame.body",
    })?;
    match body {
        proto::group_broadcast_frame::BodyView::Envelope(envelope) => {
            parse_group_envelope_hint(envelope, interest)
        }
        proto::group_broadcast_frame::BodyView::RelayStoreConfirmation(confirmation) => {
            parse_group_relay_store_confirmation_hint(confirmation, interest)
        }
    }
}

/// Parse the top-level reliable-delivery branch of the delivery boundary.
fn parse_reliable_delivery_frame_hint(
    frame: &proto::ReliableDeliveryFrameView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let body = frame.body.as_ref().context(MissingOneofSnafu {
        name: "ReliableDeliveryFrame.body",
    })?;
    match body {
        proto::reliable_delivery_frame::BodyView::Envelope(envelope) => {
            parse_reliable_envelope_hint(envelope, interest)
        }
        proto::reliable_delivery_frame::BodyView::RecipientAck(ack) => {
            parse_recipient_ack_hint(ack, interest)
        }
        proto::reliable_delivery_frame::BodyView::MailboxFetch(fetch) => {
            parse_mailbox_fetch_hint(fetch, interest)
        }
        proto::reliable_delivery_frame::BodyView::MailboxBatch(batch) => {
            parse_mailbox_batch_hint(batch, interest)
        }
        proto::reliable_delivery_frame::BodyView::MailboxAck(ack) => {
            parse_mailbox_ack_hint(ack, interest)
        }
        proto::reliable_delivery_frame::BodyView::RelayStoreConfirmation(confirmation) => {
            parse_reliable_relay_store_confirmation_hint(confirmation, interest)
        }
    }
}

/// Parse the minimal public header of a group envelope.
///
/// A group envelope is relevant only when the addressed group is currently
/// active locally.
fn parse_group_envelope_hint(
    envelope: &proto::GroupEnvelopeWireView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let header = envelope
        .public_header
        .as_option()
        .context(MissingFieldSnafu {
            message: "GroupEnvelopeWire",
            field: "public_header",
        })?;
    let group_id = group_id_from_wire(header.group_id, "GroupEnvelopeHeader.group_id")?;
    let delivery_message_id =
        message_id_from_wire(header.message_id, "GroupEnvelopeHeader.message_id")?;
    if let Some(classification) = interest.check_group(&group_id) {
        return Ok(classification);
    }
    Ok(ShallowClassification::Relevant {
        owner: SemanticOwner::GroupBroadcast,
        target: DeliveryTargetHint::GroupBroadcast {
            group_id,
            delivery_message_id,
        },
    })
}

/// Parse the minimal public header of a group relay-store confirmation.
///
/// These confirmations are only relevant to the original sender that may still
/// be correlating the relay-store result locally.
fn parse_group_relay_store_confirmation_hint(
    confirmation: &proto::GroupRelayStoreConfirmationWireView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let header = confirmation
        .public_header
        .as_option()
        .context(MissingFieldSnafu {
            message: "GroupRelayStoreConfirmationWire",
            field: "public_header",
        })?;
    let original_sender = member_identity_from_wire_view(
        header
            .original_sender
            .as_option()
            .context(MissingFieldSnafu {
                message: "GroupRelayStoreConfirmationHeader",
                field: "original_sender",
            })?,
        "GroupRelayStoreConfirmationHeader.original_sender",
    )?;
    if let Some(classification) = interest.check_local_member(&original_sender) {
        return Ok(classification);
    }
    Ok(ShallowClassification::Relevant {
        owner: SemanticOwner::GroupBroadcast,
        target: DeliveryTargetHint::OriginalSender {
            original_sender,
            delivery_message_id: message_id_from_wire(
                header.message_id,
                "GroupRelayStoreConfirmationHeader.message_id",
            )?,
        },
    })
}

/// Parse the minimal public header of a reliable envelope.
fn parse_reliable_envelope_hint(
    envelope: &proto::ReliableEnvelopeWireView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let header = envelope
        .public_header
        .as_option()
        .context(MissingFieldSnafu {
            message: "ReliableEnvelopeWire",
            field: "public_header",
        })?;
    let recipient = member_identity_from_wire_view(
        header.recipient.as_option().context(MissingFieldSnafu {
            message: "ReliableEnvelopeHeader",
            field: "recipient",
        })?,
        "ReliableEnvelopeHeader.recipient",
    )?;
    if let Some(classification) = interest.check_local_member(&recipient) {
        return Ok(classification);
    }
    Ok(ShallowClassification::Relevant {
        owner: SemanticOwner::ReliableDelivery,
        target: DeliveryTargetHint::ReliableRecipient {
            recipient,
            delivery_message_id: Some(message_id_from_wire(
                header.message_id,
                "ReliableEnvelopeHeader.message_id",
            )?),
        },
    })
}

/// Parse the minimal public header of a recipient acknowledgement.
fn parse_recipient_ack_hint(
    ack: &proto::RecipientAckWireView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let header = ack.public_header.as_option().context(MissingFieldSnafu {
        message: "RecipientAckWire",
        field: "public_header",
    })?;
    let original_sender = member_identity_from_wire_view(
        header
            .original_sender
            .as_option()
            .context(MissingFieldSnafu {
                message: "RecipientAckHeader",
                field: "original_sender",
            })?,
        "RecipientAckHeader.original_sender",
    )?;
    if let Some(classification) = interest.check_local_member(&original_sender) {
        return Ok(classification);
    }
    Ok(ShallowClassification::Relevant {
        owner: SemanticOwner::ReliableDelivery,
        target: DeliveryTargetHint::OriginalSender {
            original_sender,
            delivery_message_id: message_id_from_wire(
                header.message_id,
                "RecipientAckHeader.message_id",
            )?,
        },
    })
}

/// Parse the minimal public header of a mailbox fetch request.
fn parse_mailbox_fetch_hint(
    fetch: &proto::MailboxFetchWireView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let header = fetch.public_header.as_option().context(MissingFieldSnafu {
        message: "MailboxFetchWire",
        field: "public_header",
    })?;
    let recipient = member_identity_from_wire_view(
        header.recipient.as_option().context(MissingFieldSnafu {
            message: "MailboxFetchHeader",
            field: "recipient",
        })?,
        "MailboxFetchHeader.recipient",
    )?;
    if let Some(classification) = interest.check_hosted_mailbox(&recipient) {
        return Ok(classification);
    }
    Ok(ShallowClassification::Relevant {
        owner: SemanticOwner::ReliableDelivery,
        target: DeliveryTargetHint::HostedMailbox { recipient },
    })
}

/// Parse the minimal public header of a mailbox batch response.
///
/// Mailbox batches are recipient-scoped, but the batch as a whole does not map
/// to one delivery-domain message id, so the resulting target hint carries
/// `delivery_message_id = None`.
fn parse_mailbox_batch_hint(
    batch: &proto::MailboxBatchWireView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let header = batch.public_header.as_option().context(MissingFieldSnafu {
        message: "MailboxBatchWire",
        field: "public_header",
    })?;
    let recipient = member_identity_from_wire_view(
        header.recipient.as_option().context(MissingFieldSnafu {
            message: "MailboxBatchHeader",
            field: "recipient",
        })?,
        "MailboxBatchHeader.recipient",
    )?;
    if let Some(classification) = interest.check_local_member(&recipient) {
        return Ok(classification);
    }
    Ok(ShallowClassification::Relevant {
        owner: SemanticOwner::ReliableDelivery,
        target: DeliveryTargetHint::ReliableRecipient {
            recipient,
            delivery_message_id: None,
        },
    })
}

/// Parse the minimal public header of a mailbox acknowledgement.
fn parse_mailbox_ack_hint(
    ack: &proto::MailboxAckWireView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let header = ack.public_header.as_option().context(MissingFieldSnafu {
        message: "MailboxAckWire",
        field: "public_header",
    })?;
    let recipient = member_identity_from_wire_view(
        header.recipient.as_option().context(MissingFieldSnafu {
            message: "MailboxAckHeader",
            field: "recipient",
        })?,
        "MailboxAckHeader.recipient",
    )?;
    if let Some(classification) = interest.check_hosted_mailbox(&recipient) {
        return Ok(classification);
    }
    Ok(ShallowClassification::Relevant {
        owner: SemanticOwner::ReliableDelivery,
        target: DeliveryTargetHint::HostedMailbox { recipient },
    })
}

/// Parse the minimal public header of a reliable relay-store confirmation.
fn parse_reliable_relay_store_confirmation_hint(
    confirmation: &proto::ReliableRelayStoreConfirmationWireView<'_>,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    let header = confirmation
        .public_header
        .as_option()
        .context(MissingFieldSnafu {
            message: "ReliableRelayStoreConfirmationWire",
            field: "public_header",
        })?;
    let original_sender = member_identity_from_wire_view(
        header
            .original_sender
            .as_option()
            .context(MissingFieldSnafu {
                message: "ReliableRelayStoreConfirmationHeader",
                field: "original_sender",
            })?,
        "ReliableRelayStoreConfirmationHeader.original_sender",
    )?;
    if let Some(classification) = interest.check_local_member(&original_sender) {
        return Ok(classification);
    }
    Ok(ShallowClassification::Relevant {
        owner: SemanticOwner::ReliableDelivery,
        target: DeliveryTargetHint::OriginalSender {
            original_sender,
            delivery_message_id: message_id_from_wire(
                header.message_id,
                "ReliableRelayStoreConfirmationHeader.message_id",
            )?,
        },
    })
}

/// Decode the full boundary frame once shallow classification has already said
/// the payload is locally relevant.
fn decode_full_boundary_frame(
    payload: &[u8],
) -> Result<proto::delivery_boundary_frame::Boundary, DeliveryWireError> {
    let boundary = proto::DeliveryBoundaryFrame::decode_from_slice(payload).context(DecodeSnafu)?;
    boundary.boundary.context(MissingOneofSnafu {
        name: "DeliveryBoundaryFrame.boundary",
    })
}

/// Decode one member identifier from the generated discovery protobuf shape
/// into the local identifier type used by the delivery domain.
fn member_identity_from_wire_view(
    identifier: &discovery_proto::IdentifierView<'_>,
    field: &'static str,
) -> Result<MemberIdentity, WireValueDecodeError> {
    let mut buffer = IdentifierBuf::new();
    for segment in &identifier.segments {
        let segment = segment.to_owned();
        buffer
            .push_checked(segment.to_owned())
            .context(InvalidIdentifierSegmentSnafu { field, segment })?;
    }
    Ok(buffer.into_identifier())
}

/// Shared field-level decode failures reused across semantic delivery wire
/// adapters and the shallow ingress classifier.
#[derive(Debug, Snafu)]
pub(crate) enum WireValueDecodeError {
    #[snafu(display("Field '{field}' did not contain a valid UUID: {source}"))]
    InvalidUuid {
        field: &'static str,
        source: uuid::Error,
    },

    #[snafu(display(
        "Field '{field}' contains an invalid identifier segment '{segment}': {source}"
    ))]
    InvalidIdentifierSegment {
        field: &'static str,
        segment: String,
        source: IdentifierError,
    },

    #[snafu(display("Field '{field}' used an unknown signature scheme value {value}"))]
    UnknownSignatureScheme { field: &'static str, value: i32 },

    #[snafu(display("Field '{field}' used the unspecified signature scheme"))]
    UnspecifiedSignatureScheme { field: &'static str },

    #[snafu(display("Field '{field}' had invalid byte length {actual}; expected {expected}."))]
    InvalidByteLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
}

fn uuid_from_wire(raw: &[u8], field: &'static str) -> Result<Uuid, WireValueDecodeError> {
    Uuid::from_slice(raw).context(InvalidUuidSnafu { field })
}

/// Semantic owner for one delivery boundary branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SemanticOwner {
    GroupBroadcast,
    ReliableDelivery,
}

impl std::fmt::Display for SemanticOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GroupBroadcast => write!(f, "group_broadcast"),
            Self::ReliableDelivery => write!(f, "reliable_delivery"),
        }
    }
}

/// Result of shallow classification before full protobuf decode.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ShallowClassification {
    /// The frame is locally relevant and should proceed to full decode.
    Relevant {
        owner: SemanticOwner,
        target: DeliveryTargetHint,
    },
    /// The frame is structurally valid enough to classify, but the local node
    /// should intentionally ignore it.
    Irrelevant,
}

/// Shallow-classification output for frames that survived the local-interest
/// filter.
#[derive(Clone, Debug)]
struct RelevantShallowClassification {
    owner: SemanticOwner,
    target: DeliveryTargetHint,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use flotsync_messages::buffa::{Message, MessageField};

    fn group_id(value: u128) -> GroupId {
        GroupId(Uuid::from_u128(value))
    }

    fn message_id(value: u128) -> MessageId {
        MessageId(Uuid::from_u128(value))
    }

    fn member(segments: &[&str]) -> MemberIdentity {
        let mut buffer = IdentifierBuf::new();
        for segment in segments {
            buffer
                .push_checked((*segment).to_owned())
                .expect("test identifier must be valid");
        }
        buffer.into_identifier()
    }

    fn proto_identifier(segments: &[&str]) -> discovery_proto::Identifier {
        let segments = segments
            .iter()
            .map(|segment| (*segment).to_owned())
            .collect();
        discovery_proto::Identifier {
            segments,
            ..discovery_proto::Identifier::default()
        }
    }

    fn encode_boundary_frame(frame: &proto::DeliveryBoundaryFrame) -> IoPayload {
        IoPayload::from(frame.encode_to_bytes())
    }

    fn group_memberships(groups: impl IntoIterator<Item = GroupId>) -> GroupMemberships {
        let groups = groups.into_iter().map(|group_id| {
            (
                group_id,
                crate::GroupMembers::from_ordered_members([member(&["probe"])])
                    .expect("probe group members should build"),
            )
        });
        GroupMemberships::from_groups(groups)
    }

    fn members(values: impl IntoIterator<Item = MemberIdentity>) -> HashSet<MemberIdentity> {
        values.into_iter().collect()
    }

    #[test]
    fn irrelevant_group_envelope_is_dropped_before_full_decode() {
        let group_id = group_id(1);
        let delivery_message_id = message_id(2);
        let header = proto::GroupEnvelopeHeader {
            group_id: group_id.0.as_bytes().to_vec(),
            sender: MessageField::some(proto_identifier(&["alice"])),
            message_id: delivery_message_id.0.as_bytes().to_vec(),
            ..proto::GroupEnvelopeHeader::default()
        };
        let envelope = proto::GroupEnvelopeWire {
            public_header: MessageField::some(header),
            sealed_payload: MessageField::some(proto::SealedPSKPayload {
                ciphertext: Bytes::from(vec![0x5a; 32 * 1024]),
                signature: vec![0; 64],
                ..proto::SealedPSKPayload::default()
            }),
            ..proto::GroupEnvelopeWire::default()
        };
        let frame = proto::GroupBroadcastFrame {
            body: Some(proto::group_broadcast_frame::Body::Envelope(Box::new(
                envelope,
            ))),
            ..proto::GroupBroadcastFrame::default()
        };
        let boundary = proto::DeliveryBoundaryFrame {
            boundary: Some(proto::delivery_boundary_frame::Boundary::GroupBroadcast(
                Box::new(frame),
            )),
            ..proto::DeliveryBoundaryFrame::default()
        };

        let group_memberships = group_memberships([]);
        let local_members = members([]);
        let hosted_mailboxes = members([]);
        let mut payload = encode_boundary_frame(&boundary);
        let decoded = decode_boundary_frame_if_interested(
            &mut payload,
            DeliveryInterestView {
                group_memberships: &group_memberships,
                local_members: &local_members,
                hosted_mailboxes: &hosted_mailboxes,
            },
        )
        .expect("inactive group should be dropped cleanly");

        assert_eq!(decoded, None);
    }

    #[test]
    fn relevant_group_envelope_decodes_full_group_frame() {
        let group_id = group_id(11);
        let delivery_message_id = message_id(12);
        let header = proto::GroupEnvelopeHeader {
            group_id: group_id.0.as_bytes().to_vec(),
            sender: MessageField::some(proto_identifier(&["alice"])),
            message_id: delivery_message_id.0.as_bytes().to_vec(),
            ..proto::GroupEnvelopeHeader::default()
        };
        let envelope = proto::GroupEnvelopeWire {
            public_header: MessageField::some(header),
            sealed_payload: MessageField::some(proto::SealedPSKPayload {
                ciphertext: Bytes::from_static(b"ciphertext"),
                signature: vec![0; 64],
                ..proto::SealedPSKPayload::default()
            }),
            ..proto::GroupEnvelopeWire::default()
        };
        let frame = proto::GroupBroadcastFrame {
            body: Some(proto::group_broadcast_frame::Body::Envelope(Box::new(
                envelope.clone(),
            ))),
            ..proto::GroupBroadcastFrame::default()
        };
        let boundary = proto::DeliveryBoundaryFrame {
            boundary: Some(proto::delivery_boundary_frame::Boundary::GroupBroadcast(
                Box::new(frame.clone()),
            )),
            ..proto::DeliveryBoundaryFrame::default()
        };

        let group_memberships = group_memberships([group_id]);
        let local_members = members([]);
        let hosted_mailboxes = members([]);
        let mut payload = encode_boundary_frame(&boundary);
        let decoded = decode_boundary_frame_if_interested(
            &mut payload,
            DeliveryInterestView {
                group_memberships: &group_memberships,
                local_members: &local_members,
                hosted_mailboxes: &hosted_mailboxes,
            },
        )
        .expect("active group frame should decode")
        .expect("active group frame should be kept");

        assert_eq!(
            decoded,
            DecodedDeliveryFrame::GroupBroadcast {
                target: DeliveryTargetHint::GroupBroadcast {
                    group_id,
                    delivery_message_id,
                },
                frame,
            }
        );
    }

    #[test]
    fn recipient_ack_for_non_local_sender_is_dropped() {
        let delivery_message_id = message_id(21);
        let header = proto::RecipientAckHeader {
            message_id: delivery_message_id.0.as_bytes().to_vec(),
            original_sender: MessageField::some(proto_identifier(&["alice"])),
            recipient: MessageField::some(proto_identifier(&["bob"])),
            ..proto::RecipientAckHeader::default()
        };
        let ack = proto::RecipientAckWire {
            public_header: MessageField::some(header),
            ..proto::RecipientAckWire::default()
        };
        let frame = proto::ReliableDeliveryFrame {
            body: Some(proto::reliable_delivery_frame::Body::RecipientAck(
                Box::new(ack),
            )),
            ..proto::ReliableDeliveryFrame::default()
        };
        let boundary = proto::DeliveryBoundaryFrame {
            boundary: Some(proto::delivery_boundary_frame::Boundary::ReliableDelivery(
                Box::new(frame),
            )),
            ..proto::DeliveryBoundaryFrame::default()
        };

        let group_memberships = group_memberships([]);
        let local_members = members([member(&["charlie"])]);
        let hosted_mailboxes = members([]);
        let mut payload = encode_boundary_frame(&boundary);
        let decoded = decode_boundary_frame_if_interested(
            &mut payload,
            DeliveryInterestView {
                group_memberships: &group_memberships,
                local_members: &local_members,
                hosted_mailboxes: &hosted_mailboxes,
            },
        )
        .expect("irrelevant recipient ack should be dropped cleanly");

        assert_eq!(decoded, None);
    }

    #[test]
    fn mailbox_fetch_for_hosted_mailbox_routes_to_reliable_owner() {
        let recipient = member(&["alice"]);
        let header = proto::MailboxFetchHeader {
            recipient: MessageField::some(proto_identifier(&["alice"])),
            freshness_token: Uuid::from_u128(31).as_bytes().to_vec(),
            ..proto::MailboxFetchHeader::default()
        };
        let fetch = proto::MailboxFetchWire {
            public_header: MessageField::some(header),
            ..proto::MailboxFetchWire::default()
        };
        let frame = proto::ReliableDeliveryFrame {
            body: Some(proto::reliable_delivery_frame::Body::MailboxFetch(
                Box::new(fetch.clone()),
            )),
            ..proto::ReliableDeliveryFrame::default()
        };
        let boundary = proto::DeliveryBoundaryFrame {
            boundary: Some(proto::delivery_boundary_frame::Boundary::ReliableDelivery(
                Box::new(frame.clone()),
            )),
            ..proto::DeliveryBoundaryFrame::default()
        };

        let group_memberships = group_memberships([]);
        let local_members = members([]);
        let hosted_mailboxes = members([recipient.clone()]);
        let mut payload = encode_boundary_frame(&boundary);
        let decoded = decode_boundary_frame_if_interested(
            &mut payload,
            DeliveryInterestView {
                group_memberships: &group_memberships,
                local_members: &local_members,
                hosted_mailboxes: &hosted_mailboxes,
            },
        )
        .expect("hosted mailbox frame should decode")
        .expect("hosted mailbox frame should be kept");

        assert_eq!(
            decoded,
            DecodedDeliveryFrame::ReliableDelivery {
                target: DeliveryTargetHint::HostedMailbox { recipient },
                frame,
            }
        );
    }
}

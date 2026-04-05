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

use super::{ingress::DeliveryTargetHint, shared::MessageId};
use crate::api::{GroupId, MemberIdentity};
use flotsync_core::member::{IdentifierBuf, IdentifierError};
use flotsync_io::prelude::IoPayload;
use flotsync_messages::{
    delivery as proto,
    discovery as discovery_proto,
    protobuf::{self, CodedInputStream, Message, MessageField, rt::WireType},
};
use proto::delivery_boundary_frame::Boundary;
use snafu::prelude::*;
use std::collections::HashSet;
use uuid::Uuid;

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
    payload: &IoPayload,
    interest: DeliveryInterestView<'_>,
) -> Result<Option<DecodedDeliveryFrame>, DeliveryWireError> {
    let Some(classification) = classify_boundary_frame(payload, interest)? else {
        return Ok(None);
    };

    let decoded = decode_full_boundary_frame(payload)?;
    match (classification.owner, decoded) {
        (SemanticOwner::GroupBroadcast, Boundary::GroupBroadcast(frame)) => {
            Ok(Some(DecodedDeliveryFrame::GroupBroadcast {
                target: classification.target,
                frame,
            }))
        }
        (SemanticOwner::ReliableDelivery, Boundary::ReliableDelivery(frame)) => {
            Ok(Some(DecodedDeliveryFrame::ReliableDelivery {
                target: classification.target,
                frame,
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
        (SemanticOwner::GroupBroadcast, _) => BoundaryOwnerMismatchSnafu {
            shallow_owner: SemanticOwner::GroupBroadcast,
            full_owner: SemanticOwner::UnsupportedBoundary,
        }
        .fail(),
        (SemanticOwner::ReliableDelivery, _) => BoundaryOwnerMismatchSnafu {
            shallow_owner: SemanticOwner::ReliableDelivery,
            full_owner: SemanticOwner::UnsupportedBoundary,
        }
        .fail(),
        (SemanticOwner::UnsupportedBoundary, _) => unreachable!(
            "shallow classification never assigns UnsupportedBoundary as the expected owner"
        ),
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
    pub active_groups: &'a HashSet<GroupId>,
    pub local_members: &'a HashSet<MemberIdentity>,
    pub hosted_mailboxes: &'a HashSet<MemberIdentity>,
}

impl DeliveryInterestView<'_> {
    /// Keep the classification flowing only when the given group is active
    /// locally.
    fn check_group(self, group_id: &GroupId) -> Option<ShallowClassification> {
        (!self.active_groups.contains(group_id)).then_some(ShallowClassification::Irrelevant)
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
    #[snafu(display("Failed to parse delivery protobuf payload: {source}"))]
    Protobuf { source: protobuf::Error },

    #[snafu(display(
        "Field {field_number} in {context} used wire type {actual:?}, expected {expected:?}."
    ))]
    UnexpectedWireType {
        context: &'static str,
        field_number: u32,
        expected: WireType,
        actual: WireType,
    },

    #[snafu(display("Protobuf message '{message}' is missing required field '{field}'."))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },

    #[snafu(display("Protobuf oneof '{name}' has no selected value."))]
    MissingOneof { name: &'static str },

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

    #[snafu(display(
        "Shallow delivery classifier selected {shallow_owner}, but full protobuf decode selected {full_owner}."
    ))]
    BoundaryOwnerMismatch {
        shallow_owner: SemanticOwner,
        full_owner: SemanticOwner,
    },

    #[snafu(display(
        "Encountered invalid protobuf tag value {tag} while classifying a delivery frame."
    ))]
    InvalidTag { tag: u32 },
}

/// Local helper converting generated protobuf option fields into the error
/// style used by this module.
trait RequiredMessageFieldExt<T> {
    fn take_required(
        &mut self,
        message: &'static str,
        field: &'static str,
    ) -> Result<T, DeliveryWireError>;
}

impl<T> RequiredMessageFieldExt<T> for MessageField<T> {
    fn take_required(
        &mut self,
        message: &'static str,
        field: &'static str,
    ) -> Result<T, DeliveryWireError> {
        self.take().context(MissingFieldSnafu { message, field })
    }
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
    payload: &IoPayload,
    interest: DeliveryInterestView<'_>,
) -> Result<Option<RelevantShallowClassification>, DeliveryWireError> {
    let mut cursor = payload.cursor();
    let mut input = CodedInputStream::new(&mut cursor);
    let mut classification = None;

    while !input.eof().context(ProtobufSnafu)? {
        let (field_number, wire_type) = read_tag(&mut input)?;
        classification = match field_number {
            1 => Some(parse_group_broadcast_frame_hint(
                &mut input, wire_type, interest,
            )?),
            2 => Some(parse_reliable_delivery_frame_hint(
                &mut input, wire_type, interest,
            )?),
            _ => {
                input.skip_field(wire_type).context(ProtobufSnafu)?;
                classification
            }
        };
    }

    match classification.context(MissingOneofSnafu {
        name: "DeliveryBoundaryFrame.boundary",
    })? {
        ShallowClassification::Relevant { owner, target } => {
            Ok(Some(RelevantShallowClassification { owner, target }))
        }
        ShallowClassification::Irrelevant => Ok(None),
    }
}

/// Parse the top-level group-broadcast branch of the delivery boundary.
fn parse_group_broadcast_frame_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("DeliveryBoundaryFrame", 1, wire_type)?;
    read_length_delimited(input, |input| {
        let mut selection = None;
        while !input.eof().context(ProtobufSnafu)? {
            let (field_number, wire_type) = read_tag(input)?;
            selection = match field_number {
                1 => Some(parse_group_envelope_hint(input, wire_type, interest)?),
                2 => Some(parse_group_relay_store_confirmation_hint(
                    input, wire_type, interest,
                )?),
                _ => {
                    input.skip_field(wire_type).context(ProtobufSnafu)?;
                    selection
                }
            };
        }
        selection.context(MissingOneofSnafu {
            name: "GroupBroadcastFrame.body",
        })
    })
}

/// Parse the top-level reliable-delivery branch of the delivery boundary.
fn parse_reliable_delivery_frame_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("DeliveryBoundaryFrame", 2, wire_type)?;
    read_length_delimited(input, |input| {
        let mut selection = None;
        while !input.eof().context(ProtobufSnafu)? {
            let (field_number, wire_type) = read_tag(input)?;
            selection = match field_number {
                1 => Some(parse_reliable_envelope_hint(input, wire_type, interest)?),
                2 => Some(parse_recipient_ack_hint(input, wire_type, interest)?),
                3 => Some(parse_mailbox_fetch_hint(input, wire_type, interest)?),
                4 => Some(parse_mailbox_batch_hint(input, wire_type, interest)?),
                5 => Some(parse_mailbox_ack_hint(input, wire_type, interest)?),
                6 => Some(parse_reliable_relay_store_confirmation_hint(
                    input, wire_type, interest,
                )?),
                _ => {
                    input.skip_field(wire_type).context(ProtobufSnafu)?;
                    selection
                }
            };
        }
        selection.context(MissingOneofSnafu {
            name: "ReliableDeliveryFrame.body",
        })
    })
}

/// Parse the minimal public header of a group envelope.
///
/// A group envelope is relevant only when the addressed group is currently
/// active locally.
fn parse_group_envelope_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("GroupBroadcastFrame", 1, wire_type)?;
    read_length_delimited(input, |input| {
        let header = take_group_header(input, "GroupEnvelopeWire", "public_header")?;
        let group_id = decode_group_id(header.group_id, "GroupEnvelopeHeader.group_id")?;
        let delivery_message_id =
            decode_message_id(header.message_id, "GroupEnvelopeHeader.message_id")?;
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
    })
}

/// Parse the minimal public header of a group relay-store confirmation.
///
/// These confirmations are only relevant to the original sender that may still
/// be correlating the relay-store result locally.
fn parse_group_relay_store_confirmation_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("GroupBroadcastFrame", 2, wire_type)?;
    read_length_delimited(input, |input| {
        let mut header = take_group_relay_store_confirmation_header(
            input,
            "GroupRelayStoreConfirmationWire",
            "public_header",
        )?;
        let original_sender = decode_member_identity(
            header
                .original_sender
                .take_required("GroupRelayStoreConfirmationHeader", "original_sender")?,
            "GroupRelayStoreConfirmationHeader.original_sender",
        )?;
        if let Some(classification) = interest.check_local_member(&original_sender) {
            return Ok(classification);
        }
        Ok(ShallowClassification::Relevant {
            owner: SemanticOwner::GroupBroadcast,
            target: DeliveryTargetHint::OriginalSender {
                original_sender,
                delivery_message_id: decode_message_id(
                    header.message_id,
                    "GroupRelayStoreConfirmationHeader.message_id",
                )?,
            },
        })
    })
}

/// Parse the minimal public header of a reliable envelope.
fn parse_reliable_envelope_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("ReliableDeliveryFrame", 1, wire_type)?;
    read_length_delimited(input, |input| {
        let mut header = take_reliable_header(input, "ReliableEnvelopeWire", "public_header")?;
        let recipient = decode_member_identity(
            header
                .recipient
                .take_required("ReliableEnvelopeHeader", "recipient")?,
            "ReliableEnvelopeHeader.recipient",
        )?;
        if let Some(classification) = interest.check_local_member(&recipient) {
            return Ok(classification);
        }
        Ok(ShallowClassification::Relevant {
            owner: SemanticOwner::ReliableDelivery,
            target: DeliveryTargetHint::ReliableRecipient {
                recipient,
                delivery_message_id: Some(decode_message_id(
                    header.message_id,
                    "ReliableEnvelopeHeader.message_id",
                )?),
            },
        })
    })
}

/// Parse the minimal public header of a recipient acknowledgement.
fn parse_recipient_ack_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("ReliableDeliveryFrame", 2, wire_type)?;
    read_length_delimited(input, |input| {
        let mut header = take_recipient_ack_header(input, "RecipientAckWire", "public_header")?;
        let original_sender = decode_member_identity(
            header
                .original_sender
                .take_required("RecipientAckHeader", "original_sender")?,
            "RecipientAckHeader.original_sender",
        )?;
        if let Some(classification) = interest.check_local_member(&original_sender) {
            return Ok(classification);
        }
        Ok(ShallowClassification::Relevant {
            owner: SemanticOwner::ReliableDelivery,
            target: DeliveryTargetHint::OriginalSender {
                original_sender,
                delivery_message_id: decode_message_id(
                    header.message_id,
                    "RecipientAckHeader.message_id",
                )?,
            },
        })
    })
}

/// Parse the minimal public header of a mailbox fetch request.
fn parse_mailbox_fetch_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("ReliableDeliveryFrame", 3, wire_type)?;
    read_length_delimited(input, |input| {
        let mut header = take_mailbox_fetch_header(input, "MailboxFetchWire", "public_header")?;
        let recipient = decode_member_identity(
            header
                .recipient
                .take_required("MailboxFetchHeader", "recipient")?,
            "MailboxFetchHeader.recipient",
        )?;
        if let Some(classification) = interest.check_hosted_mailbox(&recipient) {
            return Ok(classification);
        }
        Ok(ShallowClassification::Relevant {
            owner: SemanticOwner::ReliableDelivery,
            target: DeliveryTargetHint::HostedMailbox { recipient },
        })
    })
}

/// Parse the minimal public header of a mailbox batch response.
///
/// Mailbox batches are recipient-scoped, but the batch as a whole does not map
/// to one delivery-domain message id, so the resulting target hint carries
/// `delivery_message_id = None`.
fn parse_mailbox_batch_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("ReliableDeliveryFrame", 4, wire_type)?;
    read_length_delimited(input, |input| {
        let mut header = take_mailbox_batch_header(input, "MailboxBatchWire", "public_header")?;
        let recipient = decode_member_identity(
            header
                .recipient
                .take_required("MailboxBatchHeader", "recipient")?,
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
    })
}

/// Parse the minimal public header of a mailbox acknowledgement.
fn parse_mailbox_ack_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("ReliableDeliveryFrame", 5, wire_type)?;
    read_length_delimited(input, |input| {
        let mut header = take_mailbox_ack_header(input, "MailboxAckWire", "public_header")?;
        let recipient = decode_member_identity(
            header
                .recipient
                .take_required("MailboxAckHeader", "recipient")?,
            "MailboxAckHeader.recipient",
        )?;
        if let Some(classification) = interest.check_hosted_mailbox(&recipient) {
            return Ok(classification);
        }
        Ok(ShallowClassification::Relevant {
            owner: SemanticOwner::ReliableDelivery,
            target: DeliveryTargetHint::HostedMailbox { recipient },
        })
    })
}

/// Parse the minimal public header of a reliable relay-store confirmation.
fn parse_reliable_relay_store_confirmation_hint(
    input: &mut CodedInputStream<'_>,
    wire_type: WireType,
    interest: DeliveryInterestView<'_>,
) -> Result<ShallowClassification, DeliveryWireError> {
    expect_message_wire_type("ReliableDeliveryFrame", 6, wire_type)?;
    read_length_delimited(input, |input| {
        let mut header = take_reliable_relay_store_confirmation_header(
            input,
            "ReliableRelayStoreConfirmationWire",
            "public_header",
        )?;
        let original_sender = decode_member_identity(
            header
                .original_sender
                .take_required("ReliableRelayStoreConfirmationHeader", "original_sender")?,
            "ReliableRelayStoreConfirmationHeader.original_sender",
        )?;
        if let Some(classification) = interest.check_local_member(&original_sender) {
            return Ok(classification);
        }
        Ok(ShallowClassification::Relevant {
            owner: SemanticOwner::ReliableDelivery,
            target: DeliveryTargetHint::OriginalSender {
                original_sender,
                delivery_message_id: decode_message_id(
                    header.message_id,
                    "ReliableRelayStoreConfirmationHeader.message_id",
                )?,
            },
        })
    })
}

fn take_group_header(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
) -> Result<proto::GroupEnvelopeHeader, DeliveryWireError> {
    take_public_header(input, message_name, field_name, 1)
}

fn take_reliable_header(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
) -> Result<proto::ReliableEnvelopeHeader, DeliveryWireError> {
    take_public_header(input, message_name, field_name, 1)
}

fn take_recipient_ack_header(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
) -> Result<proto::RecipientAckHeader, DeliveryWireError> {
    take_public_header(input, message_name, field_name, 1)
}

fn take_mailbox_fetch_header(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
) -> Result<proto::MailboxFetchHeader, DeliveryWireError> {
    take_public_header(input, message_name, field_name, 1)
}

fn take_mailbox_batch_header(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
) -> Result<proto::MailboxBatchHeader, DeliveryWireError> {
    take_public_header(input, message_name, field_name, 1)
}

fn take_mailbox_ack_header(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
) -> Result<proto::MailboxAckHeader, DeliveryWireError> {
    take_public_header(input, message_name, field_name, 1)
}

fn take_group_relay_store_confirmation_header(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
) -> Result<proto::GroupRelayStoreConfirmationHeader, DeliveryWireError> {
    take_public_header(input, message_name, field_name, 1)
}

fn take_reliable_relay_store_confirmation_header(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
) -> Result<proto::ReliableRelayStoreConfirmationHeader, DeliveryWireError> {
    take_public_header(input, message_name, field_name, 1)
}

/// Read exactly the public header field from one generated delivery-wire
/// message, skipping any encrypted payload or footer fields.
///
/// This is the core of the shallow-classification pass: it lets ingress make a
/// routing decision without first decoding the entire message.
fn take_public_header<M>(
    input: &mut CodedInputStream<'_>,
    message_name: &'static str,
    field_name: &'static str,
    public_header_field_number: u32,
) -> Result<M, DeliveryWireError>
where
    M: Message,
{
    let mut header = None;
    while !input.eof().context(ProtobufSnafu)? {
        let (field_number, wire_type) = read_tag(input)?;
        if field_number == public_header_field_number {
            expect_message_wire_type(message_name, field_number, wire_type)?;
            header = Some(input.read_message::<M>().context(ProtobufSnafu)?);
            continue;
        }
        input.skip_field(wire_type).context(ProtobufSnafu)?;
    }
    header.context(MissingFieldSnafu {
        message: message_name,
        field: field_name,
    })
}

/// Read one raw protobuf tag and split it into field number and wire type.
fn read_tag(input: &mut CodedInputStream<'_>) -> Result<(u32, WireType), DeliveryWireError> {
    let raw_tag = input.read_raw_varint32().context(ProtobufSnafu)?;
    let wire_type = WireType::new(raw_tag & 0x07).context(InvalidTagSnafu { tag: raw_tag })?;
    let field_number = raw_tag >> 3;
    if field_number == 0 {
        return InvalidTagSnafu { tag: raw_tag }.fail();
    }
    Ok((field_number, wire_type))
}

/// Validate that one protobuf field really is an embedded message.
fn expect_message_wire_type(
    context: &'static str,
    field_number: u32,
    actual: WireType,
) -> Result<(), DeliveryWireError> {
    if actual != WireType::LengthDelimited {
        return UnexpectedWireTypeSnafu {
            context,
            field_number,
            expected: WireType::LengthDelimited,
            actual,
        }
        .fail();
    }
    Ok(())
}

/// Decode the full boundary frame once shallow classification has already said
/// the payload is locally relevant.
fn decode_full_boundary_frame(
    payload: &IoPayload,
) -> Result<proto::delivery_boundary_frame::Boundary, DeliveryWireError> {
    let mut cursor = payload.cursor();
    let boundary =
        proto::DeliveryBoundaryFrame::parse_from_reader(&mut cursor).context(ProtobufSnafu)?;
    boundary.boundary.context(MissingOneofSnafu {
        name: "DeliveryBoundaryFrame.boundary",
    })
}

/// Run one nested parser inside the length limit of a length-delimited
/// protobuf message field.
fn read_length_delimited<T>(
    input: &mut CodedInputStream<'_>,
    f: impl FnOnce(&mut CodedInputStream<'_>) -> Result<T, DeliveryWireError>,
) -> Result<T, DeliveryWireError> {
    let len = input.read_raw_varint64().context(ProtobufSnafu)?;
    let old_limit = input.push_limit(len).context(ProtobufSnafu)?;
    let result = f(input);
    input.pop_limit(old_limit);
    result
}

/// Decode one group id encoded as raw UUID bytes in the wire format.
fn decode_group_id(raw: Vec<u8>, field: &'static str) -> Result<GroupId, DeliveryWireError> {
    Ok(GroupId(decode_uuid(raw, field)?))
}

/// Decode one delivery-domain message id encoded as raw UUID bytes in the wire
/// format.
fn decode_message_id(raw: Vec<u8>, field: &'static str) -> Result<MessageId, DeliveryWireError> {
    Ok(MessageId(decode_uuid(raw, field)?))
}

/// Decode one UUID from its raw protobuf byte representation.
fn decode_uuid(raw: Vec<u8>, field: &'static str) -> Result<Uuid, DeliveryWireError> {
    Uuid::from_slice(&raw).context(InvalidUuidSnafu { field })
}

/// Decode one member identifier from the generated discovery protobuf shape
/// into the local identifier type used by the delivery domain.
fn decode_member_identity(
    identifier: discovery_proto::Identifier,
    field: &'static str,
) -> Result<MemberIdentity, DeliveryWireError> {
    let mut buffer = IdentifierBuf::new();
    for segment in identifier.segments {
        buffer
            .push_checked(segment.clone())
            .context(InvalidIdentifierSegmentSnafu { field, segment })?;
    }
    Ok(buffer.into_identifier())
}

/// Semantic owner for one delivery boundary branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SemanticOwner {
    GroupBroadcast,
    ReliableDelivery,
    UnsupportedBoundary,
}

impl std::fmt::Display for SemanticOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GroupBroadcast => write!(f, "group_broadcast"),
            Self::ReliableDelivery => write!(f, "reliable_delivery"),
            Self::UnsupportedBoundary => write!(f, "unsupported_delivery_boundary"),
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
    use flotsync_messages::protobuf::MessageField;

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
        let mut identifier = discovery_proto::Identifier::new();
        identifier.segments = segments
            .iter()
            .map(|segment| (*segment).to_owned())
            .collect();
        identifier
    }

    fn encode_boundary_frame(frame: proto::DeliveryBoundaryFrame) -> IoPayload {
        let bytes = frame
            .write_to_bytes()
            .expect("test delivery boundary frame must encode");
        IoPayload::from(bytes::Bytes::from(bytes))
    }

    fn active_groups(groups: impl IntoIterator<Item = GroupId>) -> HashSet<GroupId> {
        groups.into_iter().collect()
    }

    fn members(values: impl IntoIterator<Item = MemberIdentity>) -> HashSet<MemberIdentity> {
        values.into_iter().collect()
    }

    #[test]
    fn irrelevant_group_envelope_is_dropped_before_full_decode() {
        let group_id = group_id(1);
        let delivery_message_id = message_id(2);
        let mut header = proto::GroupEnvelopeHeader::new();
        header.group_id = group_id.0.as_bytes().to_vec();
        header.sender = MessageField::some(proto_identifier(&["alice"]));
        header.message_id = delivery_message_id.0.as_bytes().to_vec();

        let mut envelope = proto::GroupEnvelopeWire::new();
        envelope.public_header = MessageField::some(header);
        envelope.encrypted_payload = vec![0x5a; 32 * 1024];

        let mut frame = proto::GroupBroadcastFrame::new();
        frame.body = Some(proto::group_broadcast_frame::Body::Envelope(envelope));

        let mut boundary = proto::DeliveryBoundaryFrame::new();
        boundary.boundary = Some(proto::delivery_boundary_frame::Boundary::GroupBroadcast(
            frame,
        ));

        let active_groups = active_groups([]);
        let local_members = members([]);
        let hosted_mailboxes = members([]);
        let decoded = decode_boundary_frame_if_interested(
            &encode_boundary_frame(boundary),
            DeliveryInterestView {
                active_groups: &active_groups,
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
        let mut header = proto::GroupEnvelopeHeader::new();
        header.group_id = group_id.0.as_bytes().to_vec();
        header.sender = MessageField::some(proto_identifier(&["alice"]));
        header.message_id = delivery_message_id.0.as_bytes().to_vec();

        let mut envelope = proto::GroupEnvelopeWire::new();
        envelope.public_header = MessageField::some(header);
        envelope.encrypted_payload = b"ciphertext".to_vec();

        let mut frame = proto::GroupBroadcastFrame::new();
        frame.body = Some(proto::group_broadcast_frame::Body::Envelope(
            envelope.clone(),
        ));

        let mut boundary = proto::DeliveryBoundaryFrame::new();
        boundary.boundary = Some(proto::delivery_boundary_frame::Boundary::GroupBroadcast(
            frame.clone(),
        ));

        let active_groups = active_groups([group_id]);
        let local_members = members([]);
        let hosted_mailboxes = members([]);
        let decoded = decode_boundary_frame_if_interested(
            &encode_boundary_frame(boundary),
            DeliveryInterestView {
                active_groups: &active_groups,
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
        let mut header = proto::RecipientAckHeader::new();
        header.message_id = delivery_message_id.0.as_bytes().to_vec();
        header.original_sender = MessageField::some(proto_identifier(&["alice"]));
        header.recipient = MessageField::some(proto_identifier(&["bob"]));

        let mut ack = proto::RecipientAckWire::new();
        ack.public_header = MessageField::some(header);

        let mut frame = proto::ReliableDeliveryFrame::new();
        frame.body = Some(proto::reliable_delivery_frame::Body::RecipientAck(ack));

        let mut boundary = proto::DeliveryBoundaryFrame::new();
        boundary.boundary = Some(proto::delivery_boundary_frame::Boundary::ReliableDelivery(
            frame,
        ));

        let active_groups = active_groups([]);
        let local_members = members([member(&["charlie"])]);
        let hosted_mailboxes = members([]);
        let decoded = decode_boundary_frame_if_interested(
            &encode_boundary_frame(boundary),
            DeliveryInterestView {
                active_groups: &active_groups,
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

        let mut header = proto::MailboxFetchHeader::new();
        header.recipient = MessageField::some(proto_identifier(&["alice"]));
        header.freshness_token = Uuid::from_u128(31).as_bytes().to_vec();

        let mut fetch = proto::MailboxFetchWire::new();
        fetch.public_header = MessageField::some(header);

        let mut frame = proto::ReliableDeliveryFrame::new();
        frame.body = Some(proto::reliable_delivery_frame::Body::MailboxFetch(
            fetch.clone(),
        ));

        let mut boundary = proto::DeliveryBoundaryFrame::new();
        boundary.boundary = Some(proto::delivery_boundary_frame::Boundary::ReliableDelivery(
            frame.clone(),
        ));

        let active_groups = active_groups([]);
        let local_members = members([]);
        let hosted_mailboxes = members([recipient.clone()]);
        let decoded = decode_boundary_frame_if_interested(
            &encode_boundary_frame(boundary),
            DeliveryInterestView {
                active_groups: &active_groups,
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

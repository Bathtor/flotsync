//! Wire adapters for the minimal direct-only group-broadcast slice.

use crate::delivery::wire::{
    WireValueDecodeError,
    fixed_bytes_field,
    group_id_from_wire,
    member_identity_from_wire,
    member_identity_to_wire_format,
    message_id_from_wire,
};

use super::{GroupMessageEnvelope, GroupMessageHeader};
use flotsync_messages::{
    buffa::{Message, MessageField},
    delivery as delivery_proto,
};
use flotsync_security::{SIGNATURE_LENGTH, SealedGroupPayload};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub(super) enum GroupBroadcastWireError {
    #[snafu(display("protobuf message '{message}' is missing required field '{field}'"))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },

    #[snafu(transparent)]
    WireValueDecode { source: WireValueDecodeError },
}

/// Build one outbound delivery-boundary frame for a group envelope.
pub(super) fn group_envelope_to_wire_format(
    envelope: &GroupMessageEnvelope<SealedGroupPayload>,
) -> delivery_proto::DeliveryBoundaryFrame {
    let header = group_header_to_wire_format(&envelope.header);
    let sealed_payload = delivery_proto::SealedGroupPayload {
        ciphertext: envelope.payload.ciphertext.clone(),
        sender_signature: envelope.payload.signature.to_vec(),
        ..delivery_proto::SealedGroupPayload::default()
    };
    let wire = delivery_proto::GroupEnvelopeWire {
        public_header: MessageField::some(header),
        sealed_payload: MessageField::some(sealed_payload),
        ..delivery_proto::GroupEnvelopeWire::default()
    };
    let frame = delivery_proto::GroupBroadcastFrame {
        body: Some(delivery_proto::group_broadcast_frame::Body::Envelope(
            Box::new(wire),
        )),
        ..delivery_proto::GroupBroadcastFrame::default()
    };
    delivery_proto::DeliveryBoundaryFrame {
        boundary: Some(
            delivery_proto::delivery_boundary_frame::Boundary::GroupBroadcast(Box::new(frame)),
        ),
        ..delivery_proto::DeliveryBoundaryFrame::default()
    }
}

/// Build canonical public-header bytes for group AEAD AAD and signature input.
///
/// This authenticates the semantic header projection used by delivery, not the
/// exact protobuf bytes received from transport; unknown fields and
/// non-canonical encodings are discarded during decode.
pub(super) fn group_public_header_bytes(header: &GroupMessageHeader) -> Vec<u8> {
    group_header_to_wire_format(header)
        .encode_to_bytes()
        .to_vec()
}

/// Decode one inbound wire envelope into the owned semantic broadcast model.
pub(super) fn group_envelope_from_wire(
    mut envelope: delivery_proto::GroupEnvelopeWire,
) -> Result<GroupMessageEnvelope<SealedGroupPayload>, GroupBroadcastWireError> {
    let mut header = envelope.public_header.take().context(MissingFieldSnafu {
        message: "GroupEnvelopeWire",
        field: "public_header",
    })?;
    let sealed_payload = envelope.sealed_payload.take().context(MissingFieldSnafu {
        message: "GroupEnvelopeWire",
        field: "sealed_payload",
    })?;

    let group_id = group_id_from_wire(&header.group_id, "GroupEnvelopeHeader.group_id")?;
    let sender_wire = header.sender.take().context(MissingFieldSnafu {
        message: "GroupEnvelopeHeader",
        field: "sender",
    })?;
    let sender = member_identity_from_wire(sender_wire, "GroupEnvelopeHeader.sender")?;
    let message_id = message_id_from_wire(&header.message_id, "GroupEnvelopeHeader.message_id")?;
    let signature = fixed_bytes_field::<SIGNATURE_LENGTH>(
        "SealedGroupPayload.sender_signature",
        &sealed_payload.sender_signature,
    )?;

    Ok(GroupMessageEnvelope::<SealedGroupPayload> {
        header: GroupMessageHeader {
            group_id,
            sender,
            message_id,
        },
        payload: SealedGroupPayload {
            ciphertext: sealed_payload.ciphertext,
            signature,
        },
    })
}

fn group_header_to_wire_format(header: &GroupMessageHeader) -> delivery_proto::GroupEnvelopeHeader {
    delivery_proto::GroupEnvelopeHeader {
        group_id: header.group_id.0.as_bytes().to_vec(),
        sender: MessageField::some(member_identity_to_wire_format(&header.sender)),
        message_id: header.message_id.0.as_bytes().to_vec(),
        ..delivery_proto::GroupEnvelopeHeader::default()
    }
}

//! Wire adapters for the minimal direct-only group-broadcast slice.

use crate::delivery::wire::{
    WireValueDecodeError,
    group_id_from_wire,
    member_identity_from_wire,
    member_identity_to_wire_format,
    message_id_from_wire,
    signature_from_wire,
    signature_to_wire_format,
};

use super::{EncryptedPayload, GroupMessageEnvelope, GroupMessageHeader};
use flotsync_messages::{buffa::MessageField, delivery as delivery_proto};
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
    envelope: &GroupMessageEnvelope,
) -> delivery_proto::DeliveryBoundaryFrame {
    let header = delivery_proto::GroupEnvelopeHeader {
        group_id: envelope.header.group_id.0.as_bytes().to_vec(),
        sender: MessageField::some(member_identity_to_wire_format(&envelope.header.sender)),
        message_id: envelope.header.message_id.0.as_bytes().to_vec(),
        ..delivery_proto::GroupEnvelopeHeader::default()
    };
    let wire = delivery_proto::GroupEnvelopeWire {
        public_header: MessageField::some(header),
        encrypted_payload: envelope.payload.ciphertext.clone(),
        footer: MessageField::some(signature_to_wire_format(&envelope.footer)),
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

/// Decode one inbound wire envelope into the owned semantic broadcast model.
pub(super) fn group_envelope_from_wire(
    mut envelope: delivery_proto::GroupEnvelopeWire,
) -> Result<GroupMessageEnvelope, GroupBroadcastWireError> {
    let mut header = envelope.public_header.take().context(MissingFieldSnafu {
        message: "GroupEnvelopeWire",
        field: "public_header",
    })?;
    let footer = envelope.footer.take().context(MissingFieldSnafu {
        message: "GroupEnvelopeWire",
        field: "footer",
    })?;

    let group_id = group_id_from_wire(&header.group_id, "GroupEnvelopeHeader.group_id")?;
    let sender = member_identity_from_wire(
        header.sender.take().context(MissingFieldSnafu {
            message: "GroupEnvelopeHeader",
            field: "sender",
        })?,
        "GroupEnvelopeHeader.sender",
    )?;
    let message_id = message_id_from_wire(&header.message_id, "GroupEnvelopeHeader.message_id")?;

    Ok(GroupMessageEnvelope {
        header: GroupMessageHeader {
            group_id,
            sender,
            message_id,
        },
        payload: EncryptedPayload {
            ciphertext: envelope.encrypted_payload,
        },
        footer: signature_from_wire(footer, "GroupEnvelopeWire.footer")?,
    })
}

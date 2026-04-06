use crate::delivery::wire::{
    WireValueDecodeError,
    member_identity_from_wire,
    member_identity_to_wire_format,
    message_id_from_wire,
    signature_from_wire,
    signature_to_wire_format,
};

use super::*;
use flotsync_messages::{buffa::MessageField, delivery as delivery_proto};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub(super) enum ReliableDeliveryWireError {
    #[snafu(display("protobuf message '{message}' is missing required field '{field}'"))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },

    #[snafu(transparent)]
    WireValueDecode { source: WireValueDecodeError },
}

pub(super) fn reliable_envelope_to_wire_format(
    envelope: &ReliableMessageEnvelope,
) -> delivery_proto::DeliveryBoundaryFrame {
    let header = delivery_proto::ReliableEnvelopeHeader {
        sender: MessageField::some(member_identity_to_wire_format(&envelope.header.sender)),
        recipient: MessageField::some(member_identity_to_wire_format(&envelope.header.recipient)),
        message_id: envelope.header.message_id.0.as_bytes().to_vec(),
        ..delivery_proto::ReliableEnvelopeHeader::default()
    };
    let wire = delivery_proto::ReliableEnvelopeWire {
        public_header: MessageField::some(header),
        encrypted_payload: envelope.payload.ciphertext.clone(),
        footer: MessageField::some(signature_to_wire_format(&envelope.footer)),
        ..delivery_proto::ReliableEnvelopeWire::default()
    };
    let frame = delivery_proto::ReliableDeliveryFrame {
        body: Some(delivery_proto::reliable_delivery_frame::Body::Envelope(
            Box::new(wire),
        )),
        ..delivery_proto::ReliableDeliveryFrame::default()
    };
    delivery_proto::DeliveryBoundaryFrame {
        boundary: Some(
            delivery_proto::delivery_boundary_frame::Boundary::ReliableDelivery(Box::new(frame)),
        ),
        ..delivery_proto::DeliveryBoundaryFrame::default()
    }
}

pub(super) fn recipient_ack_to_wire_format(
    ack: &RecipientAck,
) -> delivery_proto::DeliveryBoundaryFrame {
    let header = delivery_proto::RecipientAckHeader {
        message_id: ack.header.message_id.0.as_bytes().to_vec(),
        original_sender: MessageField::some(member_identity_to_wire_format(
            &ack.header.original_sender,
        )),
        recipient: MessageField::some(member_identity_to_wire_format(&ack.header.recipient)),
        ..delivery_proto::RecipientAckHeader::default()
    };
    let wire = delivery_proto::RecipientAckWire {
        public_header: MessageField::some(header),
        footer: MessageField::some(signature_to_wire_format(&ack.footer)),
        ..delivery_proto::RecipientAckWire::default()
    };
    let frame = delivery_proto::ReliableDeliveryFrame {
        body: Some(delivery_proto::reliable_delivery_frame::Body::RecipientAck(
            Box::new(wire),
        )),
        ..delivery_proto::ReliableDeliveryFrame::default()
    };
    delivery_proto::DeliveryBoundaryFrame {
        boundary: Some(
            delivery_proto::delivery_boundary_frame::Boundary::ReliableDelivery(Box::new(frame)),
        ),
        ..delivery_proto::DeliveryBoundaryFrame::default()
    }
}

pub(super) fn reliable_envelope_from_wire(
    mut envelope: delivery_proto::ReliableEnvelopeWire,
) -> Result<ReliableMessageEnvelope, ReliableDeliveryWireError> {
    let mut header = envelope.public_header.take().context(MissingFieldSnafu {
        message: "ReliableEnvelopeWire",
        field: "public_header",
    })?;
    let footer = envelope.footer.take().context(MissingFieldSnafu {
        message: "ReliableEnvelopeWire",
        field: "footer",
    })?;

    let sender = member_identity_from_wire(
        header.sender.take().context(MissingFieldSnafu {
            message: "ReliableEnvelopeHeader",
            field: "sender",
        })?,
        "ReliableEnvelopeHeader.sender",
    )?;
    let recipient = member_identity_from_wire(
        header.recipient.take().context(MissingFieldSnafu {
            message: "ReliableEnvelopeHeader",
            field: "recipient",
        })?,
        "ReliableEnvelopeHeader.recipient",
    )?;
    let message_id = message_id_from_wire(&header.message_id, "ReliableEnvelopeHeader.message_id")?;

    Ok(ReliableMessageEnvelope {
        header: ReliableMessageHeader {
            sender,
            recipient,
            message_id,
        },
        payload: EncryptedPayload {
            ciphertext: envelope.encrypted_payload,
        },
        footer: signature_from_wire(footer, "ReliableEnvelopeWire.footer")?,
    })
}

pub(super) fn recipient_ack_from_wire(
    mut ack: delivery_proto::RecipientAckWire,
) -> Result<RecipientAck, ReliableDeliveryWireError> {
    let mut header = ack.public_header.take().context(MissingFieldSnafu {
        message: "RecipientAckWire",
        field: "public_header",
    })?;
    let footer = ack.footer.take().context(MissingFieldSnafu {
        message: "RecipientAckWire",
        field: "footer",
    })?;

    let original_sender = member_identity_from_wire(
        header.original_sender.take().context(MissingFieldSnafu {
            message: "RecipientAckHeader",
            field: "original_sender",
        })?,
        "RecipientAckHeader.original_sender",
    )?;
    let recipient = member_identity_from_wire(
        header.recipient.take().context(MissingFieldSnafu {
            message: "RecipientAckHeader",
            field: "recipient",
        })?,
        "RecipientAckHeader.recipient",
    )?;
    let message_id = message_id_from_wire(&header.message_id, "RecipientAckHeader.message_id")?;

    Ok(RecipientAck {
        header: RecipientAckHeader {
            message_id,
            original_sender,
            recipient,
        },
        footer: signature_from_wire(footer, "RecipientAckWire.footer")?,
    })
}

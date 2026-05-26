use crate::delivery::wire::{
    WireValueDecodeError,
    detached_signature_from_wire,
    detached_signature_to_wire_format,
    fixed_bytes_field,
    member_identity_from_wire,
    member_identity_to_wire_format,
    message_id_from_wire,
};

use super::{
    EncryptedPayload,
    RecipientAck,
    RecipientAckHeader,
    ReliableMessageEnvelope,
    ReliableMessageHeader,
};
use flotsync_messages::{
    buffa::{Message, MessageField},
    delivery as delivery_proto,
    endpoint as endpoint_proto,
};
use flotsync_security::{HPKE_ENCAPSULATED_KEY_LENGTH, SIGNATURE_LENGTH, SealedHPKEPayload};
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
    envelope: &ReliableMessageEnvelope<EncryptedPayload>,
) -> endpoint_proto::EndpointFrame {
    let header = delivery_proto::ReliableEnvelopeHeader {
        sender: MessageField::some(member_identity_to_wire_format(&envelope.header.sender)),
        recipient: MessageField::some(member_identity_to_wire_format(&envelope.header.recipient)),
        message_id: envelope.header.message_id.0.as_bytes().to_vec(),
        ..delivery_proto::ReliableEnvelopeHeader::default()
    };
    let sealed_payload = &envelope.payload.sealed;
    let sealed_payload = delivery_proto::SealedHPKEPayload {
        encapsulated_key: sealed_payload.encapsulated_key.to_vec(),
        ciphertext: sealed_payload.ciphertext.clone().into(),
        signature: sealed_payload.signature.to_vec(),
        ..delivery_proto::SealedHPKEPayload::default()
    };
    let wire = delivery_proto::ReliableEnvelopeWire {
        public_header: MessageField::some(header),
        sealed_payload: MessageField::some(sealed_payload),
        ..delivery_proto::ReliableEnvelopeWire::default()
    };
    let frame = delivery_proto::ReliableDeliveryFrame {
        body: Some(delivery_proto::reliable_delivery_frame::Body::Envelope(
            Box::new(wire),
        )),
        ..delivery_proto::ReliableDeliveryFrame::default()
    };
    endpoint_proto::EndpointFrame {
        boundary: Some(endpoint_proto::endpoint_frame::Boundary::ReliableDelivery(
            Box::new(frame),
        )),
        ..endpoint_proto::EndpointFrame::default()
    }
}

pub(super) fn recipient_ack_to_wire_format(ack: &RecipientAck) -> endpoint_proto::EndpointFrame {
    let header = recipient_ack_header_to_wire_format(&ack.header);
    let wire = delivery_proto::RecipientAckWire {
        public_header: MessageField::some(header),
        signature: MessageField::some(detached_signature_to_wire_format(&ack.signature)),
        ..delivery_proto::RecipientAckWire::default()
    };
    let frame = delivery_proto::ReliableDeliveryFrame {
        body: Some(delivery_proto::reliable_delivery_frame::Body::RecipientAck(
            Box::new(wire),
        )),
        ..delivery_proto::ReliableDeliveryFrame::default()
    };
    endpoint_proto::EndpointFrame {
        boundary: Some(endpoint_proto::endpoint_frame::Boundary::ReliableDelivery(
            Box::new(frame),
        )),
        ..endpoint_proto::EndpointFrame::default()
    }
}

/// Build canonical public-header bytes for recipient-ack signatures.
///
/// This authenticates the semantic ack header projection used by reliable
/// delivery, not the exact protobuf bytes received from transport.
pub(super) fn recipient_ack_public_header_bytes(header: &RecipientAckHeader) -> Vec<u8> {
    recipient_ack_header_to_wire_format(header)
        .encode_to_bytes()
        .to_vec()
}

pub(super) fn reliable_envelope_from_wire(
    mut envelope: delivery_proto::ReliableEnvelopeWire,
) -> Result<ReliableMessageEnvelope<EncryptedPayload>, ReliableDeliveryWireError> {
    let mut header = envelope.public_header.take().context(MissingFieldSnafu {
        message: "ReliableEnvelopeWire",
        field: "public_header",
    })?;
    let sealed_payload = envelope.sealed_payload.take().context(MissingFieldSnafu {
        message: "ReliableEnvelopeWire",
        field: "sealed_payload",
    })?;

    let sender_wire = header.sender.take().context(MissingFieldSnafu {
        message: "ReliableEnvelopeHeader",
        field: "sender",
    })?;
    let sender = member_identity_from_wire(sender_wire, "ReliableEnvelopeHeader.sender")?;
    let recipient_wire = header.recipient.take().context(MissingFieldSnafu {
        message: "ReliableEnvelopeHeader",
        field: "recipient",
    })?;
    let recipient = member_identity_from_wire(recipient_wire, "ReliableEnvelopeHeader.recipient")?;
    let message_id = message_id_from_wire(&header.message_id, "ReliableEnvelopeHeader.message_id")?;

    let encapsulated_key = fixed_bytes_field::<HPKE_ENCAPSULATED_KEY_LENGTH>(
        "SealedHPKEPayload.encapsulated_key",
        &sealed_payload.encapsulated_key,
    )?;
    let signature = fixed_bytes_field::<SIGNATURE_LENGTH>(
        "SealedHPKEPayload.signature",
        &sealed_payload.signature,
    )?;

    Ok(ReliableMessageEnvelope::<EncryptedPayload> {
        header: ReliableMessageHeader {
            sender,
            recipient,
            message_id,
        },
        payload: EncryptedPayload {
            sealed: SealedHPKEPayload {
                encapsulated_key,
                ciphertext: sealed_payload.ciphertext.to_vec(),
                signature,
            },
        },
    })
}

pub(super) fn recipient_ack_from_wire(
    mut ack: delivery_proto::RecipientAckWire,
) -> Result<RecipientAck, ReliableDeliveryWireError> {
    let mut header = ack.public_header.take().context(MissingFieldSnafu {
        message: "RecipientAckWire",
        field: "public_header",
    })?;
    let signature = ack.signature.take().context(MissingFieldSnafu {
        message: "RecipientAckWire",
        field: "signature",
    })?;

    let original_sender_wire = header.original_sender.take().context(MissingFieldSnafu {
        message: "RecipientAckHeader",
        field: "original_sender",
    })?;
    let original_sender =
        member_identity_from_wire(original_sender_wire, "RecipientAckHeader.original_sender")?;
    let recipient_wire = header.recipient.take().context(MissingFieldSnafu {
        message: "RecipientAckHeader",
        field: "recipient",
    })?;
    let recipient = member_identity_from_wire(recipient_wire, "RecipientAckHeader.recipient")?;
    let message_id = message_id_from_wire(&header.message_id, "RecipientAckHeader.message_id")?;
    let signature = detached_signature_from_wire(signature, "RecipientAckWire.signature")?;

    Ok(RecipientAck {
        header: RecipientAckHeader {
            message_id,
            original_sender,
            recipient,
        },
        signature,
    })
}

/// Build the public recipient-ack header projection used on the wire and in
/// signature transcripts.
fn recipient_ack_header_to_wire_format(
    header: &RecipientAckHeader,
) -> delivery_proto::RecipientAckHeader {
    delivery_proto::RecipientAckHeader {
        message_id: header.message_id.0.as_bytes().to_vec(),
        original_sender: MessageField::some(member_identity_to_wire_format(
            &header.original_sender,
        )),
        recipient: MessageField::some(member_identity_to_wire_format(&header.recipient)),
        ..delivery_proto::RecipientAckHeader::default()
    }
}

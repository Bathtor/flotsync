use super::*;
use flotsync_core::member::{IdentifierBuf, IdentifierError};
use flotsync_io::pool::PayloadWriter;
use flotsync_messages::{
    buffa::{EnumValue, Message, MessageField},
    delivery as delivery_proto,
    discovery as discovery_proto,
};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub(super) enum ReliableDeliveryWireError {
    #[snafu(display("protobuf message '{message}' is missing required field '{field}'"))]
    MissingField {
        message: &'static str,
        field: &'static str,
    },

    #[snafu(display("field '{field}' did not contain a valid UUID: {source}"))]
    InvalidUuid {
        field: &'static str,
        source: uuid::Error,
    },

    #[snafu(display(
        "field '{field}' contains an invalid identifier segment '{segment}': {source}"
    ))]
    InvalidIdentifierSegment {
        field: &'static str,
        segment: String,
        source: IdentifierError,
    },

    #[snafu(display("field '{field}' used an unknown signature scheme value {value}"))]
    UnknownSignatureScheme { field: &'static str, value: i32 },

    #[snafu(display("field '{field}' used the unspecified signature scheme"))]
    UnspecifiedSignatureScheme { field: &'static str },
}

/// Temporary pre-encoded payload wrapper for the current protobuf stack.
///
/// TODO(flotsync-rp3): Replace this eager byte buffer once the protobuf
/// boundary can serialise directly into transport-owned buffers.
pub(super) struct EncodedDeliveryPayload(pub(super) Bytes);

impl FlotsyncSerializable for EncodedDeliveryPayload {
    fn serialized_size_hint(&self) -> SizeHint {
        SizeHint::Exact(self.0.len())
    }

    fn serialize_into<'a>(
        &'a self,
        writer: &'a mut flotsync_io::prelude::EgressAsyncWriter,
    ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>> {
        Box::pin(async move {
            writer
                .write_slice(&self.0)
                .await
                .map_err(|source| FlotsyncSerializeError::Io { source })?;
            Ok(())
        })
    }
}

pub(super) fn encode_reliable_envelope_boundary(
    envelope: &ReliableMessageEnvelope,
) -> Result<Bytes, ReliableDeliveryWireError> {
    let header = delivery_proto::ReliableEnvelopeHeader {
        sender: MessageField::some(proto_identifier(&envelope.header.sender)),
        recipient: MessageField::some(proto_identifier(&envelope.header.recipient)),
        message_id: envelope.header.message_id.0.as_bytes().to_vec(),
        ..delivery_proto::ReliableEnvelopeHeader::default()
    };
    let wire = delivery_proto::ReliableEnvelopeWire {
        public_header: MessageField::some(header),
        encrypted_payload: envelope.payload.ciphertext.to_vec(),
        footer: MessageField::some(encode_signature_wire(&envelope.footer)),
        ..delivery_proto::ReliableEnvelopeWire::default()
    };
    let frame = delivery_proto::ReliableDeliveryFrame {
        body: Some(delivery_proto::reliable_delivery_frame::Body::Envelope(
            Box::new(wire),
        )),
        ..delivery_proto::ReliableDeliveryFrame::default()
    };
    let boundary = delivery_proto::DeliveryBoundaryFrame {
        boundary: Some(
            delivery_proto::delivery_boundary_frame::Boundary::ReliableDelivery(Box::new(frame)),
        ),
        ..delivery_proto::DeliveryBoundaryFrame::default()
    };
    Ok(boundary.encode_to_bytes())
}

pub(super) fn encode_recipient_ack_boundary(
    ack: &RecipientAck,
) -> Result<Bytes, ReliableDeliveryWireError> {
    let header = delivery_proto::RecipientAckHeader {
        message_id: ack.header.message_id.0.as_bytes().to_vec(),
        original_sender: MessageField::some(proto_identifier(&ack.header.original_sender)),
        recipient: MessageField::some(proto_identifier(&ack.header.recipient)),
        ..delivery_proto::RecipientAckHeader::default()
    };
    let wire = delivery_proto::RecipientAckWire {
        public_header: MessageField::some(header),
        footer: MessageField::some(encode_signature_wire(&ack.footer)),
        ..delivery_proto::RecipientAckWire::default()
    };
    let frame = delivery_proto::ReliableDeliveryFrame {
        body: Some(delivery_proto::reliable_delivery_frame::Body::RecipientAck(
            Box::new(wire),
        )),
        ..delivery_proto::ReliableDeliveryFrame::default()
    };
    let boundary = delivery_proto::DeliveryBoundaryFrame {
        boundary: Some(
            delivery_proto::delivery_boundary_frame::Boundary::ReliableDelivery(Box::new(frame)),
        ),
        ..delivery_proto::DeliveryBoundaryFrame::default()
    };
    Ok(boundary.encode_to_bytes())
}

pub(super) fn decode_reliable_envelope(
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

    let sender = decode_identifier(
        header.sender.take().context(MissingFieldSnafu {
            message: "ReliableEnvelopeHeader",
            field: "sender",
        })?,
        "ReliableEnvelopeHeader.sender",
    )?;
    let recipient = decode_identifier(
        header.recipient.take().context(MissingFieldSnafu {
            message: "ReliableEnvelopeHeader",
            field: "recipient",
        })?,
        "ReliableEnvelopeHeader.recipient",
    )?;
    let message_id = decode_message_id(header.message_id, "ReliableEnvelopeHeader.message_id")?;

    Ok(ReliableMessageEnvelope {
        header: ReliableMessageHeader {
            sender,
            recipient,
            message_id,
        },
        payload: EncryptedPayload {
            ciphertext: Bytes::from(envelope.encrypted_payload),
        },
        footer: decode_signature_wire(footer, "ReliableEnvelopeWire.footer")?,
    })
}

pub(super) fn decode_recipient_ack(
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

    let original_sender = decode_identifier(
        header.original_sender.take().context(MissingFieldSnafu {
            message: "RecipientAckHeader",
            field: "original_sender",
        })?,
        "RecipientAckHeader.original_sender",
    )?;
    let recipient = decode_identifier(
        header.recipient.take().context(MissingFieldSnafu {
            message: "RecipientAckHeader",
            field: "recipient",
        })?,
        "RecipientAckHeader.recipient",
    )?;
    let message_id = decode_message_id(header.message_id, "RecipientAckHeader.message_id")?;

    Ok(RecipientAck {
        header: RecipientAckHeader {
            message_id,
            original_sender,
            recipient,
        },
        footer: decode_signature_wire(footer, "RecipientAckWire.footer")?,
    })
}

fn encode_signature_wire(footer: &SignedEnvelopeFooter) -> delivery_proto::SignatureWire {
    delivery_proto::SignatureWire {
        scheme: EnumValue::from(match footer.signature.scheme {
            SignatureScheme::Ed25519 => {
                delivery_proto::KnownSignatureScheme::KNOWN_SIGNATURE_SCHEME_ED25519
            }
        }),
        signature_bytes: footer.signature.bytes.to_vec(),
        ..delivery_proto::SignatureWire::default()
    }
}

fn decode_signature_wire(
    wire: delivery_proto::SignatureWire,
    field: &'static str,
) -> Result<SignedEnvelopeFooter, ReliableDeliveryWireError> {
    let scheme = wire.scheme.as_known().ok_or_else(|| {
        ReliableDeliveryWireError::UnknownSignatureScheme {
            field,
            value: wire.scheme.to_i32(),
        }
    })?;
    let scheme = match scheme {
        delivery_proto::KnownSignatureScheme::KNOWN_SIGNATURE_SCHEME_ED25519 => {
            SignatureScheme::Ed25519
        }
        delivery_proto::KnownSignatureScheme::KNOWN_SIGNATURE_SCHEME_UNSPECIFIED => {
            return UnspecifiedSignatureSchemeSnafu { field }.fail();
        }
    };
    Ok(SignedEnvelopeFooter {
        signature: DetachedSignature {
            scheme,
            bytes: Bytes::from(wire.signature_bytes),
        },
    })
}

fn decode_message_id(
    bytes: Vec<u8>,
    field: &'static str,
) -> Result<MessageId, ReliableDeliveryWireError> {
    let uuid = Uuid::from_slice(&bytes).context(InvalidUuidSnafu { field })?;
    Ok(MessageId(uuid))
}

fn decode_identifier(
    identifier: discovery_proto::Identifier,
    field: &'static str,
) -> Result<MemberIdentity, ReliableDeliveryWireError> {
    let mut buffer = IdentifierBuf::new();
    for segment in identifier.segments {
        buffer
            .push_checked(segment.clone())
            .context(InvalidIdentifierSegmentSnafu { field, segment })?;
    }
    Ok(buffer.into_identifier())
}

fn proto_identifier(member: &MemberIdentity) -> discovery_proto::Identifier {
    let segments = member
        .segments_iter()
        .map(|segment| segment.as_ref().to_owned())
        .collect();
    discovery_proto::Identifier {
        segments,
        ..discovery_proto::Identifier::default()
    }
}

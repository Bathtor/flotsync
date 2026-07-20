//! Reliable-delivery payload and acknowledgement security operations.

use super::*;

impl DeliverySecurity {
    /// Seal one reliable-delivery payload for its concrete recipient.
    pub(crate) async fn seal_reliable_payload(
        &self,
        header: &ReliableMessageHeader,
        plaintext: &[u8],
    ) -> Result<SealedHPKEPayload, DeliverySecurityError> {
        ensure!(
            header.sender == self.local_member,
            UnexpectedReliableSenderSnafu {
                sender: header.sender.clone(),
                local_member: self.local_member.clone()
            }
        );
        ensure!(
            header.recipient != self.local_member,
            ReliableSelfMessageSnafu {
                member_id: self.local_member.clone()
            }
        );
        let recipient_keys = self.load_permitted_public_keys(&header.recipient).await?;
        seal_reliable_payload_with_os_rng(
            self.local_keys(),
            &recipient_keys,
            ReliablePayloadContext {
                frame_kind: RELIABLE_RUNTIME_MESSAGE_FRAME_KIND,
                sender: &header.sender,
                recipient: &header.recipient,
                scope: header.scope.into(),
                message_id: header.message_id.0,
            },
            plaintext,
        )
        .boxed()
        .with_context(|_| SealReliablePayloadSnafu {
            recipient: header.recipient.clone(),
        })
    }

    /// Open one reliable-delivery payload after its public header has been decoded.
    pub(crate) async fn open_reliable_payload(
        &self,
        header: &ReliableMessageHeader,
        sealed: &SealedHPKEPayload,
    ) -> Result<Vec<u8>, DeliverySecurityError> {
        ensure!(
            header.recipient == self.local_member,
            UnexpectedReliableRecipientSnafu {
                recipient: header.recipient.clone(),
                local_member: self.local_member.clone()
            }
        );
        ensure!(
            header.sender != self.local_member,
            ReliableSelfMessageSnafu {
                member_id: self.local_member.clone()
            }
        );
        let sender_keys = self.load_permitted_public_keys(&header.sender).await?;
        open_reliable_payload(
            &sender_keys,
            self.local_keys(),
            ReliablePayloadContext {
                frame_kind: RELIABLE_RUNTIME_MESSAGE_FRAME_KIND,
                sender: &header.sender,
                recipient: &header.recipient,
                scope: header.scope.into(),
                message_id: header.message_id.0,
            },
            sealed,
        )
        .boxed()
        .context(OpenReliablePayloadSnafu)
    }

    /// Sign one semantic recipient ack for the original sender.
    pub(crate) fn sign_recipient_ack(
        &self,
        header: &RecipientAckHeader,
        public_header: &[u8],
    ) -> Result<DetachedSignature, DeliverySecurityError> {
        ensure!(
            header.recipient == self.local_member,
            UnexpectedRecipientAckRecipientSnafu {
                recipient: header.recipient.clone(),
                local_member: self.local_member.clone()
            }
        );
        let signature = sign_frame(
            self.local_keys(),
            SignedFrameParts {
                frame_kind: RELIABLE_RECIPIENT_ACK_FRAME_KIND,
                public_header,
                ciphertext: &[],
            },
        )
        .boxed()
        .with_context(|_| SignRecipientAckSnafu {
            recipient: header.recipient.clone(),
        })?;
        Ok(DetachedSignature {
            scheme: SignatureScheme::Ed25519Ph,
            bytes: Bytes::copy_from_slice(signature.as_bytes()),
        })
    }

    /// Verify one recipient ack against the expected recipient identity.
    pub(crate) async fn verify_recipient_ack(
        &self,
        header: &RecipientAckHeader,
        public_header: &[u8],
        signature: &DetachedSignature,
    ) -> Result<(), DeliverySecurityError> {
        ensure!(
            header.original_sender == self.local_member,
            UnexpectedRecipientAckOriginalSenderSnafu {
                original_sender: header.original_sender.clone(),
                local_member: self.local_member.clone()
            }
        );
        ensure!(
            header.recipient != self.local_member,
            ReliableSelfMessageSnafu {
                member_id: self.local_member.clone()
            }
        );
        let frame_signature = recipient_ack_frame_signature(header, signature)?;
        let recipient_keys = self.load_permitted_public_keys(&header.recipient).await?;
        verify_frame_signature(
            &recipient_keys,
            SignedFrameParts {
                frame_kind: RELIABLE_RECIPIENT_ACK_FRAME_KIND,
                public_header,
                ciphertext: &[],
            },
            &frame_signature,
        )
        .boxed()
        .with_context(|_| VerifyRecipientAckSnafu {
            recipient: header.recipient.clone(),
        })
    }
}

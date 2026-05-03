use super::messages::RuntimeMessage;
use crate::delivery::shared::{DetachedSignature, SignatureScheme, SignedEnvelopeFooter};
use flotsync_messages::buffa::Message as _;

/// Encode one runtime message into the temporary byte payload expected by the
/// current delivery-envelope boundary.
pub(super) fn encode_runtime_payload(message: &RuntimeMessage) -> bytes::Bytes {
    // Temporary byte serialisation at the delivery-envelope boundary.
    // See flotsync-ylo for the payload/encryption redesign.
    message.encode_to_proto().encode_to_bytes()
}

/// Build the placeholder signature footer used until runtime payload signing is
/// moved behind the delivery-envelope boundary.
pub(super) fn placeholder_signed_footer() -> SignedEnvelopeFooter {
    SignedEnvelopeFooter {
        signature: DetachedSignature {
            scheme: SignatureScheme::Ed25519,
            bytes: bytes::Bytes::from_static(b"runtime-placeholder-signature"),
        },
    }
}

use super::messages::RuntimeMessage;
use flotsync_messages::buffa::Message as _;

/// Encode one runtime message into the temporary byte payload expected by the
/// current delivery-envelope boundary.
pub(super) fn encode_runtime_payload(message: &RuntimeMessage) -> bytes::Bytes {
    // Temporary byte serialisation at the delivery-envelope boundary.
    // See flotsync-ylo for the payload/encryption redesign.
    message.encode_to_proto().encode_to_bytes()
}

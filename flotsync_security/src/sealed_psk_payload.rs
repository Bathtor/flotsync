use crate::signature::SIGNATURE_LENGTH;
use bytes::Bytes;

/// Payload encrypted under an already established symmetric/pre-shared key and
/// authenticated by a detached frame signature.
///
/// PSK payloads are used where the sender and recipients already share the
/// content-encryption context, for example group broadcast. Frames that only
/// need authentication and carry no encrypted body should use a detached
/// signature directly instead of this type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedPSKPayload {
    /// Symmetric ciphertext bytes covered by the frame signature.
    pub ciphertext: Bytes,
    /// Signature over the enclosing public header and payload bytes.
    pub signature: [u8; SIGNATURE_LENGTH],
}

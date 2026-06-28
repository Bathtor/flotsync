use crate::{
    error::{InvalidSignatureBytesSnafu, Result, SignSignatureSnafu, VerifySignatureSnafu},
    identity::{LocalMemberKeys, PublicMemberKeys},
    util::hash_len_prefixed,
};
use ed25519_dalek::{Digest, Sha512, Signature};
use flotsync_messages::{
    buffa::EnumValue,
    discovery::{DiscoverySignature, DiscoverySignatureView},
    proto,
    security as security_proto,
};
use snafu::prelude::*;

/// Byte length of an Ed25519ph frame signature.
pub const SIGNATURE_LENGTH: usize = 64;

/// Sign the public frame header and ciphertext as one detached Ed25519ph frame.
///
/// # Errors
///
/// Returns [`crate::SecurityError::SignSignature`] if the Ed25519ph signing
/// operation rejects the prehashed transcript.
pub fn sign_frame(
    local_keys: &LocalMemberKeys,
    parts: SignedFrameParts<'_>,
) -> Result<FrameSignature> {
    let signature: Signature = local_keys
        .signing_key
        .sign_prehashed(signature_transcript(parts), None)
        .context(SignSignatureSnafu)?;
    Ok(FrameSignature {
        bytes: signature.to_bytes(),
    })
}

/// Verify a detached Ed25519ph frame signature over the header and ciphertext.
///
/// # Errors
///
/// Returns [`crate::SecurityError::InvalidSignatureBytes`] when the signature
/// bytes do not form a valid Ed25519ph signature, or
/// [`crate::SecurityError::VerifySignature`] when the signature does not verify
/// against the supplied frame parts.
pub fn verify_frame_signature(
    public_keys: &PublicMemberKeys,
    parts: SignedFrameParts<'_>,
    signature: &FrameSignature,
) -> Result<()> {
    let signature =
        Signature::try_from(signature.as_bytes().as_slice()).context(InvalidSignatureBytesSnafu)?;
    public_keys
        .signing_key
        .verify_prehashed(signature_transcript(parts), None, &signature)
        .context(VerifySignatureSnafu)
}

/// Sign one discovery claim payload exactly as encoded on the wire.
///
/// # Errors
///
/// Returns [`crate::SecurityError::SignSignature`] if the Ed25519ph signing
/// operation rejects the prehashed payload.
pub fn sign_discovery_payload(
    local_keys: &LocalMemberKeys,
    payload: &[u8],
) -> Result<FrameSignature> {
    let signature: Signature = local_keys
        .signing_key
        .sign_prehashed(discovery_payload_transcript(payload), None)
        .context(SignSignatureSnafu)?;
    Ok(FrameSignature {
        bytes: signature.to_bytes(),
    })
}

/// Verify one discovery claim payload signature against the exact encoded bytes.
///
/// # Errors
///
/// Returns [`crate::SecurityError::InvalidSignatureBytes`] when the signature
/// bytes are malformed, or [`crate::SecurityError::VerifySignature`] when the
/// signature does not verify for `payload`.
pub fn verify_discovery_payload_signature(
    public_keys: &PublicMemberKeys,
    payload: &[u8],
    signature: &FrameSignature,
) -> Result<()> {
    let signature =
        Signature::try_from(signature.as_bytes().as_slice()).context(InvalidSignatureBytesSnafu)?;
    public_keys
        .signing_key
        .verify_prehashed(discovery_payload_transcript(payload), None, &signature)
        .context(VerifySignatureSnafu)
}

/// Decode failures for the protobuf discovery-signature wrapper.
#[derive(Debug, Snafu)]
pub enum FrameSignatureProtoError {
    /// The protobuf wrapper selected a signature scheme this runtime does not support.
    #[snafu(display("signature used unsupported scheme value {value}"))]
    UnsupportedSignatureScheme { value: i32 },
    /// The protobuf wrapper carried the wrong number of Ed25519ph signature bytes.
    #[snafu(display("signature had {actual} byte(s), expected {SIGNATURE_LENGTH}"))]
    InvalidSignatureLength { actual: usize },
}

/// Replication frame components covered by a detached signature.
#[derive(Clone, Copy, Debug)]
pub struct SignedFrameParts<'a> {
    /// Stable protocol frame kind, not a display label.
    pub frame_kind: &'static str,
    /// Public frame header bytes that remain visible on the wire.
    pub public_header: &'a [u8],
    /// Ciphertext bytes, including any AEAD authentication tag.
    pub ciphertext: &'a [u8],
}

/// Detached Ed25519ph frame signature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameSignature {
    bytes: [u8; SIGNATURE_LENGTH],
}

impl FrameSignature {
    /// Build a detached signature from raw Ed25519ph signature bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; SIGNATURE_LENGTH]) -> Self {
        Self { bytes }
    }

    /// Return the raw Ed25519ph signature bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; SIGNATURE_LENGTH] {
        &self.bytes
    }
}

impl proto::ProtoCodec for FrameSignature {
    type DecodeError = FrameSignatureProtoError;
    type Proto = DiscoverySignature;

    fn to_proto(&self) -> Self::Proto {
        DiscoverySignature {
            scheme: EnumValue::from(security_proto::SignatureScheme::SIGNATURE_SCHEME_ED25519PH),
            signature_bytes: self.as_bytes().to_vec(),
            ..DiscoverySignature::default()
        }
    }

    fn from_proto(signature: Self::Proto) -> std::result::Result<Self, Self::DecodeError> {
        let scheme = signature.scheme.as_known().ok_or_else(|| {
            FrameSignatureProtoError::UnsupportedSignatureScheme {
                value: signature.scheme.to_i32(),
            }
        })?;
        ensure!(
            scheme == security_proto::SignatureScheme::SIGNATURE_SCHEME_ED25519PH,
            UnsupportedSignatureSchemeSnafu {
                value: signature.scheme.to_i32()
            }
        );
        let bytes: [u8; SIGNATURE_LENGTH] = signature
            .signature_bytes
            .as_slice()
            .try_into()
            .map_err(|_| FrameSignatureProtoError::InvalidSignatureLength {
                actual: signature.signature_bytes.len(),
            })?;
        Ok(Self::from_bytes(bytes))
    }
}

impl proto::DecodeProtoView for FrameSignature {
    type Error = FrameSignatureProtoError;
    type ProtoView<'a> = DiscoverySignatureView<'a>;

    fn decode_proto_view(
        signature: &Self::ProtoView<'_>,
    ) -> std::result::Result<Self, Self::Error> {
        let scheme = signature.scheme.as_known().ok_or_else(|| {
            FrameSignatureProtoError::UnsupportedSignatureScheme {
                value: signature.scheme.to_i32(),
            }
        })?;
        ensure!(
            scheme == security_proto::SignatureScheme::SIGNATURE_SCHEME_ED25519PH,
            UnsupportedSignatureSchemeSnafu {
                value: signature.scheme.to_i32()
            }
        );
        let bytes: [u8; SIGNATURE_LENGTH] = signature.signature_bytes.try_into().map_err(|_| {
            FrameSignatureProtoError::InvalidSignatureLength {
                actual: signature.signature_bytes.len(),
            }
        })?;
        Ok(Self::from_bytes(bytes))
    }
}

const DOMAIN_SIGNATURE: &[u8] = b"flotsync/security/signature/v1";
const DOMAIN_DISCOVERY_CLAIM_PAYLOAD_SIGNATURE: &[u8] =
    b"flotsync/security/discovery-claim-payload-signature/v1";

/// Build the domain-separated prehash transcript that is signed or verified.
fn signature_transcript(parts: SignedFrameParts<'_>) -> Sha512 {
    let mut transcript = Sha512::new();
    hash_len_prefixed(&mut transcript, DOMAIN_SIGNATURE);
    hash_len_prefixed(&mut transcript, parts.frame_kind.as_bytes());
    hash_len_prefixed(&mut transcript, parts.public_header);
    hash_len_prefixed(&mut transcript, parts.ciphertext);
    transcript
}

/// Build the domain-separated transcript for exact discovery claim payload bytes.
fn discovery_payload_transcript(payload: &[u8]) -> Sha512 {
    let mut transcript = Sha512::new();
    hash_len_prefixed(&mut transcript, DOMAIN_DISCOVERY_CLAIM_PAYLOAD_SIGNATURE);
    hash_len_prefixed(&mut transcript, payload);
    transcript
}

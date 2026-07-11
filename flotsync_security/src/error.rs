use crate::{group::GroupCipherSuite, identity::KeyRole};
use flotsync_core::MemberIdentity;
use flotsync_messages::security as security_proto;
use rand_core::OsError;
use snafu::prelude::*;
use std::fmt;

/// Result type used by security crate operations.
pub type Result<T> = std::result::Result<T, SecurityError>;

/// Errors produced while generating keys, decoding key bundles, signing, or encrypting.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum SecurityError {
    #[snafu(display("Failed to obtain operating system randomness: {source}"))]
    Randomness { source: OsError },
    #[snafu(display("Failed to decode key bundle protobuf: {source}"))]
    DecodeKeyBundle {
        source: flotsync_messages::buffa::DecodeError,
    },
    #[snafu(display("Failed to decode pasteable public key bundle base64: {source}"))]
    DecodePasteablePublicKeyBundle { source: base64::DecodeError },
    #[snafu(display("Key bundle format version {actual} is unsupported; expected {expected}."))]
    UnsupportedKeyBundleVersion { expected: u32, actual: u32 },
    #[snafu(display("Key bundle is missing required field '{field}'."))]
    MissingKeyBundleField { field: &'static str },
    #[snafu(display("Key bundle contains unknown {role} key scheme value {value}."))]
    UnknownKeyScheme { role: KeyRole, value: i32 },
    #[snafu(display("Key bundle is missing a concrete {role} key scheme."))]
    UnspecifiedKeyScheme { role: KeyRole },
    #[snafu(display("Key bundle {role} key uses scheme {actual:?}; expected {expected:?}."))]
    UnexpectedKeyScheme {
        role: KeyRole,
        expected: security_proto::KeyScheme,
        actual: security_proto::KeyScheme,
    },
    #[snafu(display("Key bundle field '{field}' is {actual} bytes, expected {expected} bytes."))]
    InvalidKeyLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[snafu(display("Store secret key is {actual} bytes, expected {expected} bytes."))]
    StoreSecretKeyLength { expected: usize, actual: usize },
    #[snafu(display("Key bundle contains an invalid Ed25519 {role} key: {source}"))]
    InvalidEd25519PublicKey {
        role: KeyRole,
        source: ed25519_dalek::SignatureError,
    },
    #[snafu(display("Key bundle {role} private material does not match its public key."))]
    KeyPairMismatch { role: KeyRole },
    #[snafu(display("Signature bytes are invalid: {source}"))]
    InvalidSignatureBytes {
        source: ed25519_dalek::SignatureError,
    },
    #[snafu(display("Frame signature creation failed: {source}"))]
    SignSignature {
        source: ed25519_dalek::SignatureError,
    },
    #[snafu(display("Frame signature verification failed: {source}"))]
    VerifySignature {
        source: ed25519_dalek::SignatureError,
    },
    #[snafu(display("Group message encryption failed."))]
    GroupSeal,
    #[snafu(display("Group message authentication failed."))]
    GroupOpen,
    #[snafu(display("Stored group secret is {actual} bytes, expected {expected} bytes."))]
    StoredGroupSecretLength { expected: usize, actual: usize },
    #[snafu(display(
        "Stored group secret uses unsupported cipher suite {actual}; expected {expected}."
    ))]
    UnsupportedGroupCipherSuite {
        expected: GroupCipherSuite,
        actual: GroupCipherSuite,
    },
    #[snafu(display("Store secret encryption failed."))]
    StoreSecretSeal,
    #[snafu(display("Store secret authentication failed."))]
    StoreSecretOpen,
    #[snafu(display("HPKE key material could not be decoded: {source}"))]
    HpkeKeyDecode { source: hpke::HpkeError },
    #[snafu(display("Key bundle contains invalid HPKE {role} key material: {source}"))]
    InvalidHpkeKey {
        role: KeyRole,
        source: hpke::HpkeError,
    },
    #[snafu(display("HPKE sealing failed: {source}"))]
    HpkeSeal { source: hpke::HpkeError },
    #[snafu(display("HPKE opening failed: {source}"))]
    HpkeOpen { source: hpke::HpkeError },
    #[snafu(display(
        "Context {member_role} member {context_member} does not match supplied key member {key_member}."
    ))]
    ContextMemberMismatch {
        member_role: ContextMemberRole,
        context_member: MemberIdentity,
        key_member: MemberIdentity,
    },
}

/// Member role whose typed context identity must match the supplied key material.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContextMemberRole {
    /// Sender identity used for signing or signature verification.
    Sender,
    /// Recipient identity used for HPKE sealing or opening.
    Recipient,
}

impl fmt::Display for ContextMemberRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sender => write!(f, "sender"),
            Self::Recipient => write!(f, "recipient"),
        }
    }
}

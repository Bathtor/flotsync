use crate::{
    group::GroupCipherSuite,
    identity::{KeyRole, MemberIdentity},
};
use flotsync_core::member::IdentifierParseError;
use rand_core::OsError;
use snafu::prelude::*;

/// Result type used by security crate operations.
pub type Result<T> = std::result::Result<T, SecurityError>;

/// Errors produced while generating keys, parsing JWKS, signing, or encrypting.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum SecurityError {
    #[snafu(display("Failed to parse JWKS JSON: {source}"))]
    ParseJwks { source: serde_json::Error },
    #[snafu(display("Failed to serialise JWKS JSON: {source}"))]
    SerialiseJwks { source: serde_json::Error },
    #[snafu(display("Failed to obtain operating system randomness: {source}"))]
    Randomness { source: OsError },
    #[snafu(display("JWK key is missing a key id."))]
    MissingKeyId,
    #[snafu(display("JWK key id '{kid}' is missing '#sign' or '#enc' role suffix."))]
    InvalidKeyId { kid: String },
    #[snafu(display("JWK key id '{kid}' contains invalid member identity '{member}': {source}"))]
    InvalidMemberIdentity {
        kid: String,
        member: String,
        source: IdentifierParseError,
    },
    #[snafu(display("JWKS contains keys for multiple members: {first} and {second}."))]
    MixedMembers {
        first: MemberIdentity,
        second: MemberIdentity,
    },
    #[snafu(display("Expected JWKS for member {expected}, but found member {found}."))]
    UnexpectedMember {
        expected: MemberIdentity,
        found: MemberIdentity,
    },
    #[snafu(display("JWK key '{kid}' has unsupported key material."))]
    UnsupportedKeyMaterial { kid: String },
    #[snafu(display(
        "JWK key '{kid}' uses {actual} material while its key id marks it as {expected}."
    ))]
    KeyRoleMismatch {
        kid: String,
        expected: KeyRole,
        actual: KeyRole,
    },
    #[snafu(display("JWK key '{kid}' contains an operation invalid for {role} keys."))]
    InvalidKeyOperation { kid: String, role: KeyRole },
    #[snafu(display("JWKS contains more than one {role} key for member {member}."))]
    DuplicateKey {
        role: KeyRole,
        member: MemberIdentity,
    },
    #[snafu(display("JWKS does not contain any member keys."))]
    EmptyKeySet,
    #[snafu(display("JWKS is missing the {role} key for member {member}."))]
    MissingKey {
        role: KeyRole,
        member: MemberIdentity,
    },
    #[snafu(display("JWK key '{kid}' is missing private key material."))]
    MissingPrivateKey { kid: String },
    #[snafu(display("JWK key '{kid}' contains private key material in a public key set."))]
    UnexpectedPrivateKey { kid: String },
    #[snafu(display(
        "JWK key '{kid}' field '{field}' is {actual} bytes, expected {expected} bytes."
    ))]
    InvalidKeyLength {
        kid: String,
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[snafu(display("Store secret key is {actual} bytes, expected {expected} bytes."))]
    StoreSecretKeyLength { expected: usize, actual: usize },
    #[snafu(display("JWK key '{kid}' contains an invalid Ed25519 public key: {source}"))]
    InvalidEd25519PublicKey {
        kid: String,
        source: ed25519_dalek::SignatureError,
    },
    #[snafu(display("JWK key '{kid}' private material does not match its public key."))]
    KeyPairMismatch { kid: String },
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
    #[snafu(display("JWK key '{kid}' contains invalid HPKE key material: {source}"))]
    InvalidHpkeKey {
        kid: String,
        source: hpke::HpkeError,
    },
    #[snafu(display("HPKE sealing failed: {source}"))]
    HpkeSeal { source: hpke::HpkeError },
    #[snafu(display("HPKE opening failed: {source}"))]
    HpkeOpen { source: hpke::HpkeError },
}

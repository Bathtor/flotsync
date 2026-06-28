use crate::{
    error::{
        InvalidEd25519PublicKeySnafu,
        InvalidHpkeKeySnafu,
        MissingKeyBundleFieldSnafu,
        RandomnessSnafu,
        Result,
        SecurityError,
    },
    fingerprint::{KeyFingerprint, derive_public_key_fingerprint},
    hpke::{HpkeKem, HpkePrivateKey, HpkePublicKey},
    util::fixed_array,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
pub use flotsync_core::MemberIdentity;
use flotsync_messages::{
    buffa::{EnumValue, MessageField, MessageFieldView, UnknownFieldsView, ViewEncode as _},
    proto::{DecodeProtoViewWith, DecodeProtoWith, EncodeProto, FromProtoDecodeError},
    security as security_proto,
};
use hpke::{Deserializable, Kem, Serializable};
use rand_core::{OsRng, TryRngCore};
use snafu::prelude::*;
use std::fmt;
use zeroize::Zeroizing;

/// Generate fresh local-private and public member key bundles.
///
/// # Errors
///
/// Returns [`SecurityError::Randomness`] if the operating system random source
/// fails.
pub fn generate_member_key_bundles(member_id: MemberIdentity) -> Result<GeneratedMemberKeyBundles> {
    let mut seed = Zeroizing::new([0u8; MEMBER_KEY_SEED_LENGTH]);
    OsRng
        .try_fill_bytes(seed.as_mut())
        .context(RandomnessSnafu)?;
    Ok(generate_member_key_bundles_from_seed(member_id, &seed))
}

/// Encode public member keys as a canonical protobuf public key bundle.
#[must_use]
pub fn encode_public_key_bundle(public_keys: &PublicMemberKeys) -> Vec<u8> {
    public_keys.encode_proto_to_vec()
}

/// Decode a canonical protobuf public key bundle for `member_id`.
///
/// The bundle itself is identity-free. The caller supplies the identity from
/// the row, provisioning API, or higher-level protocol context.
///
/// # Errors
///
/// Returns [`SecurityError`] when the bundle is malformed, uses an unsupported
/// version or scheme, or contains invalid public key bytes.
pub fn public_member_keys_from_public_bundle(
    input: &[u8],
    member_id: MemberIdentity,
) -> Result<PublicMemberKeys> {
    PublicMemberKeys::decode_proto_from_slice_with(input, member_id)
}

/// Encode local private keys as canonical protobuf private key bundle bytes.
#[must_use]
pub fn encode_local_private_key_bundle(
    local_keys: &LocalMemberKeys,
) -> EncodedLocalPrivateKeyBundle {
    // hpke's X25519 wrapper only exposes serialization, not borrowed key bytes.
    let mut encryption_public = [0u8; X25519_KEY_LENGTH];
    local_keys
        .public_keys
        .hpke_public_key
        .write_exact(&mut encryption_public);
    let mut encryption_private = Zeroizing::new([0u8; X25519_KEY_LENGTH]);
    local_keys
        .hpke_private_key
        .write_exact(encryption_private.as_mut_slice());
    let view = security_proto::LocalPrivateKeyBundleView {
        format_version: KEY_BUNDLE_FORMAT_VERSION_V1,
        signing_key: MessageFieldView::some(security_proto::PrivateKeyView {
            scheme: EnumValue::from(security_proto::KeyScheme::KEY_SCHEME_ED25519),
            public_key: local_keys.public_keys.signing_key.as_bytes().as_slice(),
            private_key: local_keys.signing_key.as_bytes().as_slice(),
            __buffa_unknown_fields: UnknownFieldsView::new(),
        }),
        encryption_key: MessageFieldView::some(security_proto::PrivateKeyView {
            scheme: EnumValue::from(security_proto::KeyScheme::KEY_SCHEME_X25519),
            public_key: encryption_public.as_slice(),
            private_key: encryption_private.as_slice(),
            __buffa_unknown_fields: UnknownFieldsView::new(),
        }),
        __buffa_unknown_fields: UnknownFieldsView::new(),
    };
    let bytes = view.encode_to_vec();
    EncodedLocalPrivateKeyBundle::new(bytes)
}

/// Decode canonical protobuf local private key bundle bytes for `member_id`.
///
/// The bundle itself is identity-free. The caller supplies the identity from
/// the local member row or equivalent provisioning context.
///
/// # Errors
///
/// Returns [`SecurityError`] when the bundle is malformed, uses an unsupported
/// version or scheme, contains invalid key bytes, or the private material does
/// not match the embedded public material.
pub fn local_member_keys_from_private_bundle(
    input: &[u8],
    member_id: MemberIdentity,
) -> Result<LocalMemberKeys> {
    LocalMemberKeys::decode_proto_view_from_slice_with(input, member_id)
}

/// Protocol role of one member key inside a Flotsync key bundle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyRole {
    /// Ed25519 key used for detached Ed25519ph frame signatures.
    Signing,
    /// X25519 key used by HPKE for member-targeted encryption.
    Encryption,
}

impl fmt::Display for KeyRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Signing => write!(f, "signing"),
            Self::Encryption => write!(f, "encryption"),
        }
    }
}

/// Local member key material used for signing frames and opening HPKE payloads.
///
/// This type owns private key material and deliberately does not implement
/// `Clone`; share it via [`std::rc::Rc`] or [`std::sync::Arc`] when multiple
/// owners need access.
pub struct LocalMemberKeys {
    /// Public identity keys that can be shared with trusted peers.
    pub(crate) public_keys: PublicMemberKeys,
    /// Local Ed25519 signing key; never expose through public accessors.
    pub(crate) signing_key: SigningKey,
    /// Local HPKE private key used to open member-targeted payloads.
    pub(crate) hpke_private_key: HpkePrivateKey,
}

impl LocalMemberKeys {
    /// Return the public keys that identify this local member to trusted peers.
    #[must_use]
    pub fn public_keys(&self) -> &PublicMemberKeys {
        &self.public_keys
    }

    /// Return the member identity associated with these keys.
    #[must_use]
    pub fn member_id(&self) -> &MemberIdentity {
        self.public_keys.member_id()
    }
}

impl fmt::Debug for LocalMemberKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemberKeys")
            .field("public_keys", &self.public_keys)
            .field("signing_key", &"<redacted>")
            .field("hpke_private_key", &"<redacted>")
            .finish()
    }
}

impl DecodeProtoViewWith<MemberIdentity> for LocalMemberKeys {
    type ProtoView<'a> = security_proto::LocalPrivateKeyBundleView<'a>;
    type Error = SecurityError;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        member_id: MemberIdentity,
    ) -> Result<Self> {
        validate_bundle_version(proto.format_version)?;
        let signing = required_field_view(proto.signing_key.as_option(), "signing_key")?;
        let encryption = required_field_view(proto.encryption_key.as_option(), "encryption_key")?;
        validate_key_scheme(KeyRole::Signing, signing.scheme)?;
        validate_key_scheme(KeyRole::Encryption, encryption.scheme)?;

        let signing_public =
            fixed_key_bytes::<ED25519_KEY_LENGTH>("signing_key.public_key", signing.public_key)?;
        let signing_private = Zeroizing::new(fixed_key_bytes::<ED25519_KEY_LENGTH>(
            "signing_key.private_key",
            signing.private_key,
        )?);
        let encryption_public = fixed_key_bytes::<X25519_KEY_LENGTH>(
            "encryption_key.public_key",
            encryption.public_key,
        )?;
        let encryption_private = Zeroizing::new(fixed_key_bytes::<X25519_KEY_LENGTH>(
            "encryption_key.private_key",
            encryption.private_key,
        )?);

        let signing_key = SigningKey::from_bytes(&signing_private);
        let verifying_key = signing_key.verifying_key();
        if verifying_key.to_bytes() != signing_public {
            return Err(SecurityError::KeyPairMismatch {
                role: KeyRole::Signing,
            });
        }
        let hpke_private_key =
            HpkePrivateKey::from_bytes(&encryption_private[..]).context(InvalidHpkeKeySnafu {
                role: KeyRole::Encryption,
            })?;
        let hpke_public_key = HpkeKem::sk_to_pk(&hpke_private_key);
        if hpke_public_key.to_bytes().as_slice() != encryption_public {
            return Err(SecurityError::KeyPairMismatch {
                role: KeyRole::Encryption,
            });
        }
        Ok(Self {
            public_keys: PublicMemberKeys {
                member_id,
                signing_key: verifying_key,
                hpke_public_key,
            },
            signing_key,
            hpke_private_key,
        })
    }
}

/// Public member keys copied to trusted peers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicMemberKeys {
    member_id: MemberIdentity,
    /// Public Ed25519 verifying key for this member.
    pub(crate) signing_key: VerifyingKey,
    /// Public HPKE encryption key for member-targeted payloads.
    pub(crate) hpke_public_key: HpkePublicKey,
}

impl PublicMemberKeys {
    /// Build public member keys from raw public key bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] if either public key cannot be decoded for the
    /// selected cryptographic primitive.
    pub fn from_key_bytes(
        member_id: MemberIdentity,
        signing_key: [u8; ED25519_KEY_LENGTH],
        encryption_key: [u8; X25519_KEY_LENGTH],
    ) -> Result<Self> {
        let signing_key =
            VerifyingKey::from_bytes(&signing_key).context(InvalidEd25519PublicKeySnafu {
                role: KeyRole::Signing,
            })?;
        let hpke_public_key =
            HpkePublicKey::from_bytes(&encryption_key).context(InvalidHpkeKeySnafu {
                role: KeyRole::Encryption,
            })?;
        Ok(Self {
            member_id,
            signing_key,
            hpke_public_key,
        })
    }

    /// Return the member identity associated with these public keys.
    #[must_use]
    pub fn member_id(&self) -> &MemberIdentity {
        &self.member_id
    }

    /// Return the raw Ed25519 verifying key bytes.
    #[must_use]
    pub fn signing_key_bytes(&self) -> [u8; ED25519_KEY_LENGTH] {
        self.signing_key.to_bytes()
    }

    /// Return the raw X25519 HPKE public key bytes.
    #[must_use]
    pub fn encryption_key_bytes(&self) -> [u8; X25519_KEY_LENGTH] {
        fixed_array(self.hpke_public_key.to_bytes().as_slice())
    }

    /// Derive the stable fingerprint for this exact public key material.
    #[must_use]
    pub fn fingerprint(&self) -> KeyFingerprint {
        derive_public_key_fingerprint(
            self.signing_key.as_bytes().as_slice(),
            self.hpke_public_key.to_bytes().as_slice(),
        )
    }
}

impl EncodeProto for PublicMemberKeys {
    type Proto = security_proto::PublicKeyBundle;

    fn encode_proto(&self) -> Self::Proto {
        security_proto::PublicKeyBundle {
            format_version: KEY_BUNDLE_FORMAT_VERSION_V1,
            signing_key: MessageField::some(security_proto::PublicKey {
                scheme: EnumValue::from(security_proto::KeyScheme::KEY_SCHEME_ED25519),
                public_key: self.signing_key.to_bytes().to_vec(),
                ..security_proto::PublicKey::default()
            }),
            encryption_key: MessageField::some(security_proto::PublicKey {
                scheme: EnumValue::from(security_proto::KeyScheme::KEY_SCHEME_X25519),
                public_key: self.hpke_public_key.to_bytes().as_slice().to_vec(),
                ..security_proto::PublicKey::default()
            }),
            ..security_proto::PublicKeyBundle::default()
        }
    }
}

impl DecodeProtoWith<MemberIdentity> for PublicMemberKeys {
    type Proto = security_proto::PublicKeyBundle;
    type Error = SecurityError;

    fn decode_proto_with(mut proto: Self::Proto, member_id: MemberIdentity) -> Result<Self> {
        validate_bundle_version(proto.format_version)?;
        let signing = take_required_field(&mut proto.signing_key, "signing_key")?;
        let encryption = take_required_field(&mut proto.encryption_key, "encryption_key")?;
        validate_key_scheme(KeyRole::Signing, signing.scheme)?;
        validate_key_scheme(KeyRole::Encryption, encryption.scheme)?;

        let signing_key =
            fixed_key_bytes::<ED25519_KEY_LENGTH>("signing_key.public_key", &signing.public_key)?;
        let encryption_key = fixed_key_bytes::<X25519_KEY_LENGTH>(
            "encryption_key.public_key",
            &encryption.public_key,
        )?;
        Self::from_key_bytes(member_id, signing_key, encryption_key)
    }
}

/// Encoded local private-key bundle bytes.
///
/// The inner buffer is zeroised on drop and redacted in debug output. Use
/// [`Self::as_bytes`] at the point where the encrypted store plaintext must be
/// sealed or written.
pub struct EncodedLocalPrivateKeyBundle(Zeroizing<Vec<u8>>);

impl EncodedLocalPrivateKeyBundle {
    /// Wrap encoded private key bundle bytes so their buffer is zeroised on drop.
    #[must_use]
    pub fn new(value: Vec<u8>) -> Self {
        Self(Zeroizing::new(value))
    }

    /// Borrow the encoded private key bundle bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for EncodedLocalPrivateKeyBundle {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Debug for EncodedLocalPrivateKeyBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("EncodedLocalPrivateKeyBundle")
            .field(&"<redacted>")
            .finish()
    }
}

/// Generated member key material serialised as protobuf key bundles.
///
/// This type deliberately does not implement `Clone` because it owns an
/// [`EncodedLocalPrivateKeyBundle`] value. Share the bundle via [`std::rc::Rc`]
/// or [`std::sync::Arc`] if multiple owners need to coordinate the same
/// generated material.
pub struct GeneratedMemberKeyBundles {
    /// Encoded local private-key bundle for the member identity.
    pub local_private_bundle: EncodedLocalPrivateKeyBundle,
    /// Encoded public key bundle that can be copied to trusted peers.
    pub public_bundle: Vec<u8>,
}

impl fmt::Debug for GeneratedMemberKeyBundles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeneratedMemberKeyBundles")
            .field("local_private_bundle", &"<redacted>")
            .field("public_bundle_len", &self.public_bundle.len())
            .finish()
    }
}

/// Byte length of deterministic seed material for generated member keys.
pub(crate) const MEMBER_KEY_SEED_LENGTH: usize = 64;
/// Byte length of Ed25519 public and private seed keys.
pub const ED25519_KEY_LENGTH: usize = 32;
/// Byte length of X25519 HPKE public and private keys.
pub const X25519_KEY_LENGTH: usize = 32;

const KEY_BUNDLE_FORMAT_VERSION_V1: u32 = 1;

/// Generate member key bundles from deterministic seed material.
pub(crate) fn generate_member_key_bundles_from_seed(
    member_id: MemberIdentity,
    seed: &[u8; MEMBER_KEY_SEED_LENGTH],
) -> GeneratedMemberKeyBundles {
    let local_keys = local_member_keys_from_seed(member_id, seed);
    let local_private_bundle = encode_local_private_key_bundle(&local_keys);
    let public_bundle = encode_public_key_bundle(local_keys.public_keys());
    GeneratedMemberKeyBundles {
        local_private_bundle,
        public_bundle,
    }
}

/// Derive deterministic member key material from seed bytes.
fn local_member_keys_from_seed(
    member_id: MemberIdentity,
    seed: &[u8; MEMBER_KEY_SEED_LENGTH],
) -> LocalMemberKeys {
    let (signing_seed, hpke_seed) = seed.split_at(ED25519_KEY_LENGTH);
    let signing_seed = Zeroizing::new(fixed_array(signing_seed));

    // Ed25519 expands the first half as the signing seed; DHKEM(X25519,
    // HKDF-SHA256) deterministically derives the encryption key pair from the
    // second half. Production callers get the seed from OS randomness.
    let signing_key = SigningKey::from_bytes(&signing_seed);
    let signing_public_key = signing_key.verifying_key();
    let (hpke_private_key, hpke_public_key) = HpkeKem::derive_keypair(hpke_seed);
    let public_keys = PublicMemberKeys {
        member_id,
        signing_key: signing_public_key,
        hpke_public_key,
    };
    LocalMemberKeys {
        public_keys,
        signing_key,
        hpke_private_key,
    }
}

impl FromProtoDecodeError for SecurityError {
    fn from_proto_decode_error(source: flotsync_messages::buffa::DecodeError) -> Self {
        SecurityError::DecodeKeyBundle { source }
    }
}

/// Validate the identity-free key bundle version field.
fn validate_bundle_version(format_version: u32) -> Result<()> {
    if format_version != KEY_BUNDLE_FORMAT_VERSION_V1 {
        return Err(SecurityError::UnsupportedKeyBundleVersion {
            expected: KEY_BUNDLE_FORMAT_VERSION_V1,
            actual: format_version,
        });
    }
    Ok(())
}

/// Extract a required key bundle message field.
fn take_required_field<T: Default>(
    field: &mut MessageField<T>,
    field_name: &'static str,
) -> Result<T> {
    field
        .take()
        .context(MissingKeyBundleFieldSnafu { field: field_name })
}

/// Extract a required key bundle view field.
fn required_field_view<'a, T>(field: Option<&'a T>, field_name: &'static str) -> Result<&'a T> {
    field.context(MissingKeyBundleFieldSnafu { field: field_name })
}

/// Validate that a generic key scheme is supported in the expected bundle role.
fn validate_key_scheme(role: KeyRole, scheme: EnumValue<security_proto::KeyScheme>) -> Result<()> {
    let scheme = scheme
        .as_known()
        .ok_or_else(|| SecurityError::UnknownKeyScheme {
            role,
            value: scheme.to_i32(),
        })?;
    if scheme == security_proto::KeyScheme::KEY_SCHEME_UNSPECIFIED {
        return Err(SecurityError::UnspecifiedKeyScheme { role });
    }
    let expected = match role {
        KeyRole::Signing => security_proto::KeyScheme::KEY_SCHEME_ED25519,
        KeyRole::Encryption => security_proto::KeyScheme::KEY_SCHEME_X25519,
    };
    if scheme != expected {
        return Err(SecurityError::UnexpectedKeyScheme {
            role,
            expected,
            actual: scheme,
        });
    }
    Ok(())
}

/// Decode fixed-width protobuf key bytes, or report the offending field.
fn fixed_key_bytes<const N: usize>(field: &'static str, bytes: &[u8]) -> Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| SecurityError::InvalidKeyLength {
            field,
            expected: N,
            actual: bytes.len(),
        })
}

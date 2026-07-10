use crate::{
    error::{
        DecodePasteablePublicKeyBundleSnafu,
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
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ed25519_dalek::{SigningKey, VerifyingKey};
pub use flotsync_core::MemberIdentity;
use flotsync_messages::{
    buffa::{EnumValue, MessageField, MessageFieldView, UnknownFieldsView, ViewEncode as _},
    proto::{
        DecodeProto,
        DecodeProtoView,
        DecodeProtoViewWith,
        DecodeProtoWith,
        EncodeProto,
        FromProtoDecodeError,
    },
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
    public_keys.public_key_bundle().to_bytes()
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
    PublicKeyBundle::from_bytes(input).map(|bundle| bundle.bind_member(member_id))
}

/// Encode local private keys as canonical protobuf private key bundle bytes.
#[must_use]
pub fn encode_local_private_key_bundle(
    local_keys: &LocalMemberKeys,
) -> EncodedLocalPrivateKeyBundle {
    let encryption_public = local_keys.public_keys.hpke_public_key.as_slice();
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
            public_key: encryption_public,
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

/// Validated HPKE public key bytes owned by Flotsync public key types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HpkePublicKeyBytes([u8; X25519_KEY_LENGTH]);

impl HpkePublicKeyBytes {
    /// Validate and store raw HPKE public key bytes from a decoded source field.
    pub(crate) fn from_slice(field: &'static str, bytes: &[u8]) -> Result<Self> {
        let bytes = fixed_key_bytes::<X25519_KEY_LENGTH>(field, bytes)?;
        Self::from_array(bytes)
    }

    /// Validate and store fixed-width HPKE public key bytes.
    fn from_array(bytes: [u8; X25519_KEY_LENGTH]) -> Result<Self> {
        HpkePublicKey::from_bytes(&bytes).context(InvalidHpkeKeySnafu {
            role: KeyRole::Encryption,
        })?;
        Ok(Self(bytes))
    }

    /// Store a library HPKE public key as Flotsync-owned bytes.
    fn from_hpke_public_key(public_key: &HpkePublicKey) -> Self {
        Self(fixed_array(public_key.to_bytes().as_slice()))
    }

    /// Borrow the canonical public key bytes.
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Convert to the HPKE library type for a cryptographic operation.
    pub(crate) fn to_hpke_public_key(&self) -> Result<HpkePublicKey> {
        HpkePublicKey::from_bytes(&self.0).context(InvalidHpkeKeySnafu {
            role: KeyRole::Encryption,
        })
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
        let signing_private =
            fixed_key_bytes::<ED25519_KEY_LENGTH>("signing_key.private_key", signing.private_key)?;
        let signing_private = Zeroizing::new(signing_private);
        let encryption_public = fixed_key_bytes::<X25519_KEY_LENGTH>(
            "encryption_key.public_key",
            encryption.public_key,
        )?;
        let encryption_private = fixed_key_bytes::<X25519_KEY_LENGTH>(
            "encryption_key.private_key",
            encryption.private_key,
        )?;
        let encryption_private = Zeroizing::new(encryption_private);

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
        let hpke_public_key = HpkePublicKeyBytes::from_array(encryption_public)?;
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

/// Shareable public key material without a member identity.
///
/// A public key bundle is the material an application can show, copy, encode,
/// or send to another user before any member identity or local trust decision
/// has been attached. It deliberately contains no member identity, trust
/// evidence, private key material, policy state, or presentation metadata.
/// Bind it to a [`MemberIdentity`] with [`Self::bind_member`] only after the
/// application has decided which member the bundle should represent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicKeyBundle {
    /// Public Ed25519 verifying key for detached frame signatures.
    pub(crate) signing_key: VerifyingKey,
    /// Public X25519 HPKE key for member-targeted encryption.
    pub(crate) hpke_public_key: HpkePublicKeyBytes,
}

impl PublicKeyBundle {
    /// Build an identity-free public key bundle from raw public key bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] if either public key cannot be decoded for the
    /// selected cryptographic primitive.
    fn from_key_bytes(signing_key: &[u8], encryption_key: &[u8]) -> Result<Self> {
        let signing_key =
            fixed_key_bytes::<ED25519_KEY_LENGTH>("signing_key.public_key", signing_key)?;
        let signing_key =
            VerifyingKey::from_bytes(&signing_key).context(InvalidEd25519PublicKeySnafu {
                role: KeyRole::Signing,
            })?;
        let hpke_public_key =
            HpkePublicKeyBytes::from_slice("encryption_key.public_key", encryption_key)?;
        Ok(Self {
            signing_key,
            hpke_public_key,
        })
    }

    /// Build an identity-free bundle from existing identity-bound public member keys.
    #[must_use]
    pub fn from_public_member_keys(public_keys: &PublicMemberKeys) -> Self {
        Self {
            signing_key: public_keys.signing_key,
            hpke_public_key: public_keys.hpke_public_key.clone(),
        }
    }

    /// Bind this public key bundle to an explicit member identity.
    ///
    /// Binding does not record trust or make the member authoritative. It only
    /// produces typed member-key material that higher-level APIs can assess or
    /// store according to their own policy.
    #[must_use]
    pub fn bind_member(self, member_id: MemberIdentity) -> PublicMemberKeys {
        PublicMemberKeys {
            member_id,
            signing_key: self.signing_key,
            hpke_public_key: self.hpke_public_key,
        }
    }

    /// Derive the stable fingerprint for this exact public key material.
    ///
    /// The fingerprint is safe to show to users for comparison. It is derived
    /// only from the public key material and is independent of member identity.
    #[must_use]
    pub fn fingerprint(&self) -> KeyFingerprint {
        derive_public_key_fingerprint(
            self.signing_key.as_bytes().as_slice(),
            self.hpke_public_key.as_slice(),
        )
    }

    /// Encode this bundle as canonical Flotsync public-key-bundle bytes.
    ///
    /// Use this form when the bytes will be stored or transported by another
    /// structured format.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.encode_proto_to_vec()
    }

    /// Decode a bundle from canonical Flotsync public-key-bundle bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] when the bundle is malformed, uses an
    /// unsupported version or scheme, or contains invalid public key bytes.
    pub fn from_bytes(input: &[u8]) -> Result<Self> {
        Self::decode_proto_from_slice(input)
    }

    /// Encode this bundle as recommended pasteable text for users and CLIs.
    #[must_use]
    pub fn to_pasteable_string(&self) -> String {
        STANDARD.encode(self.to_bytes())
    }

    /// Decode a bundle from recommended pasteable text.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] when the string is not valid base64 or does
    /// not contain a valid public key bundle.
    pub fn from_pasteable_string(input: &str) -> Result<Self> {
        let bytes = STANDARD
            .decode(input)
            .context(DecodePasteablePublicKeyBundleSnafu)?;
        Self::from_bytes(&bytes)
    }
}

impl EncodeProto for PublicKeyBundle {
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
                public_key: self.hpke_public_key.as_slice().to_vec(),
                ..security_proto::PublicKey::default()
            }),
            ..security_proto::PublicKeyBundle::default()
        }
    }
}

impl DecodeProto for PublicKeyBundle {
    type Proto = security_proto::PublicKeyBundle;
    type Error = SecurityError;

    fn decode_proto(mut proto: Self::Proto) -> Result<Self> {
        validate_bundle_version(proto.format_version)?;
        let signing = take_required_field(&mut proto.signing_key, "signing_key")?;
        let encryption = take_required_field(&mut proto.encryption_key, "encryption_key")?;
        validate_key_scheme(KeyRole::Signing, signing.scheme)?;
        validate_key_scheme(KeyRole::Encryption, encryption.scheme)?;

        Self::from_key_bytes(&signing.public_key, &encryption.public_key)
    }
}

impl DecodeProtoView for PublicKeyBundle {
    type ProtoView<'a> = security_proto::PublicKeyBundleView<'a>;
    type Error = SecurityError;

    fn decode_proto_view(proto: &Self::ProtoView<'_>) -> Result<Self> {
        validate_bundle_version(proto.format_version)?;
        let signing = required_field_view(proto.signing_key.as_option(), "signing_key")?;
        let encryption = required_field_view(proto.encryption_key.as_option(), "encryption_key")?;
        validate_key_scheme(KeyRole::Signing, signing.scheme)?;
        validate_key_scheme(KeyRole::Encryption, encryption.scheme)?;

        Self::from_key_bytes(signing.public_key, encryption.public_key)
    }
}

/// Public member keys copied to trusted peers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicMemberKeys {
    member_id: MemberIdentity,
    /// Public Ed25519 verifying key for this member.
    pub(crate) signing_key: VerifyingKey,
    /// Public HPKE encryption key for member-targeted payloads.
    pub(crate) hpke_public_key: HpkePublicKeyBytes,
}

impl PublicMemberKeys {
    /// Build identity-bound public member keys from raw public key bytes.
    ///
    /// This is a low-level constructor for typed records that already separate
    /// key bytes from member identity, such as local store records. Most
    /// application code should receive or decode a [`PublicKeyBundle`] first
    /// and bind it with [`PublicKeyBundle::bind_member`].
    ///
    /// # Errors
    ///
    /// Returns [`SecurityError`] if either public key cannot be decoded for the
    /// selected cryptographic primitive.
    pub fn from_key_bytes(
        member_id: MemberIdentity,
        signing_key: &[u8],
        encryption_key: &[u8],
    ) -> Result<Self> {
        PublicKeyBundle::from_key_bytes(signing_key, encryption_key)
            .map(|bundle| bundle.bind_member(member_id))
    }

    /// Return the member identity associated with these public keys.
    #[must_use]
    pub fn member_id(&self) -> &MemberIdentity {
        &self.member_id
    }

    /// Return the raw Ed25519 verifying key bytes.
    #[must_use]
    pub fn signing_key_bytes(&self) -> &[u8] {
        self.signing_key.as_bytes()
    }

    /// Return the raw X25519 HPKE public key bytes.
    #[must_use]
    pub fn encryption_key_bytes(&self) -> &[u8] {
        self.hpke_public_key.as_slice()
    }

    /// Derive the stable fingerprint for this exact public key material.
    #[must_use]
    pub fn fingerprint(&self) -> KeyFingerprint {
        self.public_key_bundle().fingerprint()
    }

    /// Return an identity-free view of this member's public key material.
    #[must_use]
    pub fn public_key_bundle(&self) -> PublicKeyBundle {
        PublicKeyBundle::from_public_member_keys(self)
    }
}

impl EncodeProto for PublicMemberKeys {
    type Proto = security_proto::PublicKeyBundle;

    fn encode_proto(&self) -> Self::Proto {
        self.public_key_bundle().encode_proto()
    }
}

impl DecodeProtoWith<MemberIdentity> for PublicMemberKeys {
    type Proto = security_proto::PublicKeyBundle;
    type Error = SecurityError;

    fn decode_proto_with(proto: Self::Proto, member_id: MemberIdentity) -> Result<Self> {
        PublicKeyBundle::decode_proto(proto).map(|bundle| bundle.bind_member(member_id))
    }
}

impl DecodeProtoViewWith<MemberIdentity> for PublicMemberKeys {
    type ProtoView<'a> = security_proto::PublicKeyBundleView<'a>;
    type Error = SecurityError;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        member_id: MemberIdentity,
    ) -> Result<Self> {
        PublicKeyBundle::decode_proto_view(proto).map(|bundle| bundle.bind_member(member_id))
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
    let hpke_public_key = HpkePublicKeyBytes::from_hpke_public_key(&hpke_public_key);
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

use crate::{
    error::{
        InvalidEd25519PublicKeySnafu,
        InvalidHpkeKeySnafu,
        InvalidMemberIdentitySnafu,
        ParseJwksSnafu,
        RandomnessSnafu,
        Result,
        SecurityError,
        SerialiseJwksSnafu,
    },
    hpke::{HpkeKem, HpkePrivateKey, HpkePublicKey},
    util::fixed_array,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use flotsync_core::member::Identifier;
use flotsync_utils::option_when;
use hpke::{Deserializable, Kem, Serializable};
use jose_jwk::{
    Class,
    Jwk,
    JwkSet,
    Key as JwkKey,
    Okp,
    OkpCurves,
    Operations,
    Parameters,
    jose_b64::serde::Secret,
};
use rand_core::{OsRng, TryRngCore};
use snafu::prelude::*;
use std::{collections::BTreeSet, fmt, str::FromStr};
use zeroize::{Zeroize, Zeroizing};

pub type MemberIdentity = Identifier;

/// Generate a fresh pair of local-private and public member JWKS documents.
///
/// # Errors
///
/// Returns [`SecurityError::Randomness`] if the operating system random source
/// fails, or [`SecurityError::SerialiseJwks`] if the generated keys cannot be
/// encoded as JWKS JSON.
pub fn generate_member_key_files(member_id: MemberIdentity) -> Result<GeneratedMemberKeyFiles> {
    let mut seed = Zeroizing::new([0u8; MEMBER_KEY_SEED_LENGTH]);
    OsRng
        .try_fill_bytes(seed.as_mut())
        .context(RandomnessSnafu)?;
    generate_member_key_files_from_seed(member_id, &seed)
}

/// Parse the local-private member JWKS document for a configured member.
///
/// # Errors
///
/// Returns [`SecurityError`] when the input is not valid JWKS JSON, the keys do
/// not match the expected member, either required key is absent, the key roles
/// are inconsistent, or private key material is missing or mismatched.
pub fn local_member_keys_from_jwks(
    input: &str,
    expected_member: Option<&MemberIdentity>,
) -> Result<LocalMemberKeys> {
    let parsed = ParsedMemberKeys::from_jwks(input, expected_member)?;
    parsed.into_local_keys()
}

/// Parse the public member JWKS document for a trusted member.
///
/// # Errors
///
/// Returns [`SecurityError`] when the input is not valid JWKS JSON, the keys do
/// not match the expected member, either required key is absent, the key roles
/// are inconsistent, or private key material is present.
pub fn public_member_keys_from_jwks(
    input: &str,
    expected_member: Option<&MemberIdentity>,
) -> Result<PublicMemberKeys> {
    let parsed = ParsedMemberKeys::from_jwks(input, expected_member)?;
    parsed.into_public_keys()
}

/// Protocol role of one member key inside a JWKS document.
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
                kid: format!("{member_id}{KEY_ID_SIGNING_SUFFIX}"),
            })?;
        let hpke_public_key =
            HpkePublicKey::from_bytes(&encryption_key).context(InvalidHpkeKeySnafu {
                kid: format!("{member_id}{KEY_ID_ENCRYPTION_SUFFIX}"),
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
}

/// Local-private JWKS JSON containing member signing and HPKE private keys.
///
/// The inner string is zeroised on drop and redacted in debug output. Use
/// [`Self::as_str`] at the point where the key file must be written or parsed.
/// This type deliberately does not implement `Clone`: share it via
/// [`std::rc::Rc`] or [`std::sync::Arc`] when multiple owners need access, so
/// the plaintext private JWKS is not duplicated into multiple buffers that each
/// need to be zeroised on drop.
pub struct PrivateJwks(Zeroizing<String>);

impl PrivateJwks {
    /// Wrap private JWKS JSON so its buffer is zeroised on drop.
    #[must_use]
    pub fn new(value: String) -> Self {
        Self(Zeroizing::new(value))
    }

    /// Borrow the private JWKS JSON for parsing or writing to a configured file.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for PrivateJwks {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for PrivateJwks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("PrivateJwks").field(&"<redacted>").finish()
    }
}

/// Generated member key material serialised as local-private and public JWKS files.
///
/// This type deliberately does not implement `Clone` because it owns a
/// [`PrivateJwks`] value. Share the bundle via [`std::rc::Rc`] or
/// [`std::sync::Arc`] if multiple owners need to coordinate writing the same
/// generated files.
pub struct GeneratedMemberKeyFiles {
    /// Local-private JWKS JSON for the member identity.
    pub local_private_jwks: PrivateJwks,
    /// Public JWKS JSON that can be copied to trusted peers.
    pub public_jwks: String,
}

impl fmt::Debug for GeneratedMemberKeyFiles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeneratedMemberKeyFiles")
            .field("local_private_jwks", &"<redacted>")
            .field("public_jwks", &self.public_jwks)
            .finish()
    }
}

/// Byte length of deterministic seed material for generated member keys.
pub(crate) const MEMBER_KEY_SEED_LENGTH: usize = 64;

/// Generate member JWKS documents from deterministic seed material.
///
/// # Errors
///
/// Returns [`SecurityError::SerialiseJwks`] if the generated keys cannot be
/// encoded as JWKS JSON.
pub(crate) fn generate_member_key_files_from_seed(
    member_id: MemberIdentity,
    seed: &[u8; MEMBER_KEY_SEED_LENGTH],
) -> Result<GeneratedMemberKeyFiles> {
    let local_keys = local_member_keys_from_seed(member_id, seed);
    let local_private_jwks = member_keys_to_jwks(&local_keys, KeyScope::Private)?;
    let public_jwks = member_keys_to_jwks(&local_keys, KeyScope::Public)?;
    Ok(GeneratedMemberKeyFiles {
        local_private_jwks: PrivateJwks::new(local_private_jwks),
        public_jwks,
    })
}

const KEY_ID_SIGNING_SUFFIX: &str = "#sign";
const KEY_ID_ENCRYPTION_SUFFIX: &str = "#enc";
/// Byte length of Ed25519 public keys.
pub const ED25519_KEY_LENGTH: usize = 32;
/// Byte length of X25519 HPKE public keys.
pub const X25519_KEY_LENGTH: usize = 32;

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

/// Serialise local member keys to either local-private or public JWKS JSON.
fn member_keys_to_jwks(local_keys: &LocalMemberKeys, key_scope: KeyScope) -> Result<String> {
    let member_id = local_keys.member_id().to_string();
    let signing_private_key: Option<Secret<_>> = option_when!(key_scope.is_private(), {
        Box::<[u8]>::from(local_keys.signing_key.to_bytes()).into()
    });
    let hpke_private_key: Option<Secret<_>> = option_when!(key_scope.is_private(), {
        let mut bytes = local_keys.hpke_private_key.to_bytes();
        let private_key = bytes.to_vec().into_boxed_slice().into();
        bytes.zeroize();
        private_key
    });
    let set = JwkSet {
        keys: vec![
            Jwk {
                key: JwkKey::Okp(Okp {
                    crv: OkpCurves::Ed25519,
                    x: local_keys
                        .public_keys
                        .signing_key
                        .to_bytes()
                        .to_vec()
                        .into(),
                    d: signing_private_key,
                }),
                prm: key_parameters(
                    format!("{member_id}{KEY_ID_SIGNING_SUFFIX}"),
                    KeyRole::Signing,
                    key_scope,
                ),
            },
            Jwk {
                key: JwkKey::Okp(Okp {
                    crv: OkpCurves::X25519,
                    x: local_keys
                        .public_keys
                        .hpke_public_key
                        .to_bytes()
                        .as_slice()
                        .to_vec()
                        .into(),
                    d: hpke_private_key,
                }),
                prm: key_parameters(
                    format!("{member_id}{KEY_ID_ENCRYPTION_SUFFIX}"),
                    KeyRole::Encryption,
                    key_scope,
                ),
            },
        ],
    };
    serde_json::to_string(&set).context(SerialiseJwksSnafu)
}

/// Build common JWK parameters for one generated member key.
fn key_parameters(kid: String, role: KeyRole, key_scope: KeyScope) -> Parameters {
    let mut ops = BTreeSet::new();
    let cls = match role {
        KeyRole::Signing => {
            match key_scope {
                KeyScope::Public => ops.insert(Operations::Verify),
                KeyScope::Private => ops.insert(Operations::Sign),
            };
            Some(Class::Signing)
        }
        KeyRole::Encryption => {
            // Flotsync uses X25519 in HPKE, not JOSE ECDH-ES directly. JWK has no
            // encap/decap key_ops values, so these describe the key's envelope
            // role in our protocol rather than strict JOSE key-agreement verbs.
            match key_scope {
                KeyScope::Public => ops.insert(Operations::Encrypt),
                KeyScope::Private => ops.insert(Operations::Decrypt),
            };
            Some(Class::Encryption)
        }
    };
    Parameters {
        kid: Some(kid),
        cls,
        ops: Some(ops),
        ..Default::default()
    }
}

/// Split a Flotsync key id into member identity and protocol key role.
fn parse_key_id(kid: &str) -> Result<(MemberIdentity, KeyRole)> {
    let (member, role) = if let Some(member) = kid.strip_suffix(KEY_ID_SIGNING_SUFFIX) {
        (member, KeyRole::Signing)
    } else if let Some(member) = kid.strip_suffix(KEY_ID_ENCRYPTION_SUFFIX) {
        (member, KeyRole::Encryption)
    } else {
        return Err(SecurityError::InvalidKeyId {
            kid: kid.to_owned(),
        });
    };
    let member_id = MemberIdentity::from_str(member).context(InvalidMemberIdentitySnafu {
        kid: kid.to_owned(),
        member: member.to_owned(),
    })?;
    Ok((member_id, role))
}

/// Extract OKP key material and classify it by supported curve.
fn okp_key_material(kid: &str, key: JwkKey) -> Result<(Okp, KeyRole)> {
    match key {
        JwkKey::Okp(okp) => {
            let role = match okp.crv {
                OkpCurves::Ed25519 => KeyRole::Signing,
                OkpCurves::X25519 => KeyRole::Encryption,
                _ => {
                    return Err(SecurityError::UnsupportedKeyMaterial {
                        kid: kid.to_owned(),
                    });
                }
            };
            Ok((okp, role))
        }
        _ => Err(SecurityError::UnsupportedKeyMaterial {
            kid: kid.to_owned(),
        }),
    }
}

/// Validate optional JOSE metadata against Flotsync's key role and scope.
fn validate_key_parameters(
    kid: &str,
    parameters: &Parameters,
    role: KeyRole,
    key_scope: KeyScope,
) -> Result<()> {
    match (role, parameters.cls) {
        (KeyRole::Signing, Some(Class::Encryption))
        | (KeyRole::Encryption, Some(Class::Signing)) => {
            return Err(SecurityError::KeyRoleMismatch {
                kid: kid.to_owned(),
                expected: role,
                actual: match role {
                    KeyRole::Signing => KeyRole::Encryption,
                    KeyRole::Encryption => KeyRole::Signing,
                },
            });
        }
        _ => {}
    }

    if let Some(ops) = &parameters.ops
        && ops
            .iter()
            .any(|op| !operation_allowed(role, key_scope, *op))
    {
        return Err(SecurityError::InvalidKeyOperation {
            kid: kid.to_owned(),
            role,
        });
    }

    Ok(())
}

/// Return whether a JOSE `key_ops` value fits the expected key role and scope.
fn operation_allowed(role: KeyRole, key_scope: KeyScope, operation: Operations) -> bool {
    match (role, key_scope) {
        (KeyRole::Signing, KeyScope::Public) => matches!(operation, Operations::Verify),
        (KeyRole::Signing, KeyScope::Private) => matches!(operation, Operations::Sign),
        (KeyRole::Encryption, KeyScope::Public) => matches!(operation, Operations::Encrypt),
        (KeyRole::Encryption, KeyScope::Private) => matches!(operation, Operations::Decrypt),
    }
}

/// Decode fixed-width JWK key bytes and report the offending field on mismatch.
fn fixed_key_bytes<const N: usize>(
    kid: &str,
    field: &'static str,
    bytes: &[u8],
) -> Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| SecurityError::InvalidKeyLength {
            kid: kid.to_owned(),
            field,
            expected: N,
            actual: bytes.len(),
        })
}

/// Whether parsed or generated key material includes private fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyScope {
    Public,
    Private,
}

impl KeyScope {
    const fn is_private(self) -> bool {
        matches!(self, Self::Private)
    }
}

/// Parsed public key bytes with their source key id for diagnostics.
#[derive(Debug)]
struct ParsedKey<T> {
    /// Source key id used in diagnostics for this parsed key.
    kid: String,
    /// Parsed fixed-width key bytes.
    bytes: T,
}

/// Accumulator for the member keys extracted from one JWKS document.
#[derive(Default)]
struct ParsedMemberKeys {
    /// Member identity shared by every key in the JWKS document.
    member_id: Option<MemberIdentity>,
    /// Parsed public Ed25519 signing key.
    signing_public: Option<ParsedKey<[u8; ED25519_KEY_LENGTH]>>,
    /// Parsed private Ed25519 signing seed, zeroised when dropped.
    signing_private: Option<Zeroizing<[u8; ED25519_KEY_LENGTH]>>,
    /// Parsed public X25519 HPKE key.
    hpke_public: Option<ParsedKey<[u8; X25519_KEY_LENGTH]>>,
    /// Parsed private X25519 HPKE key, zeroised when dropped.
    hpke_private: Option<Zeroizing<[u8; X25519_KEY_LENGTH]>>,
}

impl fmt::Debug for ParsedMemberKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParsedMemberKeys")
            .field("member_id", &self.member_id)
            .field("signing_public", &self.signing_public)
            .field(
                "signing_private",
                &self.signing_private.as_ref().map(|_| "<redacted>"),
            )
            .field("hpke_public", &self.hpke_public)
            .field(
                "hpke_private",
                &self.hpke_private.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl ParsedMemberKeys {
    /// Parse, classify, and validate all keys in a JWKS document.
    fn from_jwks(input: &str, expected_member: Option<&MemberIdentity>) -> Result<Self> {
        let jwks: JwkSet = serde_json::from_str(input).context(ParseJwksSnafu)?;
        let mut parsed = Self::default();
        for jwk in jwks.keys {
            parsed.add_jwk(jwk)?;
        }
        let Some(member_id) = parsed.member_id.as_ref() else {
            return Ok(parsed);
        };
        if let Some(expected) = expected_member
            && expected != member_id
        {
            return Err(SecurityError::UnexpectedMember {
                expected: expected.clone(),
                found: member_id.clone(),
            });
        }
        Ok(parsed)
    }

    /// Add one role-classified JWK to the accumulator.
    fn add_jwk(&mut self, jwk: Jwk) -> Result<()> {
        let kid = jwk.prm.kid.clone().ok_or(SecurityError::MissingKeyId)?;
        let (member_id, expected_role) = parse_key_id(&kid)?;
        let (okp, actual_role) = okp_key_material(&kid, jwk.key)?;
        if expected_role != actual_role {
            return Err(SecurityError::KeyRoleMismatch {
                kid,
                expected: expected_role,
                actual: actual_role,
            });
        }
        let key_scope = if okp.d.is_some() {
            KeyScope::Private
        } else {
            KeyScope::Public
        };
        validate_key_parameters(&kid, &jwk.prm, actual_role, key_scope)?;
        self.ensure_member(&member_id)?;
        match actual_role {
            KeyRole::Signing => self.add_signing_key(kid, member_id, &okp),
            KeyRole::Encryption => self.add_encryption_key(kid, member_id, &okp),
        }
    }

    /// Ensure all keys in the document belong to the same member.
    fn ensure_member(&mut self, member_id: &MemberIdentity) -> Result<()> {
        match &self.member_id {
            Some(existing) if existing != member_id => Err(SecurityError::MixedMembers {
                first: existing.clone(),
                second: member_id.clone(),
            }),
            Some(_) => Ok(()),
            None => {
                self.member_id = Some(member_id.clone());
                Ok(())
            }
        }
    }

    /// Store the Ed25519 public key and optional private key.
    fn add_signing_key(&mut self, kid: String, member: MemberIdentity, okp: &Okp) -> Result<()> {
        if self.signing_public.is_some() {
            return Err(SecurityError::DuplicateKey {
                role: KeyRole::Signing,
                member,
            });
        }
        let public_key = fixed_key_bytes::<ED25519_KEY_LENGTH>(&kid, "x", okp.x.as_ref())?;
        let private_key = okp
            .d
            .as_ref()
            .map(|d| {
                fixed_key_bytes::<ED25519_KEY_LENGTH>(&kid, "d", d.as_ref()).map(Zeroizing::new)
            })
            .transpose()?;
        self.signing_public = Some(ParsedKey {
            kid,
            bytes: public_key,
        });
        self.signing_private = private_key;
        Ok(())
    }

    /// Store the X25519 public key and optional private key.
    fn add_encryption_key(&mut self, kid: String, member: MemberIdentity, okp: &Okp) -> Result<()> {
        if self.hpke_public.is_some() {
            return Err(SecurityError::DuplicateKey {
                role: KeyRole::Encryption,
                member,
            });
        }
        let public_key = fixed_key_bytes::<X25519_KEY_LENGTH>(&kid, "x", okp.x.as_ref())?;
        let private_key = okp
            .d
            .as_ref()
            .map(|d| {
                fixed_key_bytes::<X25519_KEY_LENGTH>(&kid, "d", d.as_ref()).map(Zeroizing::new)
            })
            .transpose()?;
        self.hpke_public = Some(ParsedKey {
            kid,
            bytes: public_key,
        });
        self.hpke_private = private_key;
        Ok(())
    }

    /// Convert a complete local-private JWKS parse result into runtime keys.
    fn into_local_keys(mut self) -> Result<LocalMemberKeys> {
        let member_id = self.take_member_for_complete_key_set()?;
        let signing_public = self.take_required_signing_public(&member_id)?;
        let hpke_public = self.take_required_hpke_public(&member_id)?;
        let signing_private =
            self.signing_private
                .take()
                .ok_or_else(|| SecurityError::MissingPrivateKey {
                    kid: signing_public.kid.clone(),
                })?;
        let hpke_private =
            self.hpke_private
                .take()
                .ok_or_else(|| SecurityError::MissingPrivateKey {
                    kid: hpke_public.kid.clone(),
                })?;

        let signing_key = SigningKey::from_bytes(&signing_private);
        let verifying_key = signing_key.verifying_key();
        if verifying_key.to_bytes() != signing_public.bytes {
            return Err(SecurityError::KeyPairMismatch {
                kid: signing_public.kid,
            });
        }
        let hpke_private_key =
            HpkePrivateKey::from_bytes(&hpke_private[..]).context(InvalidHpkeKeySnafu {
                kid: hpke_public.kid.clone(),
            })?;
        let hpke_public_key = HpkeKem::sk_to_pk(&hpke_private_key);
        if hpke_public_key.to_bytes().as_slice() != hpke_public.bytes {
            return Err(SecurityError::KeyPairMismatch {
                kid: hpke_public.kid,
            });
        }
        Ok(LocalMemberKeys {
            public_keys: PublicMemberKeys {
                member_id,
                signing_key: verifying_key,
                hpke_public_key,
            },
            signing_key,
            hpke_private_key,
        })
    }

    /// Convert a complete public JWKS parse result into trusted peer keys.
    fn into_public_keys(mut self) -> Result<PublicMemberKeys> {
        let member_id = self.take_member_for_complete_key_set()?;
        let signing_public = self.take_required_signing_public(&member_id)?;
        let hpke_public = self.take_required_hpke_public(&member_id)?;
        if self.signing_private.is_some() {
            return Err(SecurityError::UnexpectedPrivateKey {
                kid: signing_public.kid,
            });
        }
        if self.hpke_private.is_some() {
            return Err(SecurityError::UnexpectedPrivateKey {
                kid: hpke_public.kid,
            });
        }
        let signing_key = VerifyingKey::from_bytes(&signing_public.bytes).context(
            InvalidEd25519PublicKeySnafu {
                kid: signing_public.kid,
            },
        )?;
        let hpke_public_key =
            HpkePublicKey::from_bytes(&hpke_public.bytes).context(InvalidHpkeKeySnafu {
                kid: hpke_public.kid,
            })?;
        Ok(PublicMemberKeys {
            member_id,
            signing_key,
            hpke_public_key,
        })
    }

    /// Return the member id once a JWKS document has at least one recognised key.
    fn take_member_for_complete_key_set(&mut self) -> Result<MemberIdentity> {
        self.member_id.take().ok_or(SecurityError::EmptyKeySet)
    }

    /// Return the required signing public key or report the missing member key.
    fn take_required_signing_public(
        &mut self,
        member_id: &MemberIdentity,
    ) -> Result<ParsedKey<[u8; ED25519_KEY_LENGTH]>> {
        self.signing_public
            .take()
            .ok_or_else(|| SecurityError::MissingKey {
                role: KeyRole::Signing,
                member: member_id.clone(),
            })
    }

    /// Return the required HPKE public key or report the missing member key.
    fn take_required_hpke_public(
        &mut self,
        member_id: &MemberIdentity,
    ) -> Result<ParsedKey<[u8; X25519_KEY_LENGTH]>> {
        self.hpke_public
            .take()
            .ok_or_else(|| SecurityError::MissingKey {
                role: KeyRole::Encryption,
                member: member_id.clone(),
            })
    }
}

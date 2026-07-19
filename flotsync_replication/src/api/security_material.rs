//! Replication security material and trust API types.

use super::*;

const STORE_SECRET_CRYPTO_NONCE_LENGTH_V1: usize = 24;

/// Cryptographic setup version for one encrypted store secret.
///
/// Store implementations persist and compare the value opaquely. The
/// encryption boundary owns the mapping from version numbers to concrete
/// cryptographic setups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StoreSecretCryptoVersion(u16);

impl StoreSecretCryptoVersion {
    /// Build a store-secret crypto version from its wire/storage value.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Return the integer value stored in backend metadata columns.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

/// One already-encrypted secret cell stored by replication storage.
///
/// `key_id` identifies the device-local database secret used by the caller,
/// `crypto_version` identifies the encryption setup, `nonce` is opaque nonce
/// material for this cell, and `ciphertext` contains the encrypted payload
/// including the AEAD authentication tag.
#[derive(Clone, PartialEq, Eq)]
pub struct EncryptedStoreSecret {
    /// Cryptographic setup version for this encrypted cell.
    pub crypto_version: StoreSecretCryptoVersion,
    /// Generated id of the device-local database secret.
    pub key_id: StoreSecretKeyId,
    /// Opaque nonce bytes supplied by the encryption boundary.
    pub nonce: Box<[u8]>,
    /// Encrypted bytes including the AEAD authentication tag.
    pub ciphertext: Box<[u8]>,
}

impl fmt::Debug for EncryptedStoreSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedStoreSecret")
            .field("crypto_version", &self.crypto_version)
            .field("key_id", &self.key_id)
            .field("nonce_len", &self.nonce.len())
            .field("ciphertext_len", &self.ciphertext.len())
            .finish()
    }
}

impl EncryptedStoreSecret {
    /// Convert security-crate sealed bytes into the store's opaque encrypted-cell record.
    #[must_use]
    pub(crate) fn from_store_secret_ciphertext(
        key_id: StoreSecretKeyId,
        sealed: flotsync_security::StoreSecretCiphertext,
    ) -> Self {
        Self {
            crypto_version: STORE_SECRET_CRYPTO_V1,
            key_id,
            nonce: sealed.nonce.into(),
            ciphertext: sealed.ciphertext.into_boxed_slice(),
        }
    }
}

/// Build placeholder group-security material for the current security-storage slice.
///
/// This is a temporary bridge for `flotsync-sec.10`: group records need
/// encrypted security columns before replicated checklist setup provisions real
/// encrypted values from configuration. The returned bytes are not decryptable
/// production security material and this helper must be removed with that task.
#[doc(hidden)]
#[must_use]
pub fn current_slice_placeholder_group_security_material(
    group_id: GroupId,
) -> EncryptedGroupSecurityMaterial {
    current_slice_placeholder_group_security_material_with_key_id(
        group_id,
        StoreSecretKeyId::placeholder_for_current_slice(),
    )
}

/// Build placeholder group-security material with a caller-selected key id.
///
/// This exists only so temporary static group setup can satisfy loader metadata
/// validation until real group-secret provisioning lands.
#[doc(hidden)]
#[must_use]
pub fn current_slice_placeholder_group_security_material_with_key_id(
    group_id: GroupId,
    key_id: StoreSecretKeyId,
) -> EncryptedGroupSecurityMaterial {
    let seed = group_id.0.as_u128().to_le_bytes()[0];
    EncryptedGroupSecurityMaterial {
        encrypted_group_secret: EncryptedStoreSecret {
            crypto_version: STORE_SECRET_CRYPTO_V1,
            key_id,
            nonce: vec![seed; STORE_SECRET_CRYPTO_NONCE_LENGTH_V1].into_boxed_slice(),
            ciphertext: vec![seed, seed.wrapping_add(1), seed.wrapping_add(2)].into_boxed_slice(),
        },
    }
}

/// Encrypted group-security material persisted with one replication group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedGroupSecurityMaterial {
    /// Encrypted group secret payload for the replication group.
    pub encrypted_group_secret: EncryptedStoreSecret,
}

/// Encrypted local member private key material.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedLocalMemberPrivateKeys {
    /// Encrypted private key bundle for the local member.
    pub secret: EncryptedStoreSecret,
}

/// One stored encrypted local-private key bundle for a member identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalMemberPrivateKeysRecord {
    /// Member identity these private keys belong to.
    pub member_id: MemberIdentity,
    /// Already-encrypted local private key material.
    pub private_keys: EncryptedLocalMemberPrivateKeys,
}

/// Authority scope requested by one permission check.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AuthorityScope {
    /// Use member key material for active replication runtime traffic.
    ReplicationRuntime,
    /// Accept a bootstrap message from this member key for local group activation.
    BootstrapActivation,
    /// Publish a discovered route candidate for this member identity.
    MemberRoutePublication,
}

impl AuthorityScope {
    /// Authority scopes included in public key bundle assessment reports.
    pub const VALUES: [Self; 3] = [
        Self::ReplicationRuntime,
        Self::BootstrapActivation,
        Self::MemberRoutePublication,
    ];
}

impl fmt::Display for AuthorityScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplicationRuntime => f.write_str("replication runtime"),
            Self::BootstrapActivation => f.write_str("bootstrap activation"),
            Self::MemberRoutePublication => f.write_str("member route publication"),
        }
    }
}

/// Runtime policy that maps stored trust evidence to permission decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustPolicy {
    /// Evidence requirement for active replication runtime traffic.
    pub replication_runtime: MemberKeyTrustRequirement,
    /// Evidence requirement for accepting a bootstrap activation from a sender key.
    pub bootstrap_activation: MemberKeyTrustRequirement,
    /// Evidence requirement for publishing discovered routes for a member identity.
    pub member_route_publication: MemberKeyTrustRequirement,
}

impl Default for TrustPolicy {
    fn default() -> Self {
        Self {
            replication_runtime: MemberKeyTrustRequirement::LocalExplicitTrust,
            bootstrap_activation: MemberKeyTrustRequirement::LocalExplicitTrust,
            member_route_publication: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
        }
    }
}

/// Evidence requirement for one member-key authority scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemberKeyTrustRequirement {
    /// Require locally recorded explicit trust for the exact member key.
    LocalExplicitTrust,
    /// Require only that public key material is stored for the exact member key.
    StoredPublicKeyMaterial,
    /// Deny every permission request for this authority scope.
    DenyAll,
}

/// Result of evaluating whether one member key has a requested permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PermissionDecision {
    /// The key has the requested permission.
    Permit,
    /// The key does not have the requested permission.
    Deny(PermissionDenialReason),
}

impl PermissionDecision {
    /// Convert this permission decision into a plain result.
    ///
    /// Use this when the caller already knows the requested member key and authority scope and
    /// only needs to branch on permit versus denial.
    ///
    /// # Errors
    ///
    /// Returns the denial reason when this decision is [`Self::Deny`].
    pub const fn ok(self) -> Result<(), PermissionDenialReason> {
        match self {
            Self::Permit => Ok(()),
            Self::Deny(reason) => Err(reason),
        }
    }
}

impl fmt::Display for PermissionDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Permit => f.write_str("permit"),
            Self::Deny(reason) => write!(f, "deny ({reason})"),
        }
    }
}

/// Reason one member key permission request was denied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PermissionDenialReason {
    /// No stored public key material exists for the exact member key.
    MissingKeyMaterial,
    /// Stored evidence does not satisfy the policy for the requested authority.
    MissingTrustEvidence,
    /// The fingerprint is globally blocked.
    FingerprintBlocked,
    /// The active policy denies the requested authority scope.
    PolicyDenied,
}

impl fmt::Display for PermissionDenialReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingKeyMaterial => f.write_str("missing key material"),
            Self::MissingTrustEvidence => f.write_str("missing trust evidence"),
            Self::FingerprintBlocked => f.write_str("fingerprint blocked"),
            Self::PolicyDenied => f.write_str("policy denied"),
        }
    }
}

/// Identity of one exact member-key binding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemberKeyId {
    /// Member identity bound to the key material.
    pub member_id: MemberIdentity,
    /// Fingerprint derived from the bound public key material.
    pub fingerprint: KeyFingerprint,
}

/// Public key material observed for one exact member-key binding.
#[derive(Clone, PartialEq, Eq)]
pub struct MemberPublicKeysRecord {
    /// Exact member-key binding for this material.
    pub key_id: MemberKeyId,
    /// Opaque signing public key bytes.
    pub signing_public_key: Box<[u8]>,
    /// Opaque encryption public key bytes.
    pub encryption_public_key: Box<[u8]>,
}

impl MemberPublicKeysRecord {
    /// Build a store record from typed public member keys.
    #[must_use]
    pub fn from_public_keys(public_keys: &PublicMemberKeys) -> Self {
        Self {
            key_id: MemberKeyId {
                member_id: public_keys.member_id().clone(),
                fingerprint: public_keys.fingerprint(),
            },
            signing_public_key: public_keys.signing_key_bytes().into(),
            encryption_public_key: public_keys.encryption_key_bytes().into(),
        }
    }
}

impl fmt::Debug for MemberPublicKeysRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alternate = f.alternate();
        let mut debug = f.debug_struct("MemberPublicKeysRecord");
        debug.field("key_id", &self.key_id);
        if alternate {
            let signing_public_key = URL_SAFE_NO_PAD.encode(&self.signing_public_key);
            let encryption_public_key = URL_SAFE_NO_PAD.encode(&self.encryption_public_key);
            debug
                .field("signing_public_key", &signing_public_key)
                .field("encryption_public_key", &encryption_public_key)
                .finish()
        } else {
            debug
                .field("signing_public_key_len", &self.signing_public_key.len())
                .field(
                    "encryption_public_key_len",
                    &self.encryption_public_key.len(),
                )
                .finish()
        }
    }
}

/// Trust evidence recorded for one exact member-key binding.
#[derive(Debug, EnumSetType, Hash)]
pub enum MemberKeyTrustEvidenceKind {
    /// This store locally trusts the exact member-key binding.
    LocalExplicitTrust,
}

impl MemberKeyTrustEvidenceKind {
    /// Store representation for this evidence kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalExplicitTrust => "local_explicit_trust",
        }
    }
}

impl FromStr for MemberKeyTrustEvidenceKind {
    type Err = MemberKeyTrustEvidenceKindParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "local_explicit_trust" => Ok(Self::LocalExplicitTrust),
            _ => Err(MemberKeyTrustEvidenceKindParseError {
                value: input.to_owned(),
            }),
        }
    }
}

/// Error raised while parsing a stored member-key trust evidence kind.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
#[snafu(display("member-key trust evidence kind '{value}' is invalid"))]
pub struct MemberKeyTrustEvidenceKindParseError {
    /// Raw evidence-kind value read from storage.
    value: String,
}

/// One stored evidence record for a member-key binding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemberKeyTrustEvidenceRecord {
    /// Exact member-key binding the evidence describes.
    pub key_id: MemberKeyId,
    /// Kind of evidence recorded for that binding.
    pub evidence_kind: MemberKeyTrustEvidenceKind,
}

/// Evidence set loaded for one exact member-key binding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MemberKeyTrustEvidenceSet {
    /// In-memory finite-domain set of evidence kinds observed for one exact binding.
    evidence_kinds: EnumSet<MemberKeyTrustEvidenceKind>,
}

impl MemberKeyTrustEvidenceSet {
    /// Build an empty evidence set.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            evidence_kinds: EnumSet::new(),
        }
    }

    /// Add one evidence kind to the set.
    pub fn insert(&mut self, evidence_kind: MemberKeyTrustEvidenceKind) {
        self.evidence_kinds.insert(evidence_kind);
    }

    /// Return whether the set contains the requested evidence kind.
    #[must_use]
    pub fn contains(self, evidence_kind: MemberKeyTrustEvidenceKind) -> bool {
        self.evidence_kinds.contains(evidence_kind)
    }
}

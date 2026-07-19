use crate::{
    api::{
        AuthorityScope,
        BoxError,
        EncryptedGroupSecurityMaterial,
        EncryptedStoreSecret,
        GroupMemberKeys,
        LocalMemberPrivateKeysRecord,
        MemberKeyId,
        MemberPublicKeysRecord,
        PermissionDenialReason,
        StoreError,
        StoreSecretKeyId,
        security::{
            AssessPublicKeyBundleRequest,
            PublicKeyBundleReport,
            RecordPublicKeyBundleFeedbackRequest,
        },
    },
    codecs::messages::{
        BootstrapMemberKeyMessage,
        GroupSetupMessage,
        RuntimeMessageError,
        validate_bootstrap_public_key_bundle_matches_fingerprint,
    },
    delivery::{
        group_broadcast::GroupMessageHeader,
        reliable_delivery::{RecipientAckHeader, ReliableMessageHeader},
        shared::{DetachedSignature, MessageId, ReliableMessageScope, SignatureScheme},
    },
    security_store::{MemberPublicKeyLoadPolicy, SecurityStore, SecurityStoreError},
};
use bytes::Bytes;
use flotsync_core::{GroupId, MemberIdentity, member::TrieMap, membership::GroupMembers};
use flotsync_routes::route_establishment::DiscoveryKeyMaterialStatus;
use flotsync_security::{
    FrameSignature,
    GroupKey,
    GroupMessageContext,
    HpkeEnvelopeScope,
    KeyFingerprint,
    LocalMemberKeys,
    PublicKeyBundle,
    PublicMemberKeys,
    ReliablePayloadContext,
    SIGNATURE_LENGTH,
    STORE_SECRET_CRYPTO_VERSION_V1,
    STORE_SECRET_NONCE_LENGTH,
    SealedHPKEPayload,
    SealedPSKPayload,
    SignedFrameParts,
    StoreSecretCiphertext,
    StoreSecretContext,
    StoreSecretCryptoVersion,
    StoreSecretKey,
    local_member_keys_from_private_bundle,
    open_group_payload,
    open_reliable_payload,
    open_store_secret,
    open_stored_group_key,
    seal_group_payload,
    seal_reliable_payload_with_os_rng,
    seal_store_secret,
    sign_frame,
    verify_discovery_payload_signature,
    verify_frame_signature,
};
use snafu::{Location, prelude::*};
use std::sync::Arc;
use uuid::Uuid;

mod group;
mod reliable;
mod store_material;
mod trust;

use self::store_material::open_local_private_keys;

/// Signature domain for sender-signed reliable runtime payloads.
pub(crate) const RELIABLE_RUNTIME_MESSAGE_FRAME_KIND: &str = "reliable-runtime-message";
/// Signature domain for recipient-signed semantic acknowledgements.
pub(crate) const RELIABLE_RECIPIENT_ACK_FRAME_KIND: &str = "reliable-recipient-ack";
/// Signature domain for sender-signed group-broadcast runtime payloads.
pub(crate) const GROUP_BROADCAST_RUNTIME_MESSAGE_FRAME_KIND: &str =
    "group-broadcast-runtime-message";

pub(crate) const LOGICAL_GROUP_TABLE: &str = "replication_group";
pub(crate) const LOGICAL_GROUP_SECRET_COLUMN: &str = "group_secret";
pub(crate) const LOGICAL_LOCAL_MEMBER_TABLE: &str = "local_member";
pub(crate) const LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN: &str = "private_keys";

/// Security setup and delivery-envelope errors raised while wiring store records into crypto helpers.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum DeliverySecurityError {
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Local private keys for member {member_id} are not provisioned."))]
    MissingLocalPrivateKeys { member_id: MemberIdentity },
    #[snafu(display("Security material for group {group_id} is not provisioned."))]
    MissingGroupSecurity { group_id: GroupId },
    #[snafu(display(
        "Inbound setup key for group {group_id} does not match the stored group key."
    ))]
    GroupSetupKeyMismatch { group_id: GroupId },
    #[snafu(display("Reliable delivery cannot send a message from {member_id} to itself."))]
    ReliableSelfMessage { member_id: MemberIdentity },
    #[snafu(display(
        "Reliable delivery sender {sender} did not match local member {local_member}."
    ))]
    UnexpectedReliableSender {
        sender: MemberIdentity,
        local_member: MemberIdentity,
    },
    #[snafu(display(
        "Reliable delivery recipient {recipient} did not match local member {local_member}."
    ))]
    UnexpectedReliableRecipient {
        recipient: MemberIdentity,
        local_member: MemberIdentity,
    },
    #[snafu(display(
        "Reliable recipient ack original sender {original_sender} did not match local member {local_member}."
    ))]
    UnexpectedRecipientAckOriginalSender {
        original_sender: MemberIdentity,
        local_member: MemberIdentity,
    },
    #[snafu(display(
        "Reliable recipient ack recipient {recipient} did not match local member {local_member}."
    ))]
    UnexpectedRecipientAckRecipient {
        recipient: MemberIdentity,
        local_member: MemberIdentity,
    },
    #[snafu(display(
        "Reliable recipient ack signature for message {message_id} recipient {recipient} had invalid length {actual}; expected {expected}."
    ))]
    InvalidRecipientAckSignatureLength {
        message_id: MessageId,
        recipient: MemberIdentity,
        expected: usize,
        actual: usize,
    },
    #[snafu(display("Group broadcast sender {sender} did not match local member {local_member}."))]
    UnexpectedGroupSender {
        sender: MemberIdentity,
        local_member: MemberIdentity,
    },
    #[snafu(display("Group broadcast sender {sender} is not a member of group {group_id}."))]
    GroupSenderNotInGroup {
        group_id: GroupId,
        sender: MemberIdentity,
    },
    /// The stored group record names a different sender key than the verified payload used.
    #[snafu(display(
        "Group broadcast sender {sender} key fingerprint was {actual}, expected {expected}."
    ))]
    GroupSenderKeyFingerprintMismatch {
        sender: MemberIdentity,
        expected: KeyFingerprint,
        actual: KeyFingerprint,
    },
    #[snafu(display("Bootstrap payload is missing a key reference for member {member_id}."))]
    MissingBootstrapMemberKey { member_id: MemberIdentity },
    #[snafu(display(
        "Bootstrap key fingerprint for member {member_id} was {actual}, expected {expected}."
    ))]
    BootstrapKeyFingerprintMismatch {
        member_id: MemberIdentity,
        expected: KeyFingerprint,
        actual: KeyFingerprint,
    },
    #[snafu(display(
        "Member {member_id} key fingerprint {fingerprint} lacks {authority_scope} permission: {reason:?}."
    ))]
    MemberKeyPermissionDenied {
        member_id: MemberIdentity,
        fingerprint: KeyFingerprint,
        authority_scope: AuthorityScope,
        reason: PermissionDenialReason,
    },
    #[snafu(display("Bootstrap key references included non-member {member_id}."))]
    UnexpectedBootstrapMemberKey { member_id: MemberIdentity },
    #[snafu(display(
        "Bootstrap inline public key bundle for member {member_id} failed validation: {source}"
    ))]
    InvalidBootstrapPublicKeyBundle {
        member_id: MemberIdentity,
        source: RuntimeMessageError,
    },
    #[snafu(display("Encrypted secret used unsupported crypto version {version}."))]
    UnsupportedStoreSecretVersion { version: u16 },
    #[snafu(display(
        "Encrypted secret nonce had invalid length {actual}; expected {STORE_SECRET_NONCE_LENGTH}."
    ))]
    InvalidStoreSecretNonce { actual: usize },
    #[snafu(display(
        "Encrypted group secret for group {group_id} used key id {actual}; expected {expected}."
    ))]
    GroupSecretKeyIdMismatch {
        group_id: GroupId,
        expected: StoreSecretKeyId,
        actual: StoreSecretKeyId,
    },
    #[snafu(display("Local private keys were invalid: {source}"))]
    InvalidLocalPrivateKeys { source: BoxError },
    #[snafu(display("Failed to generate group key: {source}"))]
    GenerateGroupKey { source: BoxError },
    #[snafu(display("Failed to seal reliable payload for recipient {recipient}: {source}"))]
    SealReliablePayload {
        recipient: MemberIdentity,
        source: BoxError,
    },
    #[snafu(display("Failed to open reliable payload: {source}"))]
    OpenReliablePayload { source: BoxError },
    #[snafu(display("Failed to sign recipient ack for recipient {recipient}: {source}"))]
    SignRecipientAck {
        recipient: MemberIdentity,
        source: BoxError,
    },
    #[snafu(display("Failed to verify recipient ack from recipient {recipient}: {source}"))]
    VerifyRecipientAck {
        recipient: MemberIdentity,
        source: BoxError,
    },
    #[snafu(display(
        "Failed to verify discovery claim for member {member_id} fingerprint {fingerprint}: {source}"
    ))]
    VerifyDiscoveryClaim {
        member_id: MemberIdentity,
        fingerprint: KeyFingerprint,
        source: BoxError,
    },
    #[snafu(display("Public key permission lookup failed: {source}"))]
    SecurityStore { source: SecurityStoreError },
    #[snafu(display("Failed to open encrypted group secret for group {group_id}: {source}"))]
    OpenGroupSecret { group_id: GroupId, source: BoxError },
    #[snafu(display("Failed to seal group payload for group {group_id}: {source}"))]
    SealGroupPayload { group_id: GroupId, source: BoxError },
    #[snafu(display("Failed to open group payload for group {group_id}: {source}"))]
    OpenGroupPayload { group_id: GroupId, source: BoxError },
    #[snafu(display("Failed to open encrypted local private keys: {source}"))]
    OpenLocalPrivateKeys { source: BoxError },
    #[snafu(display("Failed to seal group secret for storage: {source}"))]
    SealGroupSecret { source: BoxError },
}

impl DeliverySecurityError {
    pub(crate) const fn is_retryable(&self) -> bool {
        #[allow(
            clippy::match_like_matches_macro,
            clippy::match_same_arms,
            reason = "Each security error needs an explicit retryability decision."
        )]
        match self {
            // Store access may fail due to transient I/O or lock contention.
            Self::StoreAccess { .. } => true,
            // Missing local provisioning is configuration state, not a retry condition.
            Self::MissingLocalPrivateKeys { .. } => false,
            // Missing group-security state requires provisioning or data repair.
            Self::MissingGroupSecurity { .. } => false,
            // A replayed setup key mismatch is a permanent conflict for that payload.
            Self::GroupSetupKeyMismatch { .. } => false,
            // Reliable self-send is a caller bug.
            Self::ReliableSelfMessage { .. } => false,
            // Sender/recipient mismatches are local routing or authenticity violations.
            Self::UnexpectedReliableSender { .. } | Self::UnexpectedReliableRecipient { .. } => {
                false
            }
            // Recipient-ack sender/recipient mismatches are authenticity violations.
            Self::UnexpectedRecipientAckOriginalSender { .. }
            | Self::UnexpectedRecipientAckRecipient { .. }
            | Self::InvalidRecipientAckSignatureLength { .. } => false,
            // Group sender mismatches are local routing or membership violations.
            Self::UnexpectedGroupSender { .. }
            | Self::GroupSenderNotInGroup { .. }
            | Self::GroupSenderKeyFingerprintMismatch { .. } => false,
            // Bootstrap payload membership and key mismatches are permanent for this payload.
            Self::MissingBootstrapMemberKey { .. }
            | Self::BootstrapKeyFingerprintMismatch { .. }
            | Self::UnexpectedBootstrapMemberKey { .. }
            | Self::InvalidBootstrapPublicKeyBundle { .. } => false,
            // Member-key permission denials need policy, trust, or stored key state to change.
            Self::MemberKeyPermissionDenied { .. } => false,
            // Stored secret encoding/version problems need data repair or a code change.
            Self::UnsupportedStoreSecretVersion { .. } | Self::InvalidStoreSecretNonce { .. } => {
                false
            }
            // Group-secret key id mismatches require loading with the matching local key id.
            Self::GroupSecretKeyIdMismatch { .. } => false,
            // Invalid local private-key records need reprovisioning.
            Self::InvalidLocalPrivateKeys { .. } => false,
            // Group-key generation depends on OS randomness and may recover on retry.
            Self::GenerateGroupKey { .. } => true,
            // Reliable-payload seal/open failures are crypto or key-consistency failures.
            Self::SealReliablePayload { .. } | Self::OpenReliablePayload { .. } => false,
            // Recipient-ack sign/verify failures are crypto or key-consistency failures.
            Self::SignRecipientAck { .. }
            | Self::VerifyRecipientAck { .. }
            | Self::VerifyDiscoveryClaim { .. }
            | Self::SecurityStore { .. } => false,
            // Group-secret and group-payload failures are crypto or key-consistency failures.
            Self::OpenGroupSecret { .. }
            | Self::SealGroupPayload { .. }
            | Self::OpenGroupPayload { .. } => false,
            // Local private-key opening failures are wrong-key/corrupt-record failures.
            Self::OpenLocalPrivateKeys { .. } => false,
            // Group-secret sealing also depends on OS randomness and may recover on retry.
            Self::SealGroupSecret { .. } => true,
        }
    }
}

/// Decrypted security state and store access shared by delivery-oriented security code.
#[derive(Clone)]
pub(crate) struct DeliverySecurity {
    security_store: SecurityStore,
    local_member: MemberIdentity,
    local_keys: Arc<LocalMemberKeys>,
    store_secret_key: Arc<StoreSecretKey>,
    store_secret_key_id: StoreSecretKeyId,
}

impl DeliverySecurity {
    pub(crate) async fn load(
        security_store: SecurityStore,
        local_member: &MemberIdentity,
        store_secret_key: Arc<StoreSecretKey>,
        store_secret_key_id: StoreSecretKeyId,
    ) -> Result<Self, DeliverySecurityError> {
        let mut transaction = security_store
            .replication_store()
            .begin_read_transaction()
            .await
            .context(StoreAccessSnafu)?;
        let record = transaction
            .load_local_member_private_keys(local_member)
            .await
            .context(StoreAccessSnafu)?
            .ok_or_else(|| DeliverySecurityError::MissingLocalPrivateKeys {
                member_id: local_member.clone(),
            })?;
        transaction.release().await.context(StoreAccessSnafu)?;
        let local_keys = open_local_private_keys(&record, &store_secret_key)?;
        Ok(Self {
            security_store,
            local_member: local_member.clone(),
            local_keys: Arc::new(local_keys),
            store_secret_key,
            store_secret_key_id,
        })
    }

    pub(crate) fn local_keys(&self) -> &LocalMemberKeys {
        &self.local_keys
    }

    /// Return the local member's shareable public key bundle.
    pub(crate) fn local_public_key_bundle(&self) -> PublicKeyBundle {
        self.local_keys.public_keys().public_key_bundle()
    }

    /// Return the local public key bundle fingerprint used for discovery signatures.
    pub(crate) fn local_discovery_key_fingerprint(&self) -> KeyFingerprint {
        self.local_keys.public_keys().fingerprint()
    }
}

impl From<ReliableMessageScope> for HpkeEnvelopeScope {
    fn from(scope: ReliableMessageScope) -> Self {
        match scope {
            ReliableMessageScope::DirectMessage => Self::DirectMessage,
            ReliableMessageScope::Group { group_id } => Self::Group { group_id },
        }
    }
}

/// Convert the transport-domain detached signature into the fixed-width
/// security signature expected by the crypto helper.
fn recipient_ack_frame_signature(
    header: &RecipientAckHeader,
    signature: &DetachedSignature,
) -> Result<FrameSignature, DeliverySecurityError> {
    match signature.scheme {
        SignatureScheme::Ed25519Ph => {}
    }
    let bytes = signature.bytes.as_ref().try_into().map_err(|_| {
        DeliverySecurityError::InvalidRecipientAckSignatureLength {
            message_id: header.message_id,
            recipient: header.recipient.clone(),
            expected: SIGNATURE_LENGTH,
            actual: signature.bytes.len(),
        }
    })?;
    Ok(FrameSignature::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SqliteReplicationStore,
        api::{MemberKeyTrustRequirement, ReplicationStore, TrustPolicy},
        codecs::messages::GroupSetupKey,
        test_support::{
            provision_test_security,
            provision_test_trusted_public_keys,
            test_public_member_keys,
            test_replication_security_secrets,
        },
    };
    use flotsync_core::member::Identifier;
    use flotsync_security::{GROUP_CIPHER_SUITE_CHACHA20_POLY1305, sign_discovery_payload};
    use std::{future::Future, sync::Arc, time::Duration};

    const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

    fn wait_for_delivery_security_future<F>(future: F) -> F::Output
    where
        F: Future,
    {
        flotsync_io::test_support::wait_for_future(
            TEST_WAIT_TIMEOUT,
            future,
            "timed out waiting for delivery-security future",
        )
    }

    fn application_id() -> Identifier {
        Identifier::from_array(["delivery-security", "application"])
    }

    fn alice_member() -> MemberIdentity {
        Identifier::from_array(["delivery-security", "alice"])
    }

    fn bob_member() -> MemberIdentity {
        Identifier::from_array(["delivery-security", "bob"])
    }

    fn charlie_member() -> MemberIdentity {
        Identifier::from_array(["delivery-security", "charlie"])
    }

    fn sqlite_store(local_member: MemberIdentity) -> Arc<SqliteReplicationStore> {
        Arc::new(
            wait_for_delivery_security_future(SqliteReplicationStore::in_memory(local_member))
                .expect("store should build"),
        )
    }

    fn provision_security(
        store: &dyn ReplicationStore,
        local_member: &MemberIdentity,
        trusted_members: Vec<MemberIdentity>,
    ) {
        wait_for_delivery_security_future(provision_test_security(
            application_id(),
            store,
            local_member,
            trusted_members,
        ))
        .expect("test security should provision");
    }

    fn load_delivery_security_with_policy(
        store: Arc<SqliteReplicationStore>,
        local_member: &MemberIdentity,
        trust_policy: TrustPolicy,
    ) -> DeliverySecurity {
        let store: Arc<dyn ReplicationStore> = store;
        let security_secrets = test_replication_security_secrets();
        wait_for_delivery_security_future(DeliverySecurity::load(
            SecurityStore::new(store, trust_policy),
            local_member,
            Arc::clone(security_secrets.store_secret_key()),
            *security_secrets.store_secret_key_id(),
        ))
        .expect("delivery security should load")
    }

    fn load_delivery_security(
        store: Arc<SqliteReplicationStore>,
        local_member: &MemberIdentity,
    ) -> DeliverySecurity {
        load_delivery_security_with_policy(store, local_member, TrustPolicy::default())
    }

    fn signing_security() -> DeliverySecurity {
        let alice = alice_member();
        let store = sqlite_store(alice.clone());
        provision_security(store.as_ref(), &alice, Vec::new());
        load_delivery_security(store, &alice)
    }

    fn test_group_setup(
        creator: &MemberIdentity,
        invited_member: &MemberIdentity,
        key_byte: u8,
    ) -> GroupSetupMessage {
        let members = vec![creator.clone(), invited_member.clone()];
        let mut member_keys = TrieMap::new();
        for member in &members {
            member_keys.insert(
                member.clone(),
                BootstrapMemberKeyMessage::from_public_keys(&test_public_member_keys(member)),
            );
        }
        GroupSetupMessage::new(
            members,
            member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            GroupSetupKey::from_group_key(GroupKey::from_bytes([key_byte; 32])),
        )
        .expect("test group setup should build")
    }

    #[test]
    fn existing_group_setup_security_accepts_same_plaintext_key_after_random_sealing() {
        let alice = alice_member();
        let bob = bob_member();
        let group_id = GroupId(Uuid::from_u128(70_001));
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, vec![alice.clone()]);
        let bob_security = load_delivery_security(bob_store, &bob);
        let setup = test_group_setup(&alice, &bob, 41);

        let stored = wait_for_delivery_security_future(
            bob_security.prepare_security_material_from_group_setup(group_id, &setup, &alice),
        )
        .expect("initial setup should seal");
        wait_for_delivery_security_future(
            bob_security.validate_existing_group_setup_security(group_id, &setup, &alice, &stored),
        )
        .expect("same plaintext setup key should match stored ciphertext");
    }

    #[test]
    fn existing_group_setup_security_rejects_different_plaintext_key() {
        let alice = alice_member();
        let bob = bob_member();
        let group_id = GroupId(Uuid::from_u128(70_002));
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, vec![alice.clone()]);
        let bob_security = load_delivery_security(bob_store, &bob);
        let original = test_group_setup(&alice, &bob, 42);
        let conflicting = test_group_setup(&alice, &bob, 43);

        let stored = wait_for_delivery_security_future(
            bob_security.prepare_security_material_from_group_setup(group_id, &original, &alice),
        )
        .expect("initial setup should seal");
        let error =
            wait_for_delivery_security_future(bob_security.validate_existing_group_setup_security(
                group_id,
                &conflicting,
                &alice,
                &stored,
            ))
            .expect_err("different plaintext setup key should conflict");

        assert!(matches!(
            error,
            DeliverySecurityError::GroupSetupKeyMismatch {
                group_id: actual_group_id
            } if actual_group_id == group_id
        ));
    }

    #[test]
    fn discovery_claim_verifies_with_exact_stored_key_material() {
        let alice = alice_member();
        let bob = bob_member();
        let alice_security = signing_security();
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, vec![alice.clone()]);
        let bob_security = load_delivery_security(bob_store, &bob);
        let payload = b"signed introduction claim";
        let signature = sign_discovery_payload(alice_security.local_keys(), payload)
            .expect("claim should sign");
        let alice_fingerprint = test_public_member_keys(&alice).fingerprint();

        wait_for_delivery_security_future(bob_security.verify_discovery_claim_payload(
            &alice,
            alice_fingerprint,
            payload,
            &signature,
        ))
        .expect("claim should verify with exact stored key material");
    }

    #[test]
    fn discovery_claim_rejects_unknown_fingerprint() {
        let alice = alice_member();
        let bob = bob_member();
        let alice_security = signing_security();
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, Vec::new());
        let bob_security = load_delivery_security(bob_store, &bob);
        let payload = b"signed introduction claim";
        let signature = sign_discovery_payload(alice_security.local_keys(), payload)
            .expect("claim should sign");
        let alice_fingerprint = test_public_member_keys(&alice).fingerprint();

        let error = wait_for_delivery_security_future(bob_security.verify_discovery_claim_payload(
            &alice,
            alice_fingerprint,
            payload,
            &signature,
        ))
        .expect_err("unknown fingerprint should not verify");

        assert!(matches!(
            error,
            DeliverySecurityError::SecurityStore {
                source: SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                    denial_reasons,
                    ..
                },
            } if denial_reasons.contains(&PermissionDenialReason::MissingKeyMaterial)
        ));
    }

    #[test]
    fn discovery_claim_rejects_rebound_fingerprint() {
        let alice = alice_member();
        let bob = bob_member();
        let charlie = charlie_member();
        let alice_security = signing_security();
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, Vec::new());
        wait_for_delivery_security_future(provision_test_trusted_public_keys(
            application_id(),
            bob_store.as_ref(),
            alice.clone(),
            &test_public_member_keys(&charlie),
        ))
        .expect("rebound public keys should provision");
        let bob_security = load_delivery_security(bob_store, &bob);
        let payload = b"signed introduction claim";
        let signature = sign_discovery_payload(alice_security.local_keys(), payload)
            .expect("claim should sign");
        let rebound_fingerprint = test_public_member_keys(&charlie).fingerprint();

        let error = wait_for_delivery_security_future(bob_security.verify_discovery_claim_payload(
            &alice,
            rebound_fingerprint,
            payload,
            &signature,
        ))
        .expect_err("signature should not verify with rebound key material");

        assert!(matches!(
            error,
            DeliverySecurityError::VerifyDiscoveryClaim {
                member_id,
                fingerprint,
                ..
            } if member_id == alice && fingerprint == rebound_fingerprint
        ));
    }

    #[test]
    fn discovery_claim_rejects_signed_payload_tampering() {
        let alice = alice_member();
        let bob = bob_member();
        let alice_security = signing_security();
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, vec![alice.clone()]);
        let bob_security = load_delivery_security(bob_store, &bob);
        let signed_payload = b"signed introduction claim";
        let tampered_payload = b"signed introduction claim with swapped selector";
        let signature = sign_discovery_payload(alice_security.local_keys(), signed_payload)
            .expect("claim should sign");
        let alice_fingerprint = test_public_member_keys(&alice).fingerprint();

        let error = wait_for_delivery_security_future(bob_security.verify_discovery_claim_payload(
            &alice,
            alice_fingerprint,
            tampered_payload,
            &signature,
        ))
        .expect_err("tampered payload should not verify");

        assert!(matches!(
            error,
            DeliverySecurityError::VerifyDiscoveryClaim {
                member_id,
                fingerprint,
                ..
            } if member_id == alice && fingerprint == alice_fingerprint
        ));
    }

    #[test]
    fn discovery_claim_rejects_blocked_fingerprint() {
        let alice = alice_member();
        let bob = bob_member();
        let alice_security = signing_security();
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, vec![alice.clone()]);
        let alice_fingerprint = test_public_member_keys(&alice).fingerprint();
        let mut transaction = wait_for_delivery_security_future(bob_store.begin_transaction())
            .expect("transaction starts");
        wait_for_delivery_security_future(
            transaction.ensure_blocked_key_fingerprint(alice_fingerprint),
        )
        .expect("blocked fingerprint stores");
        wait_for_delivery_security_future(transaction.commit()).expect("transaction commits");
        let bob_security = load_delivery_security(bob_store, &bob);
        let payload = b"signed introduction claim";
        let signature = sign_discovery_payload(alice_security.local_keys(), payload)
            .expect("claim should sign");

        let error = wait_for_delivery_security_future(bob_security.verify_discovery_claim_payload(
            &alice,
            alice_fingerprint,
            payload,
            &signature,
        ))
        .expect_err("blocked fingerprint should not verify");

        assert!(matches!(
            error,
            DeliverySecurityError::SecurityStore {
                source: SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                    denial_reasons,
                    ..
                },
            } if denial_reasons.contains(&PermissionDenialReason::FingerprintBlocked)
        ));
    }

    #[test]
    fn ensure_discovery_public_key_bundle_stores_candidate_without_runtime_trust() {
        let alice = alice_member();
        let bob = bob_member();
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, Vec::new());
        let bob_security = load_delivery_security(bob_store, &bob);
        let alice_bundle = test_public_member_keys(&alice).public_key_bundle();
        let alice_fingerprint = alice_bundle.fingerprint();

        wait_for_delivery_security_future(
            bob_security.ensure_discovery_public_key_bundle(&alice, alice_bundle),
        )
        .expect("candidate discovery key material should store");

        let status = wait_for_delivery_security_future(
            bob_security.discovery_key_material_status(&alice, alice_fingerprint),
        )
        .expect("stored discovery key material should be visible");
        assert_eq!(status, DiscoveryKeyMaterialStatus::Available);
        wait_for_delivery_security_future(bob_security.require_member_key_permission(
            &alice,
            alice_fingerprint,
            AuthorityScope::MemberRoutePublication,
        ))
        .expect("stored key material should permit route publication under the default policy");

        let error = wait_for_delivery_security_future(bob_security.require_member_key_permission(
            &alice,
            alice_fingerprint,
            AuthorityScope::ReplicationRuntime,
        ))
        .expect_err("candidate key material should not imply runtime trust");

        assert!(matches!(
            error,
            DeliverySecurityError::MemberKeyPermissionDenied {
                member_id,
                fingerprint,
                authority_scope: AuthorityScope::ReplicationRuntime,
                reason: PermissionDenialReason::MissingTrustEvidence,
            } if member_id == alice && fingerprint == alice_fingerprint
        ));
    }

    #[test]
    fn route_publication_permission_rejects_policy_denial() {
        let alice = alice_member();
        let bob = bob_member();
        let bob_store = sqlite_store(bob.clone());
        provision_security(bob_store.as_ref(), &bob, vec![alice.clone()]);
        let bob_security = load_delivery_security_with_policy(
            bob_store,
            &bob,
            TrustPolicy {
                replication_runtime: MemberKeyTrustRequirement::LocalExplicitTrust,
                bootstrap_activation: MemberKeyTrustRequirement::LocalExplicitTrust,
                member_route_publication: MemberKeyTrustRequirement::DenyAll,
            },
        );
        let alice_fingerprint = test_public_member_keys(&alice).fingerprint();

        let error = wait_for_delivery_security_future(bob_security.require_member_key_permission(
            &alice,
            alice_fingerprint,
            AuthorityScope::MemberRoutePublication,
        ))
        .expect_err("route-publication policy should deny the claim");

        assert!(matches!(
            error,
            DeliverySecurityError::MemberKeyPermissionDenied {
                member_id,
                fingerprint,
                authority_scope: AuthorityScope::MemberRoutePublication,
                reason: PermissionDenialReason::PolicyDenied,
            } if member_id == alice && fingerprint == alice_fingerprint
        ));
    }
}

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
        PermissionDecision,
        PermissionDenialReason,
        StoreError,
        StoreSecretKeyId,
        security::{
            AssessPublicKeyBundleRequest,
            PublicKeyBundleReport,
            RecordPublicKeyBundleFeedbackRequest,
        },
    },
    delivery::{
        group_broadcast::GroupMessageHeader,
        reliable_delivery::{RecipientAckHeader, ReliableMessageHeader},
        shared::{DetachedSignature, MessageId, SignatureScheme},
    },
    runtime::messages::{
        BootstrapGroupMessage,
        BootstrapMemberKeyMessage,
        RuntimeMessageError,
        validate_bootstrap_public_key_bundle_matches_fingerprint,
    },
    security_store::{SecurityStore, SecurityStoreError},
};
use bytes::Bytes;
use flotsync_core::{GroupId, MemberIdentity, member::TrieMap, membership::GroupMembers};
use flotsync_security::{
    FrameSignature,
    GroupKey,
    GroupMessageContext,
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
    sign_discovery_payload,
    sign_frame,
    verify_discovery_payload_signature,
    verify_frame_signature,
};
use itertools::Itertools;
use snafu::{Location, prelude::*};
use std::sync::Arc;
use uuid::Uuid;

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
        "Bootstrap sender {member_id} key fingerprint {fingerprint} lacks BootstrapActivation permission: {reason:?}."
    ))]
    BootstrapSenderPermissionDenied {
        member_id: MemberIdentity,
        fingerprint: KeyFingerprint,
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
    #[snafu(display("Failed to sign discovery claim payload: {source}"))]
    #[allow(
        dead_code,
        reason = "used by the route establishment adapter once host wiring lands"
    )]
    SignDiscoveryClaim { source: BoxError },
    #[snafu(display("Failed to verify recipient ack from recipient {recipient}: {source}"))]
    VerifyRecipientAck {
        recipient: MemberIdentity,
        source: BoxError,
    },
    #[snafu(display("Failed to verify discovery claim for member {member_id}: {source}"))]
    #[allow(
        dead_code,
        reason = "used by the route establishment adapter once host wiring lands"
    )]
    VerifyDiscoveryClaim {
        member_id: MemberIdentity,
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
            | Self::BootstrapSenderPermissionDenied { .. }
            | Self::UnexpectedBootstrapMemberKey { .. }
            | Self::InvalidBootstrapPublicKeyBundle { .. } => false,
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
            | Self::SignDiscoveryClaim { .. }
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

    /// Assess one decoded public key bundle against local security state.
    pub(crate) async fn assess_public_key_bundle(
        &self,
        request: AssessPublicKeyBundleRequest,
    ) -> Result<PublicKeyBundleReport, DeliverySecurityError> {
        self.security_store
            .assess_public_key_bundle(request)
            .await
            .context(SecurityStoreSnafu)
    }

    /// Record public key bundle feedback.
    pub(crate) async fn record_public_key_bundle_feedback(
        &self,
        request: RecordPublicKeyBundleFeedbackRequest,
    ) -> Result<(), DeliverySecurityError> {
        self.security_store
            .record_public_key_bundle_feedback(request)
            .await
            .context(SecurityStoreSnafu)
    }

    /// Generate a fresh group key for runtime bootstrap material.
    pub(crate) fn generate_group_key() -> Result<GroupKey, DeliverySecurityError> {
        GroupKey::generate().boxed().context(GenerateGroupKeySnafu)
    }

    /// Seal one reliable-delivery payload for its concrete recipient.
    pub(crate) async fn seal_reliable_payload(
        &self,
        header: &ReliableMessageHeader,
        plaintext: &[u8],
    ) -> Result<SealedHPKEPayload, DeliverySecurityError> {
        ensure!(
            header.sender == self.local_member,
            UnexpectedReliableSenderSnafu {
                sender: header.sender.clone(),
                local_member: self.local_member.clone(),
            }
        );
        ensure!(
            header.recipient != self.local_member,
            ReliableSelfMessageSnafu {
                member_id: self.local_member.clone(),
            }
        );
        let recipient_keys = self.load_permitted_public_keys(&header.recipient).await?;
        seal_reliable_payload_with_os_rng(
            self.local_keys(),
            &recipient_keys,
            ReliablePayloadContext {
                frame_kind: RELIABLE_RUNTIME_MESSAGE_FRAME_KIND,
                sender: &header.sender,
                recipient: &header.recipient,
                message_id: header.message_id.0,
            },
            plaintext,
        )
        .boxed()
        .with_context(|_| SealReliablePayloadSnafu {
            recipient: header.recipient.clone(),
        })
    }

    /// Open one reliable-delivery payload after its public header has been decoded.
    pub(crate) async fn open_reliable_payload(
        &self,
        header: &ReliableMessageHeader,
        sealed: &SealedHPKEPayload,
    ) -> Result<Vec<u8>, DeliverySecurityError> {
        ensure!(
            header.recipient == self.local_member,
            UnexpectedReliableRecipientSnafu {
                recipient: header.recipient.clone(),
                local_member: self.local_member.clone(),
            }
        );
        ensure!(
            header.sender != self.local_member,
            ReliableSelfMessageSnafu {
                member_id: self.local_member.clone(),
            }
        );
        let sender_keys = self.load_permitted_public_keys(&header.sender).await?;
        open_reliable_payload(
            &sender_keys,
            self.local_keys(),
            ReliablePayloadContext {
                frame_kind: RELIABLE_RUNTIME_MESSAGE_FRAME_KIND,
                sender: &header.sender,
                recipient: &header.recipient,
                message_id: header.message_id.0,
            },
            sealed,
        )
        .boxed()
        .context(OpenReliablePayloadSnafu)
    }

    /// Sign one semantic recipient ack for the original sender.
    pub(crate) fn sign_recipient_ack(
        &self,
        header: &RecipientAckHeader,
        public_header: &[u8],
    ) -> Result<DetachedSignature, DeliverySecurityError> {
        ensure!(
            header.recipient == self.local_member,
            UnexpectedRecipientAckRecipientSnafu {
                recipient: header.recipient.clone(),
                local_member: self.local_member.clone(),
            }
        );
        let signature = sign_frame(
            self.local_keys(),
            SignedFrameParts {
                frame_kind: RELIABLE_RECIPIENT_ACK_FRAME_KIND,
                public_header,
                ciphertext: &[],
            },
        )
        .boxed()
        .with_context(|_| SignRecipientAckSnafu {
            recipient: header.recipient.clone(),
        })?;
        Ok(DetachedSignature {
            scheme: SignatureScheme::Ed25519Ph,
            bytes: Bytes::copy_from_slice(signature.as_bytes()),
        })
    }

    /// Sign one exact encoded discovery claim payload with the local member key.
    #[allow(
        dead_code,
        reason = "used by the route establishment adapter once host wiring lands"
    )]
    pub(crate) fn sign_discovery_claim_payload(
        &self,
        payload: &[u8],
    ) -> Result<FrameSignature, DeliverySecurityError> {
        sign_discovery_payload(self.local_keys(), payload)
            .boxed()
            .context(SignDiscoveryClaimSnafu)
    }

    /// Verify one recipient ack against the expected recipient identity.
    pub(crate) async fn verify_recipient_ack(
        &self,
        header: &RecipientAckHeader,
        public_header: &[u8],
        signature: &DetachedSignature,
    ) -> Result<(), DeliverySecurityError> {
        ensure!(
            header.original_sender == self.local_member,
            UnexpectedRecipientAckOriginalSenderSnafu {
                original_sender: header.original_sender.clone(),
                local_member: self.local_member.clone(),
            }
        );
        ensure!(
            header.recipient != self.local_member,
            ReliableSelfMessageSnafu {
                member_id: self.local_member.clone(),
            }
        );
        let frame_signature = recipient_ack_frame_signature(header, signature)?;
        let recipient_keys = self.load_permitted_public_keys(&header.recipient).await?;
        verify_frame_signature(
            &recipient_keys,
            SignedFrameParts {
                frame_kind: RELIABLE_RECIPIENT_ACK_FRAME_KIND,
                public_header,
                ciphertext: &[],
            },
            &frame_signature,
        )
        .boxed()
        .with_context(|_| VerifyRecipientAckSnafu {
            recipient: header.recipient.clone(),
        })
    }

    /// Verify one exact encoded discovery claim payload against a trusted member key.
    #[allow(
        dead_code,
        reason = "used by the route establishment adapter once host wiring lands"
    )]
    pub(crate) async fn verify_discovery_claim_payload(
        &self,
        member_id: &MemberIdentity,
        payload: &[u8],
        signature: &FrameSignature,
    ) -> Result<(), DeliverySecurityError> {
        let public_keys = if member_id == &self.local_member {
            self.local_keys.public_keys().clone()
        } else {
            self.load_permitted_public_keys(member_id).await?
        };
        verify_discovery_payload_signature(&public_keys, payload, signature)
            .boxed()
            .with_context(|_| VerifyDiscoveryClaimSnafu {
                member_id: member_id.clone(),
            })
    }

    /// Seal one group-broadcast payload for transport fan-out.
    pub(crate) async fn seal_group_payload(
        &self,
        header: &GroupMessageHeader,
        public_header: &[u8],
        plaintext: &[u8],
    ) -> Result<SealedPSKPayload, DeliverySecurityError> {
        ensure!(
            header.sender == self.local_member,
            UnexpectedGroupSenderSnafu {
                sender: header.sender.clone(),
                local_member: self.local_member.clone(),
            }
        );
        let group_security = self.load_group_security(&header.group_id).await?;
        ensure!(
            group_security.member_keys.contains_member(&header.sender),
            GroupSenderNotInGroupSnafu {
                group_id: header.group_id,
                sender: header.sender.clone(),
            }
        );
        seal_group_payload(
            self.local_keys(),
            &group_security.group_key,
            group_message_context(header),
            public_header,
            plaintext,
        )
        .boxed()
        .context(SealGroupPayloadSnafu {
            group_id: header.group_id,
        })
    }

    /// Verify and open one group-broadcast payload received from transport.
    pub(crate) async fn open_group_payload(
        &self,
        header: &GroupMessageHeader,
        public_header: &[u8],
        sealed: &SealedPSKPayload,
    ) -> Result<Bytes, DeliverySecurityError> {
        let group_security = self.load_group_security(&header.group_id).await?;
        ensure!(
            group_security.member_keys.contains_member(&header.sender),
            GroupSenderNotInGroupSnafu {
                group_id: header.group_id,
                sender: header.sender.clone(),
            }
        );
        let expected_sender_key = group_security
            .member_keys
            .member_key(&header.sender)
            .expect("sender membership was checked against the same key set");
        let sender_keys = if header.sender == self.local_member {
            let local_public_keys = self.local_keys.public_keys().clone();
            ensure!(
                local_public_keys.fingerprint() == expected_sender_key.fingerprint,
                GroupSenderKeyFingerprintMismatchSnafu {
                    sender: header.sender.clone(),
                    expected: expected_sender_key.fingerprint,
                    actual: local_public_keys.fingerprint(),
                }
            );
            local_public_keys
        } else {
            self.load_public_keys_for_key_if_permitted(expected_sender_key)
                .await?
        };
        open_group_payload(
            &sender_keys,
            &group_security.group_key,
            group_message_context(header),
            public_header,
            sealed,
        )
        .boxed()
        .context(OpenGroupPayloadSnafu {
            group_id: header.group_id,
        })
    }

    /// Validate bootstrap member keys and prepare encrypted group-security material.
    pub(crate) async fn prepare_security_material_from_bootstrap_msg(
        &self,
        payload: &BootstrapGroupMessage,
        bootstrap_sender: &MemberIdentity,
    ) -> Result<EncryptedGroupSecurityMaterial, DeliverySecurityError> {
        self.validate_bootstrap_payload_member_keys(payload, bootstrap_sender)
            .await?;
        self.store_bootstrap_inline_public_keys(payload).await?;
        self.seal_group_secret(payload.group_id().0, payload.group_key().as_group_key())
    }

    fn local_public_bootstrap_key(&self) -> BootstrapMemberKeyMessage {
        BootstrapMemberKeyMessage::from_public_keys(self.local_keys.public_keys())
    }

    async fn load_group_security(
        &self,
        group_id: &GroupId,
    ) -> Result<GroupSecurityMaterial, DeliverySecurityError> {
        let mut transaction = self
            .security_store
            .replication_store()
            .begin_read_transaction()
            .await
            .context(StoreAccessSnafu)?;
        let record = transaction
            .load_replication_group(group_id)
            .await
            .context(StoreAccessSnafu)?
            .ok_or_else(|| DeliverySecurityError::MissingGroupSecurity {
                group_id: *group_id,
            })?;
        transaction.release().await.context(StoreAccessSnafu)?;
        let group_key = self.open_group_secret(record.group_id, &record.security_material)?;
        Ok(GroupSecurityMaterial {
            member_keys: record.member_keys,
            group_key,
        })
    }

    fn open_group_secret(
        &self,
        group_id: GroupId,
        security_material: &EncryptedGroupSecurityMaterial,
    ) -> Result<GroupKey, DeliverySecurityError> {
        let secret = &security_material.encrypted_group_secret;
        ensure!(
            secret.key_id == self.store_secret_key_id,
            GroupSecretKeyIdMismatchSnafu {
                group_id,
                expected: self.store_secret_key_id,
                actual: secret.key_id,
            }
        );
        let sealed = secret.to_store_secret_ciphertext()?;
        let context = StoreSecretContext {
            table: LOGICAL_GROUP_TABLE,
            column: LOGICAL_GROUP_SECRET_COLUMN,
            row_id: group_id.0.as_bytes(),
            key_id: self.store_secret_key_id.as_bytes(),
            crypto_version: StoreSecretCryptoVersion::new(secret.crypto_version.as_u16()),
        };
        open_stored_group_key(&self.store_secret_key, context, &sealed)
            .boxed()
            .context(OpenGroupSecretSnafu { group_id })
    }

    pub(crate) fn seal_group_secret(
        &self,
        group_id: Uuid,
        group_key: &GroupKey,
    ) -> Result<EncryptedGroupSecurityMaterial, DeliverySecurityError> {
        let row_id = group_id.as_bytes();
        let context = StoreSecretContext {
            table: LOGICAL_GROUP_TABLE,
            column: LOGICAL_GROUP_SECRET_COLUMN,
            row_id,
            key_id: self.store_secret_key_id.as_bytes(),
            crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
        };
        let plaintext = group_key.stored_secret_plaintext();
        let sealed = seal_store_secret(&self.store_secret_key, context, plaintext.as_slice())
            .boxed()
            .context(SealGroupSecretSnafu)?;
        Ok(EncryptedGroupSecurityMaterial {
            encrypted_group_secret: EncryptedStoreSecret::from_store_secret_ciphertext(
                self.store_secret_key_id,
                sealed,
            ),
        })
    }

    /// Load public keys for the exact members that will be included in one bootstrap payload.
    pub(crate) async fn public_keys_for_members(
        &self,
        members: &GroupMembers,
    ) -> Result<TrieMap<PublicMemberKeys>, DeliverySecurityError> {
        let mut public_keys = TrieMap::new();
        for member in members.ordered_members() {
            let member_public_keys = if member == self.local_member {
                self.local_keys.public_keys().clone()
            } else {
                self.load_permitted_public_keys(&member).await?
            };
            public_keys.insert(member, member_public_keys);
        }
        Ok(public_keys)
    }

    /// Load and decode public keys permitted for replication runtime use.
    async fn load_permitted_public_keys(
        &self,
        member_id: &MemberIdentity,
    ) -> Result<PublicMemberKeys, DeliverySecurityError> {
        self.load_permitted_public_keys_for_scope(member_id, AuthorityScope::ReplicationRuntime)
            .await
    }

    /// Load and decode public keys permitted for one authority scope.
    async fn load_permitted_public_keys_for_scope(
        &self,
        member_id: &MemberIdentity,
        authority_scope: AuthorityScope,
    ) -> Result<PublicMemberKeys, DeliverySecurityError> {
        self.security_store
            .load_permitted_member_public_keys(member_id, authority_scope)
            .await
            .context(SecurityStoreSnafu)
    }

    /// Load and decode public keys for one exact member-key binding if permitted.
    async fn load_public_keys_for_key_if_permitted(
        &self,
        key_id: &MemberKeyId,
    ) -> Result<PublicMemberKeys, DeliverySecurityError> {
        self.security_store
            .load_member_key_public_keys_if_permitted(key_id, AuthorityScope::ReplicationRuntime)
            .await
            .context(SecurityStoreSnafu)
    }

    /// Compare bootstrap payload key references against locally provisioned trust records.
    pub(crate) async fn validate_bootstrap_payload_member_keys(
        &self,
        payload: &BootstrapGroupMessage,
        bootstrap_sender: &MemberIdentity,
    ) -> Result<(), DeliverySecurityError> {
        for member_id in payload.members() {
            if payload.member_keys().get(member_id).is_none() {
                return Err(DeliverySecurityError::MissingBootstrapMemberKey {
                    member_id: member_id.clone(),
                });
            }
        }
        if payload.member_keys().len() != payload.members().len() {
            for (member_id, _) in payload.member_keys().owned_entries() {
                if !payload.members().contains(&member_id) {
                    return Err(DeliverySecurityError::UnexpectedBootstrapMemberKey { member_id });
                }
            }
        }
        let Some(sender_key) = payload.member_keys().get(bootstrap_sender) else {
            return Err(DeliverySecurityError::MissingBootstrapMemberKey {
                member_id: bootstrap_sender.clone(),
            });
        };
        if bootstrap_sender == &self.local_member {
            let expected_fingerprint = self.local_public_bootstrap_key().fingerprint();
            ensure!(
                sender_key.fingerprint() == expected_fingerprint,
                BootstrapKeyFingerprintMismatchSnafu {
                    member_id: bootstrap_sender.clone(),
                    expected: expected_fingerprint,
                    actual: sender_key.fingerprint(),
                }
            );
        } else {
            self.request_bootstrap_sender_permission(bootstrap_sender, sender_key.fingerprint())
                .await?;
        }
        for (member_id, member_key) in payload.member_keys().owned_entries() {
            if let Some(public_keys) = member_key.public_keys() {
                validate_bootstrap_public_key_bundle_matches_fingerprint(
                    &member_id,
                    member_key.fingerprint(),
                    public_keys,
                )
                .context(InvalidBootstrapPublicKeyBundleSnafu { member_id })?;
            }
        }
        Ok(())
    }

    /// Request `BootstrapActivation` permission for one advertised sender key.
    async fn request_bootstrap_sender_permission(
        &self,
        bootstrap_sender: &MemberIdentity,
        fingerprint: KeyFingerprint,
    ) -> Result<(), DeliverySecurityError> {
        let decision = self
            .security_store
            .request_member_key_permission_for(
                bootstrap_sender,
                fingerprint,
                AuthorityScope::BootstrapActivation,
            )
            .await
            .context(SecurityStoreSnafu)?;
        match decision {
            PermissionDecision::Permit => Ok(()),
            PermissionDecision::Deny(reason) => {
                Err(DeliverySecurityError::BootstrapSenderPermissionDenied {
                    member_id: bootstrap_sender.clone(),
                    fingerprint,
                    reason,
                })
            }
        }
    }

    /// Store valid inline bootstrap public bundles as observed key material.
    async fn store_bootstrap_inline_public_keys(
        &self,
        payload: &BootstrapGroupMessage,
    ) -> Result<(), DeliverySecurityError> {
        let inline_public_keys = payload
            .member_keys()
            .owned_entries()
            .filter_map(|(member_id, member_key)| {
                member_key
                    .public_keys()
                    .map(|public_keys| (member_id, member_key.fingerprint(), public_keys.clone()))
            })
            .collect_vec();
        if !inline_public_keys.is_empty() {
            let mut transaction = self
                .security_store
                .replication_store()
                .begin_transaction()
                .await
                .context(StoreAccessSnafu)?;
            for (member_id, expected_fingerprint, public_keys) in inline_public_keys {
                validate_bootstrap_public_key_bundle_matches_fingerprint(
                    &member_id,
                    expected_fingerprint,
                    &public_keys,
                )
                .context(InvalidBootstrapPublicKeyBundleSnafu {
                    member_id: member_id.clone(),
                })?;
                transaction
                    .ensure_member_public_keys(MemberPublicKeysRecord::from_public_keys(
                        &public_keys,
                    ))
                    .await
                    .context(StoreAccessSnafu)?;
            }
            transaction.commit().await.context(StoreAccessSnafu)?;
        }
        Ok(())
    }
}

fn group_message_context(header: &GroupMessageHeader) -> GroupMessageContext<'_> {
    GroupMessageContext {
        group_id: header.group_id.0,
        frame_kind: GROUP_BROADCAST_RUNTIME_MESSAGE_FRAME_KIND,
        sender: &header.sender,
        message_id: header.message_id.0,
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

/// Decrypted group security state needed for one group-broadcast frame.
struct GroupSecurityMaterial {
    /// Exact member-key bindings expected by the persisted group record.
    member_keys: GroupMemberKeys,
    /// Symmetric group key used to open or seal group payloads.
    group_key: GroupKey,
}

fn open_local_private_keys(
    record: &LocalMemberPrivateKeysRecord,
    store_secret_key: &StoreSecretKey,
) -> Result<LocalMemberKeys, DeliverySecurityError> {
    let secret = &record.private_keys.secret;
    let sealed = secret.to_store_secret_ciphertext()?;
    let row_id = record.member_id.to_string();
    let context = StoreSecretContext {
        table: LOGICAL_LOCAL_MEMBER_TABLE,
        column: LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN,
        row_id: row_id.as_bytes(),
        key_id: secret.key_id.as_bytes(),
        crypto_version: StoreSecretCryptoVersion::new(secret.crypto_version.as_u16()),
    };
    let plaintext = open_store_secret(store_secret_key, context, &sealed)
        .boxed()
        .context(OpenLocalPrivateKeysSnafu)?;
    local_member_keys_from_private_bundle(plaintext.as_slice(), record.member_id.clone())
        .boxed()
        .context(InvalidLocalPrivateKeysSnafu)
}

impl EncryptedStoreSecret {
    /// Convert this opaque encrypted-cell record into the fixed-width security ciphertext type.
    pub(crate) fn to_store_secret_ciphertext(
        &self,
    ) -> Result<StoreSecretCiphertext, DeliverySecurityError> {
        let version = self.crypto_version.as_u16();
        ensure!(
            version == STORE_SECRET_CRYPTO_VERSION_V1.as_u16(),
            UnsupportedStoreSecretVersionSnafu { version }
        );
        let nonce = self.nonce.as_ref().try_into().map_err(|_| {
            DeliverySecurityError::InvalidStoreSecretNonce {
                actual: self.nonce.len(),
            }
        })?;
        Ok(StoreSecretCiphertext {
            nonce,
            ciphertext: self.ciphertext.as_ref().to_vec(),
        })
    }
}

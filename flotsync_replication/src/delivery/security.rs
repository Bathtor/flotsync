use crate::{
    GroupMembers,
    GroupMembersError,
    api::{
        BoxError,
        EncryptedGroupSecurityMaterial,
        EncryptedStoreSecret,
        GroupId,
        LocalMemberPrivateKeysRecord,
        MemberIdentity,
        ReplicationStore,
        StoreError,
        StoreSecretKeyId,
        TrustedMemberPublicKeysRecord,
    },
    delivery::{
        group_broadcast::GroupMessageHeader,
        reliable_delivery::{RecipientAckHeader, ReliableMessageHeader},
        shared::{DetachedSignature, MessageId, SignatureScheme},
    },
    runtime::messages::{BootstrapGroupMessage, BootstrapMemberPublicKeysMessage},
};
use bytes::Bytes;
use flotsync_core::member::TrieMap;
use flotsync_security::{
    FrameSignature,
    GroupKey,
    GroupMessageContext,
    LocalMemberKeys,
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
    local_member_keys_from_jwks,
    open_group_payload,
    open_reliable_payload,
    open_store_secret,
    open_stored_group_key,
    seal_group_payload,
    seal_reliable_payload_with_os_rng,
    seal_store_secret,
    sign_frame,
    verify_frame_signature,
};
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
    #[snafu(display("Trusted public keys for member {member_id} are not provisioned."))]
    MissingTrustedPublicKeys { member_id: MemberIdentity },
    #[snafu(display("Security material for group {group_id} is not provisioned."))]
    MissingGroupSecurity { group_id: GroupId },
    #[snafu(display("Stored group {group_id} has invalid members: {source}"))]
    InvalidGroupMembers {
        group_id: GroupId,
        source: GroupMembersError,
    },
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
    #[snafu(display("Bootstrap payload is missing public keys for member {member_id}."))]
    MissingBootstrapPublicKeys { member_id: MemberIdentity },
    #[snafu(display("Bootstrap public keys for member {member_id} do not match trusted keys."))]
    TrustedPublicKeysMismatch { member_id: MemberIdentity },
    #[snafu(display("Bootstrap public keys included non-member {member_id}."))]
    UnexpectedBootstrapPublicKeys { member_id: MemberIdentity },
    #[snafu(display("Encrypted secret used unsupported crypto version {version}."))]
    UnsupportedStoreSecretVersion { version: u16 },
    #[snafu(display(
        "Encrypted secret nonce had invalid length {actual}; expected {STORE_SECRET_NONCE_LENGTH}."
    ))]
    InvalidStoreSecretNonce { actual: usize },
    #[snafu(display(
        "Trusted public key bytes for member {member_id} had invalid length {actual}; expected {expected}."
    ))]
    InvalidTrustedPublicKeyLength {
        member_id: MemberIdentity,
        expected: usize,
        actual: usize,
    },
    #[snafu(display(
        "Encrypted group secret for group {group_id} used key id {actual}; expected {expected}."
    ))]
    GroupSecretKeyIdMismatch {
        group_id: GroupId,
        expected: StoreSecretKeyId,
        actual: StoreSecretKeyId,
    },
    #[snafu(display("Local private key payload was not valid UTF-8: {source}"))]
    InvalidLocalPrivateKeyUtf8 { source: std::str::Utf8Error },
    #[snafu(display("Local private keys were invalid: {source}"))]
    InvalidLocalPrivateKeys { source: BoxError },
    #[snafu(display("Trusted public keys for member {member_id} were invalid: {source}"))]
    InvalidTrustedPublicKeys {
        member_id: MemberIdentity,
        source: BoxError,
    },
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
            // Missing trusted keys require provisioning or a trust update.
            Self::MissingTrustedPublicKeys { .. } => false,
            // Missing or malformed group-security state requires provisioning or data repair.
            Self::MissingGroupSecurity { .. } | Self::InvalidGroupMembers { .. } => false,
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
            Self::UnexpectedGroupSender { .. } | Self::GroupSenderNotInGroup { .. } => false,
            // Bootstrap payload membership and key mismatches are permanent for this payload.
            Self::MissingBootstrapPublicKeys { .. }
            | Self::TrustedPublicKeysMismatch { .. }
            | Self::UnexpectedBootstrapPublicKeys { .. } => false,
            // Stored secret encoding/version problems need data repair or a code change.
            Self::UnsupportedStoreSecretVersion { .. } | Self::InvalidStoreSecretNonce { .. } => {
                false
            }
            // Invalid key material needs reprovisioning.
            Self::InvalidTrustedPublicKeyLength { .. } | Self::InvalidTrustedPublicKeys { .. } => {
                false
            }
            // Group-secret key id mismatches require loading with the matching local key id.
            Self::GroupSecretKeyIdMismatch { .. } => false,
            // Invalid local private-key records need reprovisioning.
            Self::InvalidLocalPrivateKeyUtf8 { .. } | Self::InvalidLocalPrivateKeys { .. } => false,
            // Group-key generation depends on OS randomness and may recover on retry.
            Self::GenerateGroupKey { .. } => true,
            // Reliable-payload seal/open failures are crypto or key-consistency failures.
            Self::SealReliablePayload { .. } | Self::OpenReliablePayload { .. } => false,
            // Recipient-ack sign/verify failures are crypto or key-consistency failures.
            Self::SignRecipientAck { .. } | Self::VerifyRecipientAck { .. } => false,
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
    store: Arc<dyn ReplicationStore>,
    local_member: MemberIdentity,
    local_keys: Arc<LocalMemberKeys>,
    store_secret_key: Arc<StoreSecretKey>,
    store_secret_key_id: StoreSecretKeyId,
}

impl DeliverySecurity {
    pub(crate) async fn load(
        store: Arc<dyn ReplicationStore>,
        local_member: &MemberIdentity,
        store_secret_key: Arc<StoreSecretKey>,
        store_secret_key_id: StoreSecretKeyId,
    ) -> Result<Self, DeliverySecurityError> {
        let mut transaction = store
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
            store,
            local_member: local_member.clone(),
            local_keys: Arc::new(local_keys),
            store_secret_key,
            store_secret_key_id,
        })
    }

    pub(crate) fn local_keys(&self) -> &LocalMemberKeys {
        &self.local_keys
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
        let recipient_keys = self.load_trusted_public_keys(&header.recipient).await?;
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
        let sender_keys = self.load_trusted_public_keys(&header.sender).await?;
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
            scheme: SignatureScheme::Ed25519,
            bytes: Bytes::copy_from_slice(signature.as_bytes()),
        })
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
        let recipient_keys = self.load_trusted_public_keys(&header.recipient).await?;
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
            group_security.members.contains(&header.sender),
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
            group_security.members.contains(&header.sender),
            GroupSenderNotInGroupSnafu {
                group_id: header.group_id,
                sender: header.sender.clone(),
            }
        );
        let sender_keys = if header.sender == self.local_member {
            self.local_keys.public_keys().clone()
        } else {
            self.load_trusted_public_keys(&header.sender).await?
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

    /// Validate bootstrap public keys and prepare encrypted group-security material.
    pub(crate) async fn prepare_security_material_from_bootstrap_msg(
        &self,
        payload: &BootstrapGroupMessage,
    ) -> Result<EncryptedGroupSecurityMaterial, DeliverySecurityError> {
        self.validate_bootstrap_payload_public_keys(payload).await?;
        self.seal_group_secret(payload.group_id().0, payload.group_key().as_group_key())
    }

    fn local_public_bootstrap_keys(&self) -> BootstrapMemberPublicKeysMessage {
        BootstrapMemberPublicKeysMessage::from_public_keys(self.local_keys.public_keys())
    }

    async fn load_group_security(
        &self,
        group_id: &GroupId,
    ) -> Result<GroupSecurityMaterial, DeliverySecurityError> {
        let mut transaction = self
            .store
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
        let members = GroupMembers::from_ordered_members(record.members).context(
            InvalidGroupMembersSnafu {
                group_id: record.group_id,
            },
        )?;
        let group_key = self.open_group_secret(record.group_id, &record.security_material)?;
        Ok(GroupSecurityMaterial { members, group_key })
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
                self.load_trusted_public_keys(&member).await?
            };
            public_keys.insert(member, member_public_keys);
        }
        Ok(public_keys)
    }

    /// Load and decode trusted public keys for one member from the replication store.
    async fn load_trusted_public_keys(
        &self,
        member_id: &MemberIdentity,
    ) -> Result<PublicMemberKeys, DeliverySecurityError> {
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(StoreAccessSnafu)?;
        let record = transaction
            .load_trusted_member_public_keys(member_id)
            .await
            .context(StoreAccessSnafu)?
            .ok_or_else(|| DeliverySecurityError::MissingTrustedPublicKeys {
                member_id: member_id.clone(),
            })?;
        transaction.release().await.context(StoreAccessSnafu)?;
        public_keys_from_record(record)
    }

    /// Compare bootstrap payload keys against locally provisioned trust records.
    pub(crate) async fn validate_bootstrap_payload_public_keys(
        &self,
        payload: &BootstrapGroupMessage,
    ) -> Result<(), DeliverySecurityError> {
        for member_id in payload.members() {
            if payload.member_public_keys().get(member_id).is_none() {
                return Err(DeliverySecurityError::MissingBootstrapPublicKeys {
                    member_id: member_id.clone(),
                });
            }
        }
        if payload.member_public_keys().len() != payload.members().len() {
            for (member_id, _) in payload.member_public_keys() {
                if !payload.members().contains(&member_id) {
                    return Err(DeliverySecurityError::UnexpectedBootstrapPublicKeys { member_id });
                }
            }
        }
        for (member_id, member_keys) in payload.member_public_keys() {
            let trusted_keys = if member_id == self.local_member {
                self.local_public_bootstrap_keys()
            } else {
                let trusted = self.load_trusted_public_keys(&member_id).await?;
                BootstrapMemberPublicKeysMessage::from_public_keys(&trusted)
            };
            if &trusted_keys != member_keys {
                return Err(DeliverySecurityError::TrustedPublicKeysMismatch { member_id });
            }
        }
        Ok(())
    }
}

/// Decode one opaque trusted-public-key store record into typed security keys.
pub(crate) fn public_keys_from_record(
    record: TrustedMemberPublicKeysRecord,
) -> Result<PublicMemberKeys, DeliverySecurityError> {
    let signing_public_key =
        fixed_public_key(&record.member_id, record.signing_public_key.as_ref())?;
    let encryption_public_key =
        fixed_public_key(&record.member_id, record.encryption_public_key.as_ref())?;
    PublicMemberKeys::from_key_bytes(
        record.member_id.clone(),
        signing_public_key,
        encryption_public_key,
    )
    .boxed()
    .context(InvalidTrustedPublicKeysSnafu {
        member_id: record.member_id,
    })
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
        SignatureScheme::Ed25519 => {}
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
    members: GroupMembers,
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
    let jwks =
        std::str::from_utf8(plaintext.as_slice()).context(InvalidLocalPrivateKeyUtf8Snafu)?;
    local_member_keys_from_jwks(jwks, Some(&record.member_id))
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

/// Validate and copy opaque public-key bytes from store records.
fn fixed_public_key<const N: usize>(
    member_id: &MemberIdentity,
    bytes: &[u8],
) -> Result<[u8; N], DeliverySecurityError> {
    bytes
        .try_into()
        .map_err(|_| DeliverySecurityError::InvalidTrustedPublicKeyLength {
            member_id: member_id.clone(),
            expected: N,
            actual: bytes.len(),
        })
}

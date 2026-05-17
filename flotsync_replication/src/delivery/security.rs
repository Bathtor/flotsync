use crate::{
    GroupMembers,
    api::{
        BoxError,
        EncryptedGroupSecurityMaterial,
        EncryptedStoreSecret,
        LocalMemberPrivateKeysRecord,
        MemberIdentity,
        ReplicationStore,
        StoreError,
        StoreSecretKeyId,
        TrustedMemberPublicKeysRecord,
    },
    delivery::reliable_delivery::ReliableMessageHeader,
    runtime::messages::{BootstrapGroupMessage, BootstrapMemberPublicKeysMessage},
};
use flotsync_core::member::TrieMap;
use flotsync_security::{
    GroupKey,
    LocalMemberKeys,
    PublicMemberKeys,
    ReliablePayloadContext,
    STORE_SECRET_CRYPTO_VERSION_V1,
    STORE_SECRET_NONCE_LENGTH,
    SealedReliablePayload,
    StoreSecretCiphertext,
    StoreSecretContext,
    StoreSecretCryptoVersion,
    StoreSecretKey,
    local_member_keys_from_jwks,
    open_reliable_payload,
    open_store_secret,
    seal_reliable_payload_with_os_rng,
    seal_store_secret,
};
use snafu::{Location, prelude::*};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) const RELIABLE_RUNTIME_MESSAGE_FRAME_KIND: &str = "reliable-runtime-message";

const LOGICAL_GROUP_TABLE: &str = "replication_group";
const LOGICAL_GROUP_SECRET_COLUMN: &str = "group_secret";
const LOGICAL_LOCAL_MEMBER_TABLE: &str = "local_member";
const LOGICAL_LOCAL_PRIVATE_KEYS_COLUMN: &str = "private_keys";

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
    #[snafu(display("Local private key payload was not valid UTF-8: {source}"))]
    InvalidLocalPrivateKeyUtf8 { source: std::string::FromUtf8Error },
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
            // Reliable self-send is a caller bug.
            Self::ReliableSelfMessage { .. } => false,
            // Sender/recipient mismatches are local routing or authenticity violations.
            Self::UnexpectedReliableSender { .. } | Self::UnexpectedReliableRecipient { .. } => {
                false
            }
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
            // Invalid local private-key records need reprovisioning.
            Self::InvalidLocalPrivateKeyUtf8 { .. } | Self::InvalidLocalPrivateKeys { .. } => false,
            // Group-key generation depends on OS randomness and may recover on retry.
            Self::GenerateGroupKey { .. } => true,
            // Reliable-payload seal/open failures are crypto or key-consistency failures.
            Self::SealReliablePayload { .. } | Self::OpenReliablePayload { .. } => false,
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
    ) -> Result<SealedReliablePayload, DeliverySecurityError> {
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
        sealed: &SealedReliablePayload,
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
            key_id: self.store_secret_key_id.as_str(),
            crypto_version: STORE_SECRET_CRYPTO_VERSION_V1,
        };
        let plaintext = group_key.stored_secret_plaintext();
        let sealed = seal_store_secret(&self.store_secret_key, context, plaintext.as_slice())
            .boxed()
            .context(SealGroupSecretSnafu)?;
        Ok(EncryptedGroupSecurityMaterial {
            encrypted_group_secret: EncryptedStoreSecret::from_store_secret_ciphertext(
                self.store_secret_key_id.clone(),
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
        key_id: secret.key_id.as_str(),
        crypto_version: StoreSecretCryptoVersion::new(secret.crypto_version.as_u16()),
    };
    let plaintext = open_store_secret(store_secret_key, context, &sealed)
        .boxed()
        .context(OpenLocalPrivateKeysSnafu)?;
    let jwks = String::from_utf8(plaintext).context(InvalidLocalPrivateKeyUtf8Snafu)?;
    local_member_keys_from_jwks(&jwks, Some(&record.member_id))
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

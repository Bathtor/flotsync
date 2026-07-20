//! Group-broadcast envelope handling and group-bootstrap security material.

use super::*;
use itertools::Itertools;

impl DeliverySecurity {
    /// Generate a fresh group key for runtime bootstrap material.
    pub(crate) fn generate_group_key() -> Result<GroupKey, DeliverySecurityError> {
        GroupKey::generate().boxed().context(GenerateGroupKeySnafu)
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
    pub(crate) async fn prepare_security_material_from_group_setup(
        &self,
        group_id: GroupId,
        payload: &GroupSetupMessage,
        setup_sender: &MemberIdentity,
    ) -> Result<EncryptedGroupSecurityMaterial, DeliverySecurityError> {
        self.validate_group_setup_member_keys(payload, setup_sender)
            .await?;
        self.store_group_setup_inline_public_keys(payload).await?;
        self.seal_group_secret(group_id.0, payload.group_key().as_group_key())
    }

    /// Validate replayed setup against existing encrypted group-security material.
    ///
    /// Callers are expected to compare cheap group-definition fields before
    /// invoking this method. The stored key is only decrypted after member-key
    /// trust and inline bundle validation succeed.
    pub(crate) async fn validate_existing_group_setup_security(
        &self,
        group_id: GroupId,
        payload: &GroupSetupMessage,
        setup_sender: &MemberIdentity,
        security_material: &EncryptedGroupSecurityMaterial,
    ) -> Result<(), DeliverySecurityError> {
        self.validate_group_setup_member_keys(payload, setup_sender)
            .await?;
        self.store_group_setup_inline_public_keys(payload).await?;
        let stored_group_key = self.open_group_secret(group_id, security_material)?;
        ensure!(
            stored_group_key == *payload.group_key().as_group_key(),
            GroupSetupKeyMismatchSnafu { group_id }
        );
        Ok(())
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

    /// Store valid inline bootstrap public bundles as observed key material.
    async fn store_group_setup_inline_public_keys(
        &self,
        payload: &GroupSetupMessage,
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

/// Decrypted group security state needed for one group-broadcast frame.
struct GroupSecurityMaterial {
    /// Exact member-key bindings expected by the persisted group record.
    member_keys: GroupMemberKeys,
    /// Symmetric group key used to open or seal group payloads.
    group_key: GroupKey,
}

//! Public member-key trust, discovery, and bootstrap validation helpers.

use super::*;

impl DeliverySecurity {
    /// Return local availability of exact public key material for discovery verification.
    pub(crate) async fn discovery_key_material_status(
        &self,
        member_id: &MemberIdentity,
        key_fingerprint: KeyFingerprint,
    ) -> Result<DiscoveryKeyMaterialStatus, DeliverySecurityError> {
        let key_id = MemberKeyId {
            member_id: member_id.clone(),
            fingerprint: key_fingerprint,
        };
        match self
            .security_store
            .load_member_key_public_keys(&key_id, MemberPublicKeyLoadPolicy::RejectBlocked)
            .await
        {
            Ok(_) => Ok(DiscoveryKeyMaterialStatus::Available),
            Err(SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                denial_reasons, ..
            }) if denial_reasons.contains(&PermissionDenialReason::FingerprintBlocked) => {
                Ok(DiscoveryKeyMaterialStatus::Unusable)
            }
            Err(SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                denial_reasons, ..
            }) if denial_reasons.contains(&PermissionDenialReason::MissingKeyMaterial) => {
                Ok(DiscoveryKeyMaterialStatus::Missing)
            }
            Err(source) => Err(source).context(SecurityStoreSnafu),
        }
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

    /// Ensure one directly acquired public key bundle as candidate discovery key material.
    pub(crate) async fn ensure_discovery_public_key_bundle(
        &self,
        member_id: &MemberIdentity,
        bundle: PublicKeyBundle,
    ) -> Result<(), DeliverySecurityError> {
        self.security_store
            .ensure_discovery_public_key_bundle(member_id, bundle)
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

    /// Verify one exact encoded discovery claim payload against selected member key material.
    pub(crate) async fn verify_discovery_claim_payload(
        &self,
        member_id: &MemberIdentity,
        key_fingerprint: KeyFingerprint,
        payload: &[u8],
        signature: &FrameSignature,
    ) -> Result<(), DeliverySecurityError> {
        let public_keys = if member_id == &self.local_member
            && key_fingerprint == self.local_keys.public_keys().fingerprint()
        {
            self.local_keys.public_keys().clone()
        } else {
            let key_id = MemberKeyId {
                member_id: member_id.clone(),
                fingerprint: key_fingerprint,
            };
            self.security_store
                .load_member_key_public_keys(&key_id, MemberPublicKeyLoadPolicy::RejectBlocked)
                .await
                .context(SecurityStoreSnafu)?
        };
        verify_discovery_payload_signature(&public_keys, payload, signature)
            .boxed()
            .with_context(|_| VerifyDiscoveryClaimSnafu {
                member_id: member_id.clone(),
                fingerprint: key_fingerprint,
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
    pub(super) async fn load_permitted_public_keys(
        &self,
        member_id: &MemberIdentity,
    ) -> Result<PublicMemberKeys, DeliverySecurityError> {
        self.load_permitted_public_keys_for_scope(member_id, AuthorityScope::ReplicationRuntime)
            .await
    }

    /// Load and decode public keys for one authority scope.
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
    pub(super) async fn load_public_keys_for_key_if_permitted(
        &self,
        key_id: &MemberKeyId,
    ) -> Result<PublicMemberKeys, DeliverySecurityError> {
        self.security_store
            .load_member_key_public_keys_if_permitted(key_id, AuthorityScope::ReplicationRuntime)
            .await
            .context(SecurityStoreSnafu)
    }

    /// Compare bootstrap payload key references against locally provisioned trust records.
    pub(crate) async fn validate_group_setup_member_keys(
        &self,
        payload: &GroupSetupMessage,
        setup_sender: &MemberIdentity,
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
        let Some(sender_key) = payload.member_keys().get(setup_sender) else {
            return Err(DeliverySecurityError::MissingBootstrapMemberKey {
                member_id: setup_sender.clone(),
            });
        };
        if setup_sender == &self.local_member {
            let expected_fingerprint = self.local_public_bootstrap_key().fingerprint();
            ensure!(
                sender_key.fingerprint() == expected_fingerprint,
                BootstrapKeyFingerprintMismatchSnafu {
                    member_id: setup_sender.clone(),
                    expected: expected_fingerprint,
                    actual: sender_key.fingerprint(),
                }
            );
        } else {
            self.require_member_key_permission(
                setup_sender,
                sender_key.fingerprint(),
                AuthorityScope::BootstrapActivation,
            )
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

    /// Require one exact member key to have the requested authority scope.
    pub(crate) async fn require_member_key_permission(
        &self,
        member_id: &MemberIdentity,
        fingerprint: KeyFingerprint,
        authority_scope: AuthorityScope,
    ) -> Result<(), DeliverySecurityError> {
        let decision = self
            .security_store
            .request_member_key_permission_for(member_id, fingerprint, authority_scope)
            .await
            .context(SecurityStoreSnafu)?;
        decision
            .ok()
            .map_err(|reason| DeliverySecurityError::MemberKeyPermissionDenied {
                member_id: member_id.clone(),
                fingerprint,
                authority_scope,
                reason,
            })
    }

    /// Return the local public key reference used in bootstrap messages.
    fn local_public_bootstrap_key(&self) -> BootstrapMemberKeyMessage {
        BootstrapMemberKeyMessage::from_public_keys(self.local_keys.public_keys())
    }
}

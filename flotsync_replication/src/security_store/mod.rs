//! Public security-state facade for member-key permission checks.

use crate::api::{
    AuthorityScope,
    MemberKeyId,
    MemberKeyTrustEvidenceKind,
    MemberKeyTrustEvidenceRecord,
    MemberKeyTrustEvidenceSet,
    MemberKeyTrustRequirement,
    MemberPublicKeysRecord,
    PermissionDecision,
    PermissionDenialReason,
    ReplicationStore,
    ReplicationStoreReadTransaction,
    ReplicationStoreTransaction,
    StoreError,
    TrustPolicy,
    security::{
        AssessPublicKeyBundleRequest,
        CandidateMemberKeyReport,
        MemberKeyAuthorityReport,
        MemberKeyBindingReport,
        MemberKeyTrustReport,
        PublicKeyBundleAssessmentStorage,
        PublicKeyBundleFeedback,
        PublicKeyBundleReport,
        PublicKeyBundleSchemeReport,
        RecordPublicKeyBundleFeedbackRequest,
    },
};
use flotsync_core::MemberIdentity;
use flotsync_security::{KeyFingerprint, PublicKeyBundle, PublicMemberKeys};
use flotsync_utils::{BoxError, BoxFuture};
use futures_util::FutureExt as _;
use snafu::{Location, prelude::*};
use std::{collections::HashSet, sync::Arc};

mod feedback;
mod permissions;
mod reports;

use feedback::{
    ensure_candidate_public_key_bindings,
    ensure_discovery_public_key_binding,
    record_public_key_bundle_feedback_in_transaction,
};
use permissions::{
    request_member_key_permission,
    request_member_key_permission_from_transaction,
    request_stored_member_key_permission_from_transaction,
};
use reports::public_key_bundle_report_from_transaction;

/// Policy for loading exact member-key public material before a separate authority decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemberPublicKeyLoadPolicy {
    /// Return stored key material even if the fingerprint is blocked.
    #[allow(
        dead_code,
        reason = "Production signature verification currently rejects blocked keys; tests exercise this policy and future exact-key inspection paths may need it."
    )]
    AllowBlocked,
    /// Reject stored key material when the fingerprint is globally blocked.
    RejectBlocked,
}

impl MemberPublicKeyLoadPolicy {
    /// Return whether this policy rejects globally blocked fingerprints.
    const fn rejects_blocked(self) -> bool {
        matches!(self, Self::RejectBlocked)
    }
}

/// Public security-state facade used by runtime security code.
#[derive(Clone)]
pub(crate) struct SecurityStore {
    store: Arc<dyn ReplicationStore>,
    trust_policy: TrustPolicy,
}

impl SecurityStore {
    /// Build a security facade over one replication store and trust policy.
    #[must_use]
    pub(crate) fn new(store: Arc<dyn ReplicationStore>, trust_policy: TrustPolicy) -> Self {
        Self {
            store,
            trust_policy,
        }
    }

    /// Return the underlying replication store.
    #[must_use]
    pub(crate) fn replication_store(&self) -> &Arc<dyn ReplicationStore> {
        &self.store
    }

    /// Request permission for one exact member-key binding.
    pub(crate) fn request_member_key_permission<'a>(
        &'a self,
        key_id: &'a MemberKeyId,
        authority_scope: AuthorityScope,
    ) -> BoxFuture<'a, Result<PermissionDecision, SecurityStoreError>> {
        async move {
            let mut transaction = self
                .store
                .begin_read_transaction()
                .await
                .context(StoreAccessSnafu)?;
            let decision = request_member_key_permission_from_transaction(
                transaction.as_mut(),
                &self.trust_policy,
                authority_scope,
                key_id,
            )
            .await?;
            transaction.release().await.context(StoreAccessSnafu)?;
            Ok(decision)
        }
        .boxed()
    }

    /// Request permission for one exact member/fingerprint binding.
    pub(crate) fn request_member_key_permission_for<'a>(
        &'a self,
        member_id: &'a MemberIdentity,
        fingerprint: KeyFingerprint,
        authority_scope: AuthorityScope,
    ) -> BoxFuture<'a, Result<PermissionDecision, SecurityStoreError>> {
        async move {
            let key_id = MemberKeyId {
                member_id: member_id.clone(),
                fingerprint,
            };
            self.request_member_key_permission(&key_id, authority_scope)
                .await
        }
        .boxed()
    }

    /// Load this exact member key's public material if it exists and is permitted.
    pub(crate) fn load_member_key_public_keys_if_permitted<'a>(
        &'a self,
        key_id: &'a MemberKeyId,
        authority_scope: AuthorityScope,
    ) -> BoxFuture<'a, Result<PublicMemberKeys, SecurityStoreError>> {
        async move {
            let mut transaction = self
                .store
                .begin_read_transaction()
                .await
                .context(StoreAccessSnafu)?;
            let record = transaction
                .load_member_public_keys(key_id)
                .await
                .context(StoreAccessSnafu)?;
            let Some(record) = record else {
                transaction.release().await.context(StoreAccessSnafu)?;
                return Err(SecurityStoreError::MemberKeyPublicKeysUnavailable {
                    key_id: key_id.clone(),
                    authority_scope,
                    denial_reasons: vec![PermissionDenialReason::MissingKeyMaterial],
                });
            };
            let decision = request_stored_member_key_permission_from_transaction(
                transaction.as_mut(),
                &self.trust_policy,
                authority_scope,
                key_id,
            )
            .await?;
            transaction.release().await.context(StoreAccessSnafu)?;
            decision.ok().map_err(
                |reason| SecurityStoreError::MemberKeyPublicKeysUnavailable {
                    key_id: key_id.clone(),
                    authority_scope,
                    denial_reasons: vec![reason],
                },
            )?;
            public_keys_from_record(&record)
        }
        .boxed()
    }

    /// Load this exact member key's public material.
    pub(crate) fn load_member_key_public_keys<'a>(
        &'a self,
        key_id: &'a MemberKeyId,
        load_policy: MemberPublicKeyLoadPolicy,
    ) -> BoxFuture<'a, Result<PublicMemberKeys, SecurityStoreError>> {
        async move {
            let mut transaction = self
                .store
                .begin_read_transaction()
                .await
                .context(StoreAccessSnafu)?;
            let record = transaction
                .load_member_public_keys(key_id)
                .await
                .context(StoreAccessSnafu)?;
            let Some(record) = record else {
                transaction.release().await.context(StoreAccessSnafu)?;
                return Err(SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                    key_id: key_id.clone(),
                    denial_reasons: vec![PermissionDenialReason::MissingKeyMaterial],
                });
            };
            let reject_blocked_fingerprint = if load_policy.rejects_blocked() {
                transaction
                    .is_key_fingerprint_blocked(&key_id.fingerprint)
                    .await
                    .context(StoreAccessSnafu)?
            } else {
                false
            };
            transaction.release().await.context(StoreAccessSnafu)?;
            if reject_blocked_fingerprint {
                return Err(SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                    key_id: key_id.clone(),
                    denial_reasons: vec![PermissionDenialReason::FingerprintBlocked],
                });
            }
            public_keys_from_record(&record)
        }
        .boxed()
    }

    /// Load the single permitted public key bundle for one member and authority scope.
    pub(crate) fn load_permitted_member_public_keys<'a>(
        &'a self,
        member_id: &'a MemberIdentity,
        authority_scope: AuthorityScope,
    ) -> BoxFuture<'a, Result<PublicMemberKeys, SecurityStoreError>> {
        async move {
            let mut transaction = self
                .store
                .begin_read_transaction()
                .await
                .context(StoreAccessSnafu)?;
            let records = transaction
                .load_member_public_keys_for_member(member_id)
                .await
                .context(StoreAccessSnafu)?;
            let mut permitted = Vec::new();
            let mut denial_reasons = Vec::new();
            for record in records {
                let decision = request_member_key_permission_from_transaction(
                    transaction.as_mut(),
                    &self.trust_policy,
                    authority_scope,
                    &record.key_id,
                )
                .await?;
                match decision {
                    PermissionDecision::Permit => permitted.push(record),
                    PermissionDecision::Deny(reason) => denial_reasons.push(reason),
                }
            }
            transaction.release().await.context(StoreAccessSnafu)?;
            if permitted.is_empty() {
                if denial_reasons.is_empty() {
                    denial_reasons.push(PermissionDenialReason::MissingKeyMaterial);
                }
                return Err(SecurityStoreError::NoPermittedMemberPublicKeys {
                    member_id: member_id.clone(),
                    authority_scope,
                    denial_reasons,
                });
            }
            ensure!(
                permitted.len() == 1,
                AmbiguousPermittedMemberPublicKeysSnafu {
                    member_id: member_id.clone(),
                    authority_scope,
                    permitted_count: permitted.len(),
                }
            );
            let record = permitted
                .pop()
                .expect("permitted length is checked to contain exactly one key");
            public_keys_from_record(&record)
        }
        .boxed()
    }

    /// Assess one decoded public key bundle against local trust and block state.
    pub(crate) fn assess_public_key_bundle(
        &self,
        request: AssessPublicKeyBundleRequest,
    ) -> BoxFuture<'_, Result<PublicKeyBundleReport, SecurityStoreError>> {
        async move {
            let fingerprint = request.bundle.fingerprint();
            match request.material_storage {
                PublicKeyBundleAssessmentStorage::ReadOnly => {
                    let mut transaction = self
                        .store
                        .begin_read_transaction()
                        .await
                        .context(StoreAccessSnafu)?;
                    let report = public_key_bundle_report_from_transaction(
                        transaction.as_mut(),
                        &self.trust_policy,
                        fingerprint,
                        request.candidate_member_ids,
                    )
                    .await?;
                    transaction.release().await.context(StoreAccessSnafu)?;
                    Ok(report)
                }
                PublicKeyBundleAssessmentStorage::StoreCandidateBindings => {
                    let mut transaction = self
                        .store
                        .begin_transaction()
                        .await
                        .context(StoreAccessSnafu)?;
                    ensure_candidate_public_key_bindings(
                        transaction.as_mut(),
                        &request.bundle,
                        &request.candidate_member_ids,
                    )
                    .await?;
                    let report = public_key_bundle_report_from_transaction(
                        transaction.as_mut(),
                        &self.trust_policy,
                        fingerprint,
                        request.candidate_member_ids,
                    )
                    .await?;
                    transaction.commit().await.context(StoreAccessSnafu)?;
                    Ok(report)
                }
            }
        }
        .boxed()
    }

    /// Ensure one directly discovered public-key bundle binding if it is still useful.
    ///
    /// This operation is intentionally transactional: if the exact member/fingerprint binding is
    /// already present, the method leaves the store unchanged and returns success. Otherwise it
    /// records the candidate binding without adding trust evidence. Blocked fingerprints are
    /// rejected.
    pub(crate) fn ensure_discovery_public_key_bundle<'a>(
        &'a self,
        member_id: &'a MemberIdentity,
        bundle: PublicKeyBundle,
    ) -> BoxFuture<'a, Result<(), SecurityStoreError>> {
        async move {
            let mut transaction = self
                .store
                .begin_transaction()
                .await
                .context(StoreAccessSnafu)?;
            ensure_discovery_public_key_binding(transaction.as_mut(), &bundle, member_id.clone())
                .await?;
            transaction.commit().await.context(StoreAccessSnafu)?;
            Ok(())
        }
        .boxed()
    }

    /// Record user/application feedback for one public key bundle.
    pub(crate) fn record_public_key_bundle_feedback(
        &self,
        request: RecordPublicKeyBundleFeedbackRequest,
    ) -> BoxFuture<'_, Result<(), SecurityStoreError>> {
        async move {
            let fingerprint = request.bundle.fingerprint();
            let mut transaction = self
                .store
                .begin_transaction()
                .await
                .context(StoreAccessSnafu)?;
            record_public_key_bundle_feedback_in_transaction(
                transaction.as_mut(),
                &request.bundle,
                request.feedback,
                fingerprint,
            )
            .await?;
            transaction.commit().await.context(StoreAccessSnafu)?;
            Ok(())
        }
        .boxed()
    }
}

/// Security-store failures raised while loading or authorising public key material.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum SecurityStoreError {
    /// Replication-store access failed.
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    /// No observed member key had permission for the requested authority scope.
    #[snafu(display(
        "No public keys for member {member_id} have {authority_scope:?} permission: {denial_reasons:?}."
    ))]
    NoPermittedMemberPublicKeys {
        member_id: MemberIdentity,
        authority_scope: AuthorityScope,
        denial_reasons: Vec<PermissionDenialReason>,
    },
    /// One exact member key cannot be used for the requested authority.
    #[snafu(display(
        "Member key {key_id:?} cannot be used for {authority_scope:?}: {denial_reasons:?}."
    ))]
    MemberKeyPublicKeysUnavailable {
        key_id: MemberKeyId,
        authority_scope: AuthorityScope,
        denial_reasons: Vec<PermissionDenialReason>,
    },
    /// One exact member key's public material is unavailable.
    #[snafu(display("Member key {key_id:?} public material is unavailable: {denial_reasons:?}."))]
    ExactMemberKeyPublicKeysUnavailable {
        key_id: MemberKeyId,
        denial_reasons: Vec<PermissionDenialReason>,
    },
    /// More than one observed member key had permission where one key was required.
    #[snafu(display(
        "{permitted_count} public keys for member {member_id} have {authority_scope:?} permission."
    ))]
    AmbiguousPermittedMemberPublicKeys {
        member_id: MemberIdentity,
        authority_scope: AuthorityScope,
        permitted_count: usize,
    },
    /// Stored public key bytes could not be decoded.
    #[snafu(display(
        "Stored public keys for member {member_id} fingerprint {fingerprint} were invalid: {source}"
    ))]
    InvalidMemberPublicKeys {
        member_id: MemberIdentity,
        fingerprint: KeyFingerprint,
        source: BoxError,
    },
    /// Stored public key bytes did not derive their stored fingerprint.
    #[snafu(display(
        "Stored public keys for member {member_id} derived fingerprint {actual}, expected {expected}."
    ))]
    MemberPublicKeyFingerprintMismatch {
        member_id: MemberIdentity,
        expected: KeyFingerprint,
        actual: KeyFingerprint,
    },
    /// A globally blocked key fingerprint cannot be trusted again.
    #[snafu(display(
        "Public key bundle fingerprint {fingerprint} is globally blocked and cannot be trusted for member {member_id}."
    ))]
    BlockedKeyTrust {
        member_id: MemberIdentity,
        fingerprint: KeyFingerprint,
    },
}

/// Decode one borrowed opaque public-key store record into typed security keys.
pub(crate) fn public_keys_from_record(
    record: &MemberPublicKeysRecord,
) -> Result<PublicMemberKeys, SecurityStoreError> {
    let public_keys = PublicMemberKeys::from_key_bytes(
        record.key_id.member_id.clone(),
        record.signing_public_key.as_ref(),
        record.encryption_public_key.as_ref(),
    )
    .boxed()
    .context(InvalidMemberPublicKeysSnafu {
        member_id: record.key_id.member_id.clone(),
        fingerprint: record.key_id.fingerprint,
    })?;
    ensure!(
        public_keys.fingerprint() == record.key_id.fingerprint,
        MemberPublicKeyFingerprintMismatchSnafu {
            member_id: record.key_id.member_id.clone(),
            expected: record.key_id.fingerprint,
            actual: public_keys.fingerprint(),
        }
    );
    Ok(public_keys)
}

#[cfg(test)]
mod tests;

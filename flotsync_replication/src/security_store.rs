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

/// Request one member-key permission from already-loaded evidence.
pub(crate) fn request_member_key_permission(
    policy: &TrustPolicy,
    scope: AuthorityScope,
    _key_id: &MemberKeyId,
    evidence: MemberKeyTrustEvidenceSet,
    fingerprint_blocked: bool,
) -> PermissionDecision {
    if fingerprint_blocked {
        return PermissionDecision::Deny(PermissionDenialReason::FingerprintBlocked);
    }
    let requirement = match scope {
        AuthorityScope::ReplicationRuntime => policy.replication_runtime,
        AuthorityScope::BootstrapActivation => policy.bootstrap_activation,
        AuthorityScope::MemberRoutePublication => policy.member_route_publication,
    };
    match requirement {
        MemberKeyTrustRequirement::DenyAll => {
            PermissionDecision::Deny(PermissionDenialReason::PolicyDenied)
        }
        MemberKeyTrustRequirement::LocalExplicitTrust => {
            if evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust) {
                PermissionDecision::Permit
            } else {
                PermissionDecision::Deny(PermissionDenialReason::MissingTrustEvidence)
            }
        }
        MemberKeyTrustRequirement::StoredPublicKeyMaterial => PermissionDecision::Permit,
    }
}

/// Request one member-key permission by loading evidence from a transaction.
async fn request_member_key_permission_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    scope: AuthorityScope,
    key_id: &MemberKeyId,
) -> Result<PermissionDecision, SecurityStoreError> {
    let material = transaction
        .load_member_public_keys(key_id)
        .await
        .context(StoreAccessSnafu)?;
    if material.is_none() {
        return Ok(PermissionDecision::Deny(
            PermissionDenialReason::MissingKeyMaterial,
        ));
    }
    request_stored_member_key_permission_from_transaction(transaction, policy, scope, key_id).await
}

/// Request one member-key permission after key material has already been found.
async fn request_stored_member_key_permission_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    scope: AuthorityScope,
    key_id: &MemberKeyId,
) -> Result<PermissionDecision, SecurityStoreError> {
    let evidence = transaction
        .load_member_key_trust_evidence(key_id)
        .await
        .context(StoreAccessSnafu)?;
    let fingerprint_blocked = transaction
        .is_key_fingerprint_blocked(&key_id.fingerprint)
        .await
        .context(StoreAccessSnafu)?;
    Ok(request_member_key_permission(
        policy,
        scope,
        key_id,
        evidence,
        fingerprint_blocked,
    ))
}

/// Build a public bundle report from an already-open store transaction.
async fn public_key_bundle_report_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    fingerprint: KeyFingerprint,
    candidate_member_ids: HashSet<MemberIdentity>,
) -> Result<PublicKeyBundleReport, SecurityStoreError> {
    let globally_blocked = transaction
        .is_key_fingerprint_blocked(&fingerprint)
        .await
        .context(StoreAccessSnafu)?;
    let known_records = transaction
        .load_member_public_keys_for_fingerprint(&fingerprint)
        .await
        .context(StoreAccessSnafu)?;
    let mut known_bindings = Vec::with_capacity(known_records.len());
    for record in known_records {
        public_keys_from_record(&record)?;
        let report = member_key_binding_report_from_transaction(
            transaction,
            policy,
            &record.key_id,
            globally_blocked,
        )
        .await?;
        known_bindings.push(report);
    }

    let mut candidate_member_ids = candidate_member_ids.into_iter().collect::<Vec<_>>();
    candidate_member_ids.sort();
    let mut candidate_members = Vec::with_capacity(candidate_member_ids.len());
    for member_id in candidate_member_ids {
        let candidate_report = candidate_member_key_report_from_transaction(
            transaction,
            policy,
            member_id,
            fingerprint,
            globally_blocked,
        )
        .await?;
        candidate_members.push(candidate_report);
    }

    Ok(PublicKeyBundleReport {
        fingerprint,
        schemes: PublicKeyBundleSchemeReport::SUPPORTED,
        globally_blocked,
        known_bindings,
        candidate_members,
    })
}

/// Build a report for one candidate identity supplied by the application.
async fn candidate_member_key_report_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    member_id: MemberIdentity,
    fingerprint: KeyFingerprint,
    globally_blocked: bool,
) -> Result<CandidateMemberKeyReport, SecurityStoreError> {
    let records = transaction
        .load_member_public_keys_for_member(&member_id)
        .await
        .context(StoreAccessSnafu)?;
    let mut binding_for_bundle = None;
    let mut other_known_fingerprints = Vec::new();
    for record in records {
        public_keys_from_record(&record)?;
        if record.key_id.fingerprint == fingerprint {
            let report = member_key_binding_report_from_transaction(
                transaction,
                policy,
                &record.key_id,
                globally_blocked,
            )
            .await?;
            binding_for_bundle = Some(report);
        } else {
            other_known_fingerprints.push(record.key_id.fingerprint);
        }
    }
    Ok(CandidateMemberKeyReport {
        member_id,
        binding_for_bundle,
        other_known_fingerprints,
    })
}

/// Build a local trust and authority report for one exact member-key binding.
async fn member_key_binding_report_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    key_id: &MemberKeyId,
    globally_blocked: bool,
) -> Result<MemberKeyBindingReport, SecurityStoreError> {
    let evidence = transaction
        .load_member_key_trust_evidence(key_id)
        .await
        .context(StoreAccessSnafu)?;
    let trust = MemberKeyTrustReport {
        has_local_explicit_trust: evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust),
    };
    let authority = AuthorityScope::VALUES
        .iter()
        .map(|scope| MemberKeyAuthorityReport {
            scope: *scope,
            decision: request_member_key_permission(
                policy,
                *scope,
                key_id,
                evidence,
                globally_blocked,
            ),
        })
        .collect();
    Ok(MemberKeyBindingReport {
        key_id: key_id.clone(),
        trust,
        authority,
    })
}

/// Store one observed public-key binding for each candidate member identity.
async fn ensure_candidate_public_key_bindings(
    transaction: &mut dyn ReplicationStoreTransaction,
    bundle: &PublicKeyBundle,
    candidate_member_ids: &HashSet<MemberIdentity>,
) -> Result<(), SecurityStoreError> {
    for member_id in candidate_member_ids {
        ensure_candidate_public_key_binding(transaction, bundle, member_id.clone()).await?;
    }
    Ok(())
}

/// Store one observed public-key binding for a candidate member identity.
async fn ensure_candidate_public_key_binding(
    transaction: &mut dyn ReplicationStoreTransaction,
    bundle: &PublicKeyBundle,
    member_id: MemberIdentity,
) -> Result<(), SecurityStoreError> {
    let record = member_public_keys_record_from_bundle(bundle, member_id);
    transaction
        .ensure_member_public_keys(record)
        .await
        .context(StoreAccessSnafu)?;
    Ok(())
}

/// Ensure one directly discovered public-key binding when exact material is still missing.
async fn ensure_discovery_public_key_binding(
    transaction: &mut dyn ReplicationStoreTransaction,
    bundle: &PublicKeyBundle,
    member_id: MemberIdentity,
) -> Result<(), SecurityStoreError> {
    let fingerprint = bundle.fingerprint();
    let key_id = MemberKeyId {
        member_id: member_id.clone(),
        fingerprint,
    };
    let existing = transaction
        .load_member_public_keys(&key_id)
        .await
        .context(StoreAccessSnafu)?;
    if existing.is_some() {
        // The member/fingerprint binding is already present. The fingerprint identifies the
        // public-key material, so an existing exact binding is necessarily identical to the bundle
        // this lookup returned.
        return Ok(());
    }
    let fingerprint_blocked = transaction
        .is_key_fingerprint_blocked(&fingerprint)
        .await
        .context(StoreAccessSnafu)?;
    if fingerprint_blocked {
        return Err(SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
            key_id,
            denial_reasons: vec![PermissionDenialReason::FingerprintBlocked],
        });
    }
    ensure_candidate_public_key_binding(transaction, bundle, member_id).await
}

/// Persist one feedback action in an already-open write transaction.
async fn record_public_key_bundle_feedback_in_transaction(
    transaction: &mut dyn ReplicationStoreTransaction,
    bundle: &PublicKeyBundle,
    feedback: PublicKeyBundleFeedback,
    fingerprint: KeyFingerprint,
) -> Result<(), SecurityStoreError> {
    match feedback {
        PublicKeyBundleFeedback::TrustMember { member_id } => {
            let fingerprint_blocked = transaction
                .is_key_fingerprint_blocked(&fingerprint)
                .await
                .context(StoreAccessSnafu)?;
            ensure!(
                !fingerprint_blocked,
                BlockedKeyTrustSnafu {
                    member_id: member_id.clone(),
                    fingerprint,
                }
            );
            let record = member_public_keys_record_from_bundle(bundle, member_id);
            let key_id = record.key_id.clone();
            transaction
                .ensure_member_public_keys(record)
                .await
                .context(StoreAccessSnafu)?;
            transaction
                .ensure_member_key_trust_evidence(MemberKeyTrustEvidenceRecord {
                    key_id: key_id.clone(),
                    evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
                })
                .await
                .context(StoreAccessSnafu)?;
            Ok(())
        }
        PublicKeyBundleFeedback::BlockFingerprint => {
            transaction
                .ensure_blocked_key_fingerprint(fingerprint)
                .await
                .context(StoreAccessSnafu)?;
            Ok(())
        }
    }
}

/// Build a member-public-keys store record by binding one public bundle to a member.
fn member_public_keys_record_from_bundle(
    bundle: &PublicKeyBundle,
    member_id: MemberIdentity,
) -> MemberPublicKeysRecord {
    let public_keys = bundle.clone().bind_member(member_id);
    MemberPublicKeysRecord::from_public_keys(&public_keys)
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
mod tests {
    use super::*;
    use crate::{
        SqliteReplicationStore,
        api::{
            MemberKeyTrustEvidenceRecord,
            MemberPublicKeysRecord,
            ReplicationStore,
            security::{
                AssessPublicKeyBundleRequest,
                MemberKeyBindingReport,
                PublicKeyBundleAssessmentStorage,
                PublicKeyBundleFeedback,
                PublicKeyBundleReport,
                RecordPublicKeyBundleFeedbackRequest,
            },
        },
        test_support::test_public_member_keys,
    };
    use flotsync_core::member::Identifier;
    use std::{collections::HashSet, time::Duration};

    const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

    fn wait_for_security_store_future<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        flotsync_io::test_support::wait_for_future(
            TEST_WAIT_TIMEOUT,
            future,
            "timed out waiting for security-store future",
        )
    }

    fn local_member() -> MemberIdentity {
        Identifier::from_array(["security", "local"])
    }

    fn remote_member() -> MemberIdentity {
        Identifier::from_array(["security", "remote"])
    }

    fn alternate_member() -> MemberIdentity {
        Identifier::from_array(["security", "alternate"])
    }

    fn sqlite_store() -> Arc<SqliteReplicationStore> {
        Arc::new(SqliteReplicationStore::in_memory(local_member()).expect("store should build"))
    }

    fn security_store(store: Arc<SqliteReplicationStore>) -> SecurityStore {
        security_store_with_policy(store, TrustPolicy::default())
    }

    fn security_store_with_policy(
        store: Arc<SqliteReplicationStore>,
        trust_policy: TrustPolicy,
    ) -> SecurityStore {
        let store: Arc<dyn ReplicationStore> = store;
        SecurityStore::new(store, trust_policy)
    }

    fn provision_member_public_keys(
        store: &dyn ReplicationStore,
        member_id: MemberIdentity,
        source_member: &MemberIdentity,
        trusted: bool,
    ) -> MemberPublicKeysRecord {
        let public_keys = test_public_member_keys(source_member);
        let mut record = MemberPublicKeysRecord::from_public_keys(&public_keys);
        record.key_id.member_id = member_id;
        let mut transaction =
            wait_for_security_store_future(store.begin_transaction()).expect("transaction starts");
        wait_for_security_store_future(transaction.ensure_member_public_keys(record.clone()))
            .expect("member public keys store");
        if trusted {
            wait_for_security_store_future(transaction.ensure_member_key_trust_evidence(
                MemberKeyTrustEvidenceRecord {
                    key_id: record.key_id.clone(),
                    evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
                },
            ))
            .expect("trust evidence stores");
        }
        wait_for_security_store_future(transaction.commit()).expect("transaction commits");
        record
    }

    fn binding_report<'a>(
        report: &'a PublicKeyBundleReport,
        key_id: &MemberKeyId,
    ) -> &'a MemberKeyBindingReport {
        report
            .known_bindings
            .iter()
            .find(|binding| &binding.key_id == key_id)
            .expect("binding report should exist")
    }

    fn authority_decision(
        binding: &MemberKeyBindingReport,
        scope: AuthorityScope,
    ) -> PermissionDecision {
        binding
            .authority
            .iter()
            .find(|authority| authority.scope == scope)
            .expect("authority report should exist")
            .decision
    }

    #[test]
    fn evaluator_requires_local_explicit_trust_by_default() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let evidence = MemberKeyTrustEvidenceSet::empty();

        let decision = request_member_key_permission(
            &TrustPolicy::default(),
            AuthorityScope::ReplicationRuntime,
            &key_id,
            evidence,
            false,
        );

        assert_eq!(
            decision,
            PermissionDecision::Deny(PermissionDenialReason::MissingTrustEvidence)
        );
    }

    #[test]
    fn evaluator_permits_member_route_publication_for_stored_material_by_default() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let evidence = MemberKeyTrustEvidenceSet::empty();

        let decision = request_member_key_permission(
            &TrustPolicy::default(),
            AuthorityScope::MemberRoutePublication,
            &key_id,
            evidence,
            false,
        );

        assert_eq!(decision, PermissionDecision::Permit);
    }

    #[test]
    fn evaluator_denies_blocked_fingerprint_before_positive_evidence() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let mut evidence = MemberKeyTrustEvidenceSet::empty();
        evidence.insert(MemberKeyTrustEvidenceKind::LocalExplicitTrust);

        let decision = request_member_key_permission(
            &TrustPolicy::default(),
            AuthorityScope::ReplicationRuntime,
            &key_id,
            evidence,
            true,
        );

        assert_eq!(
            decision,
            PermissionDecision::Deny(PermissionDenialReason::FingerprintBlocked)
        );
    }

    #[test]
    fn evaluator_denies_local_explicit_trust_when_policy_denies_all() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let mut evidence = MemberKeyTrustEvidenceSet::empty();
        evidence.insert(MemberKeyTrustEvidenceKind::LocalExplicitTrust);
        let policy = TrustPolicy {
            replication_runtime: MemberKeyTrustRequirement::DenyAll,
            bootstrap_activation: MemberKeyTrustRequirement::LocalExplicitTrust,
            member_route_publication: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
        };

        let decision = request_member_key_permission(
            &policy,
            AuthorityScope::ReplicationRuntime,
            &key_id,
            evidence,
            false,
        );

        assert_eq!(
            decision,
            PermissionDecision::Deny(PermissionDenialReason::PolicyDenied)
        );
    }

    #[test]
    fn evaluator_denies_bootstrap_activation_when_policy_denies_all() {
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let mut evidence = MemberKeyTrustEvidenceSet::empty();
        evidence.insert(MemberKeyTrustEvidenceKind::LocalExplicitTrust);
        let policy = TrustPolicy {
            replication_runtime: MemberKeyTrustRequirement::LocalExplicitTrust,
            bootstrap_activation: MemberKeyTrustRequirement::DenyAll,
            member_route_publication: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
        };

        let decision = request_member_key_permission(
            &policy,
            AuthorityScope::BootstrapActivation,
            &key_id,
            evidence,
            false,
        );

        assert_eq!(
            decision,
            PermissionDecision::Deny(PermissionDenialReason::PolicyDenied)
        );
    }

    #[test]
    fn security_store_loads_locally_permitted_member_public_keys() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let security_store = security_store(store);

        let public_keys =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::ReplicationRuntime,
            ))
            .expect("permitted member public keys should load");

        assert_eq!(public_keys.fingerprint(), record.key_id.fingerprint);
    }

    #[test]
    fn security_store_denies_observed_member_public_keys_without_trust_evidence() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), false);
        let security_store = security_store(store);

        let error =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::ReplicationRuntime,
            ))
            .expect_err("observed-only member public keys should not be permitted");

        assert!(matches!(
            error,
            SecurityStoreError::NoPermittedMemberPublicKeys {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::MissingTrustEvidence)
        ));
    }

    #[test]
    fn security_store_exact_key_unavailable_reports_member_key_id() {
        let store = sqlite_store();
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let security_store = security_store(store);

        let error = wait_for_security_store_future(
            security_store.load_member_key_public_keys_if_permitted(
                &key_id,
                AuthorityScope::ReplicationRuntime,
            ),
        )
        .expect_err("missing exact member key should not be permitted");

        assert!(matches!(
            error,
            SecurityStoreError::MemberKeyPublicKeysUnavailable {
                key_id: error_key_id,
                denial_reasons,
                ..
            } if error_key_id == key_id
                && denial_reasons.contains(&PermissionDenialReason::MissingKeyMaterial)
        ));
    }

    #[test]
    fn security_store_loads_exact_member_key_public_keys_without_trust_evidence() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), false);
        let security_store = security_store(store);

        let public_keys =
            wait_for_security_store_future(security_store.load_member_key_public_keys(
                &record.key_id,
                MemberPublicKeyLoadPolicy::RejectBlocked,
            ))
            .expect("stored exact public keys should load without trust evidence");

        assert_eq!(public_keys.fingerprint(), record.key_id.fingerprint);
    }

    #[test]
    fn security_store_exact_member_key_public_keys_unavailable_reports_member_key_id() {
        let store = sqlite_store();
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint: test_public_member_keys(&remote_member()).fingerprint(),
        };
        let security_store = security_store(store);

        let error = wait_for_security_store_future(
            security_store
                .load_member_key_public_keys(&key_id, MemberPublicKeyLoadPolicy::RejectBlocked),
        )
        .expect_err("missing exact public keys should be unavailable");

        assert!(matches!(
            error,
            SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                key_id: error_key_id,
                denial_reasons,
            } if error_key_id == key_id
                && denial_reasons.contains(&PermissionDenialReason::MissingKeyMaterial)
        ));
    }

    #[test]
    fn security_store_denies_globally_blocked_fingerprint() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let mut transaction =
            wait_for_security_store_future(store.begin_transaction()).expect("transaction starts");
        wait_for_security_store_future(
            transaction.ensure_blocked_key_fingerprint(record.key_id.fingerprint),
        )
        .expect("blocked fingerprint stores");
        wait_for_security_store_future(transaction.commit()).expect("transaction commits");
        let security_store = security_store(store);

        let error =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::ReplicationRuntime,
            ))
            .expect_err("blocked fingerprint should not be permitted");

        assert!(matches!(
            error,
            SecurityStoreError::NoPermittedMemberPublicKeys {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::FingerprintBlocked)
        ));
    }

    #[test]
    fn security_store_rejects_blocked_exact_member_key_public_keys() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), false);
        let mut transaction =
            wait_for_security_store_future(store.begin_transaction()).expect("transaction starts");
        wait_for_security_store_future(
            transaction.ensure_blocked_key_fingerprint(record.key_id.fingerprint),
        )
        .expect("blocked fingerprint stores");
        wait_for_security_store_future(transaction.commit()).expect("transaction commits");
        let security_store = security_store(store);

        let public_keys = wait_for_security_store_future(
            security_store.load_member_key_public_keys(
                &record.key_id,
                MemberPublicKeyLoadPolicy::AllowBlocked,
            ),
        )
        .expect("blocked exact public keys should load when the caller allows blocked material");
        assert_eq!(public_keys.fingerprint(), record.key_id.fingerprint);

        let error =
            wait_for_security_store_future(security_store.load_member_key_public_keys(
                &record.key_id,
                MemberPublicKeyLoadPolicy::RejectBlocked,
            ))
            .expect_err("blocked exact public keys should be unavailable when rejected");

        assert!(matches!(
            error,
            SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::FingerprintBlocked)
        ));
    }

    #[test]
    fn direct_discovery_key_bundle_rejects_blocked_fingerprint() {
        let store = sqlite_store();
        let bundle = test_public_member_keys(&remote_member()).public_key_bundle();
        let fingerprint = bundle.fingerprint();
        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint,
        };
        let mut transaction =
            wait_for_security_store_future(store.begin_transaction()).expect("transaction starts");
        wait_for_security_store_future(transaction.ensure_blocked_key_fingerprint(fingerprint))
            .expect("blocked fingerprint stores");
        wait_for_security_store_future(transaction.commit()).expect("transaction commits");
        let security_store = security_store(store.clone());

        let error = wait_for_security_store_future(
            security_store.ensure_discovery_public_key_bundle(&remote_member(), bundle),
        )
        .expect_err("blocked direct discovery material should be rejected");

        assert!(matches!(
            error,
            SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
                key_id: error_key_id,
                denial_reasons,
            } if error_key_id == key_id
                && denial_reasons.contains(&PermissionDenialReason::FingerprintBlocked)
        ));

        let mut transaction =
            wait_for_security_store_future(store.begin_read_transaction()).expect("read starts");
        let stored = wait_for_security_store_future(transaction.load_member_public_keys(&key_id))
            .expect("member keys should load");
        wait_for_security_store_future(transaction.release()).expect("release succeeds");
        assert!(stored.is_none());
    }

    #[test]
    fn public_key_bundle_assessment_does_not_store_or_trust_unknown_bundle() {
        let store = sqlite_store();
        let security_store = security_store(store.clone());
        let bundle = test_public_member_keys(&remote_member()).public_key_bundle();

        let report = wait_for_security_store_future(security_store.assess_public_key_bundle(
            AssessPublicKeyBundleRequest {
                bundle,
                candidate_member_ids: HashSet::from([remote_member()]),
                material_storage: PublicKeyBundleAssessmentStorage::ReadOnly,
            },
        ))
        .expect("bundle assessment should succeed");

        assert!(report.known_bindings.is_empty());
        assert_eq!(report.candidate_members.len(), 1);
        assert!(report.candidate_members[0].binding_for_bundle.is_none());

        let mut transaction =
            wait_for_security_store_future(store.begin_read_transaction()).expect("read starts");
        let stored = wait_for_security_store_future(
            transaction.load_member_public_keys_for_member(&remote_member()),
        )
        .expect("member keys should load");
        wait_for_security_store_future(transaction.release()).expect("release succeeds");
        assert!(stored.is_empty());
    }

    #[test]
    fn public_key_bundle_assessment_stores_candidate_bindings_without_trust() {
        let store = sqlite_store();
        let security_store = security_store_with_policy(
            store.clone(),
            TrustPolicy {
                replication_runtime: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
                bootstrap_activation: MemberKeyTrustRequirement::LocalExplicitTrust,
                member_route_publication: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
            },
        );
        let bundle = test_public_member_keys(&remote_member()).public_key_bundle();

        let report = wait_for_security_store_future(security_store.assess_public_key_bundle(
            AssessPublicKeyBundleRequest {
                bundle,
                candidate_member_ids: HashSet::from([remote_member(), alternate_member()]),
                material_storage: PublicKeyBundleAssessmentStorage::StoreCandidateBindings,
            },
        ))
        .expect("bundle assessment should store candidate bindings");

        assert_eq!(report.known_bindings.len(), 2);
        for candidate in &report.candidate_members {
            let binding = candidate
                .binding_for_bundle
                .as_ref()
                .expect("candidate binding should be reported");
            assert!(!binding.trust.has_local_explicit_trust);
            assert_eq!(
                authority_decision(binding, AuthorityScope::ReplicationRuntime),
                PermissionDecision::Permit
            );
            assert_eq!(
                authority_decision(binding, AuthorityScope::BootstrapActivation),
                PermissionDecision::Deny(PermissionDenialReason::MissingTrustEvidence)
            );
        }

        let mut transaction =
            wait_for_security_store_future(store.begin_read_transaction()).expect("read starts");
        let stored = wait_for_security_store_future(
            transaction.load_member_public_keys_for_member(&remote_member()),
        )
        .expect("member keys should load");
        wait_for_security_store_future(transaction.release()).expect("release succeeds");
        assert_eq!(stored.len(), 1);
    }

    #[test]
    fn public_key_bundle_assessment_reports_ambiguous_bindings_and_candidate_keys() {
        let store = sqlite_store();
        let trusted_record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let ambiguous_record = provision_member_public_keys(
            store.as_ref(),
            alternate_member(),
            &remote_member(),
            false,
        );
        let other_remote_record = provision_member_public_keys(
            store.as_ref(),
            remote_member(),
            &alternate_member(),
            false,
        );
        let security_store = security_store(store);
        let bundle = test_public_member_keys(&remote_member()).public_key_bundle();

        let report = wait_for_security_store_future(security_store.assess_public_key_bundle(
            AssessPublicKeyBundleRequest {
                bundle,
                candidate_member_ids: HashSet::from([remote_member(), alternate_member()]),
                material_storage: PublicKeyBundleAssessmentStorage::ReadOnly,
            },
        ))
        .expect("bundle assessment should succeed");

        assert_eq!(report.fingerprint, trusted_record.key_id.fingerprint);
        assert!(!report.globally_blocked);
        assert_eq!(report.known_bindings.len(), 2);
        assert!(
            report
                .known_bindings
                .iter()
                .any(|binding| binding.key_id == trusted_record.key_id)
        );
        assert!(
            report
                .known_bindings
                .iter()
                .any(|binding| binding.key_id == ambiguous_record.key_id)
        );
        let trusted_binding = binding_report(&report, &trusted_record.key_id);
        assert!(trusted_binding.trust.has_local_explicit_trust);
        assert_eq!(
            authority_decision(trusted_binding, AuthorityScope::ReplicationRuntime),
            PermissionDecision::Permit
        );

        let remote_candidate = report
            .candidate_members
            .iter()
            .find(|candidate| candidate.member_id == remote_member())
            .expect("remote candidate should be reported");
        assert_eq!(
            remote_candidate
                .binding_for_bundle
                .as_ref()
                .expect("remote binding should be reported")
                .key_id,
            trusted_record.key_id
        );
        assert_eq!(
            remote_candidate.other_known_fingerprints,
            vec![other_remote_record.key_id.fingerprint]
        );
    }

    #[test]
    fn public_key_bundle_feedback_trusts_member() {
        let store = sqlite_store();
        let security_store = security_store(store.clone());
        let bundle = test_public_member_keys(&remote_member()).public_key_bundle();
        let fingerprint = bundle.fingerprint();

        wait_for_security_store_future(security_store.record_public_key_bundle_feedback(
            RecordPublicKeyBundleFeedbackRequest {
                bundle: bundle.clone(),
                feedback: PublicKeyBundleFeedback::TrustMember {
                    member_id: remote_member(),
                },
            },
        ))
        .expect("trust feedback should store");

        let key_id = MemberKeyId {
            member_id: remote_member(),
            fingerprint,
        };
        let report = wait_for_security_store_future(security_store.assess_public_key_bundle(
            AssessPublicKeyBundleRequest {
                bundle,
                candidate_member_ids: HashSet::from([remote_member()]),
                material_storage: PublicKeyBundleAssessmentStorage::ReadOnly,
            },
        ))
        .expect("bundle assessment should succeed");
        let binding = binding_report(&report, &key_id);
        assert!(binding.trust.has_local_explicit_trust);
        assert_eq!(
            authority_decision(binding, AuthorityScope::ReplicationRuntime),
            PermissionDecision::Permit
        );

        let mut transaction =
            wait_for_security_store_future(store.begin_read_transaction()).expect("read starts");
        let evidence =
            wait_for_security_store_future(transaction.load_member_key_trust_evidence(&key_id))
                .expect("trust evidence should load");
        wait_for_security_store_future(transaction.release()).expect("release succeeds");
        assert!(evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust));
    }

    #[test]
    fn public_key_bundle_feedback_blocks_fingerprint() {
        let store = sqlite_store();
        let security_store = security_store(store);
        let bundle = test_public_member_keys(&remote_member()).public_key_bundle();
        let fingerprint = bundle.fingerprint();
        wait_for_security_store_future(security_store.record_public_key_bundle_feedback(
            RecordPublicKeyBundleFeedbackRequest {
                bundle: bundle.clone(),
                feedback: PublicKeyBundleFeedback::TrustMember {
                    member_id: remote_member(),
                },
            },
        ))
        .expect("trust feedback should store");

        wait_for_security_store_future(security_store.record_public_key_bundle_feedback(
            RecordPublicKeyBundleFeedbackRequest {
                bundle: bundle.clone(),
                feedback: PublicKeyBundleFeedback::BlockFingerprint,
            },
        ))
        .expect("block feedback should store");

        let report = wait_for_security_store_future(security_store.assess_public_key_bundle(
            AssessPublicKeyBundleRequest {
                bundle,
                candidate_member_ids: HashSet::from([remote_member()]),
                material_storage: PublicKeyBundleAssessmentStorage::ReadOnly,
            },
        ))
        .expect("bundle assessment should succeed");
        assert!(report.globally_blocked);
        assert_eq!(report.fingerprint, fingerprint);
        let binding = &report.known_bindings[0];
        assert_eq!(
            authority_decision(binding, AuthorityScope::ReplicationRuntime),
            PermissionDecision::Deny(PermissionDenialReason::FingerprintBlocked)
        );
    }

    #[test]
    fn public_key_bundle_feedback_rejects_trust_for_blocked_fingerprint() {
        let store = sqlite_store();
        let security_store = security_store(store);
        let bundle = test_public_member_keys(&remote_member()).public_key_bundle();
        let fingerprint = bundle.fingerprint();
        wait_for_security_store_future(security_store.record_public_key_bundle_feedback(
            RecordPublicKeyBundleFeedbackRequest {
                bundle: bundle.clone(),
                feedback: PublicKeyBundleFeedback::BlockFingerprint,
            },
        ))
        .expect("block feedback should store");

        let error =
            wait_for_security_store_future(security_store.record_public_key_bundle_feedback(
                RecordPublicKeyBundleFeedbackRequest {
                    bundle,
                    feedback: PublicKeyBundleFeedback::TrustMember {
                        member_id: remote_member(),
                    },
                },
            ))
            .expect_err("trust feedback should reject blocked key material");

        assert!(matches!(
            error,
            SecurityStoreError::BlockedKeyTrust {
                member_id,
                fingerprint: error_fingerprint,
            } if member_id == remote_member() && error_fingerprint == fingerprint
        ));
    }

    #[test]
    fn security_store_denies_local_explicit_trust_when_policy_denies_all() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let security_store = security_store_with_policy(
            store,
            TrustPolicy {
                replication_runtime: MemberKeyTrustRequirement::DenyAll,
                bootstrap_activation: MemberKeyTrustRequirement::LocalExplicitTrust,
                member_route_publication: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
            },
        );

        let error =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::ReplicationRuntime,
            ))
            .expect_err("policy should deny explicitly trusted member public keys");

        assert!(matches!(
            error,
            SecurityStoreError::NoPermittedMemberPublicKeys {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::PolicyDenied)
        ));
    }

    #[test]
    fn security_store_denies_bootstrap_activation_when_policy_denies_all() {
        let store = sqlite_store();
        let record =
            provision_member_public_keys(store.as_ref(), remote_member(), &remote_member(), true);
        let security_store = security_store_with_policy(
            store,
            TrustPolicy {
                replication_runtime: MemberKeyTrustRequirement::LocalExplicitTrust,
                bootstrap_activation: MemberKeyTrustRequirement::DenyAll,
                member_route_publication: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
            },
        );

        let error =
            wait_for_security_store_future(security_store.load_permitted_member_public_keys(
                &record.key_id.member_id,
                AuthorityScope::BootstrapActivation,
            ))
            .expect_err("policy should deny bootstrap activation for explicitly trusted keys");

        assert!(matches!(
            error,
            SecurityStoreError::NoPermittedMemberPublicKeys {
                denial_reasons,
                ..
            } if denial_reasons.contains(&PermissionDenialReason::PolicyDenied)
        ));
    }

    #[test]
    fn security_store_rejects_ambiguous_permitted_member_keys() {
        let store = sqlite_store();
        let member_id = remote_member();
        provision_member_public_keys(store.as_ref(), member_id.clone(), &remote_member(), true);
        provision_member_public_keys(store.as_ref(), member_id.clone(), &alternate_member(), true);
        let security_store = security_store(store);

        let error = wait_for_security_store_future(
            security_store
                .load_permitted_member_public_keys(&member_id, AuthorityScope::ReplicationRuntime),
        )
        .expect_err("multiple permitted member keys should be ambiguous");

        assert!(matches!(
            error,
            SecurityStoreError::AmbiguousPermittedMemberPublicKeys {
                member_id: error_member,
                permitted_count: 2,
                ..
            } if error_member == member_id
        ));
    }
}

//! Trust-policy permission evaluation for the security store.

use super::*;

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
pub(super) async fn request_member_key_permission_from_transaction(
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
pub(super) async fn request_stored_member_key_permission_from_transaction(
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

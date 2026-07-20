//! Tests for policy evaluation and authority-scope permission decisions.

use super::{fixtures::*, *};

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

    let error = wait_for_security_store_future(security_store.load_permitted_member_public_keys(
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

    let error = wait_for_security_store_future(security_store.load_permitted_member_public_keys(
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

//! Tests for public-key bundle assessment and feedback persistence.

use super::{fixtures::*, *};

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
    let ambiguous_record =
        provision_member_public_keys(store.as_ref(), alternate_member(), &remote_member(), false);
    let other_remote_record =
        provision_member_public_keys(store.as_ref(), remote_member(), &alternate_member(), false);
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

    let error = wait_for_security_store_future(security_store.record_public_key_bundle_feedback(
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

//! Tests for loading and rejecting member public-key material.

use super::{fixtures::*, *};

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

    let error = wait_for_security_store_future(security_store.load_permitted_member_public_keys(
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
        security_store
            .load_member_key_public_keys_if_permitted(&key_id, AuthorityScope::ReplicationRuntime),
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

    let public_keys = wait_for_security_store_future(
        security_store
            .load_member_key_public_keys(&record.key_id, MemberPublicKeyLoadPolicy::RejectBlocked),
    )
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

    let error = wait_for_security_store_future(security_store.load_permitted_member_public_keys(
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
        security_store
            .load_member_key_public_keys(&record.key_id, MemberPublicKeyLoadPolicy::AllowBlocked),
    )
    .expect("blocked exact public keys should load when the caller allows blocked material");
    assert_eq!(public_keys.fingerprint(), record.key_id.fingerprint);

    let error = wait_for_security_store_future(
        security_store
            .load_member_key_public_keys(&record.key_id, MemberPublicKeyLoadPolicy::RejectBlocked),
    )
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

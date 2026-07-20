//! Group setup and security-bootstrap scenarios.

use super::*;

#[test]
fn create_group_rejects_missing_permitted_keys_without_storing_group() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let runtime = load_runtime_with_parts(
        app_alice_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );

    let error = wait_for_test_reply(runtime.create_group(CreateGroupRequest {
        members: vec![alice_member, bob_member.clone()],
        group_schema: GroupSchema::default(),
    }))
    .expect_err("missing permitted keys should reject group creation");

    match error {
        ApiError::ApiExternal { source } => match source.downcast_ref::<CreateGroupError>() {
            Some(CreateGroupError::Security { source }) => {
                assert!(
                    matches!(
                        source.downcast_ref::<DeliverySecurityError>(),
                        Some(DeliverySecurityError::SecurityStore {
                            source: SecurityStoreError::NoPermittedMemberPublicKeys {
                                member_id,
                                ..
                            },
                        })
                            if member_id == &bob_member
                    ),
                    "unexpected security source: {source:?}"
                );
            }
            other => panic!("unexpected create-group error source: {other:?}"),
        },
        other => panic!("unexpected API error: {other:?}"),
    }
    assert!(load_persisted_groups(store.as_ref()).is_empty());
}

#[test]
fn bootstrap_payload_validation_rejects_unpermitted_sender_fingerprint() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let store = sqlite_store(bob_member.clone());
    provision_test_security(store.as_ref(), &bob_member, []);
    provision_test_security(store.as_ref(), &bob_member, [alice_member.clone()]);
    let security = load_test_runtime_security(store.clone(), &bob_member);
    let bob_keys = test_public_keys(&bob_member);
    let probe_keys = test_public_keys(&Identifier::from_array(["probe", "laptop"]));
    let mismatched_alice_key =
        BootstrapMemberKeyMessage::from_fingerprint(probe_keys.fingerprint());
    let payload = GroupSetupMessage::new(
        vec![alice_member.clone(), bob_member.clone()],
        bootstrap_member_keys([
            (alice_member.clone(), mismatched_alice_key),
            bootstrap_member_key(&bob_keys),
        ]),
        GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
        GroupSetupKey::from_bytes([70; 32]),
    )
    .expect("bootstrap payload should build");

    let err =
        wait_for_test_reply(security.validate_group_setup_member_keys(&payload, &alice_member))
            .expect_err("mismatched bootstrap keys should reject payload");

    match err {
        DeliverySecurityError::MemberKeyPermissionDenied {
            member_id,
            fingerprint,
            authority_scope: AuthorityScope::BootstrapActivation,
            reason,
        } => {
            assert_eq!(member_id, alice_member);
            assert_eq!(fingerprint, probe_keys.fingerprint());
            assert_eq!(reason, PermissionDenialReason::MissingKeyMaterial);
        }
        other => panic!("unexpected bootstrap validation error: {other:?}"),
    }
}

#[test]
fn bootstrap_payload_validation_accepts_advertised_sender_fingerprint_when_multiple_keys_are_permitted()
 {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let store = sqlite_store(bob_member.clone());
    provision_test_security(store.as_ref(), &bob_member, [alice_member.clone()]);
    let alternate_alice_source = test_public_keys(&Identifier::from_array(["alice", "phone"]));
    let alternate_alice_keys = PublicMemberKeys::from_key_bytes(
        alice_member.clone(),
        alternate_alice_source.signing_key_bytes(),
        alternate_alice_source.encryption_key_bytes(),
    )
    .expect("alternate alice public keys should build");
    let alternate_alice_record = MemberPublicKeysRecord::from_public_keys(&alternate_alice_keys);
    let alternate_alice_key_id = alternate_alice_record.key_id.clone();
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    wait_for_test_reply(transaction.ensure_member_public_keys(alternate_alice_record))
        .expect("alternate alice public keys should store");
    wait_for_test_reply(transaction.ensure_member_key_trust_evidence(
        MemberKeyTrustEvidenceRecord {
            key_id: alternate_alice_key_id,
            evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
        },
    ))
    .expect("alternate alice trust evidence should store");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
    let security = load_test_runtime_security(store, &bob_member);
    let bob_keys = test_public_keys(&bob_member);
    let payload = GroupSetupMessage::new(
        vec![alice_member.clone(), bob_member.clone()],
        bootstrap_member_keys([
            bootstrap_member_key(&alternate_alice_keys),
            bootstrap_member_key(&bob_keys),
        ]),
        GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
        GroupSetupKey::from_bytes([73; 32]),
    )
    .expect("bootstrap payload should build");

    wait_for_test_reply(security.validate_group_setup_member_keys(&payload, &alice_member))
        .expect("advertised permitted sender key should validate");
}

#[test]
fn bootstrap_payload_validation_rejects_sender_without_bootstrap_activation_permission() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let store = sqlite_store(bob_member.clone());
    provision_test_security(store.as_ref(), &bob_member, []);
    provision_test_security(store.as_ref(), &bob_member, [alice_member.clone()]);
    let store_for_security: Arc<dyn ReplicationStore> = store;
    let security_secrets = test_replication_security_secrets();
    let policy = TrustPolicy {
        replication_runtime: MemberKeyTrustRequirement::LocalExplicitTrust,
        bootstrap_activation: MemberKeyTrustRequirement::DenyAll,
        member_route_publication: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
    };
    let security = wait_for_test_reply(DeliverySecurity::load(
        SecurityStore::new(store_for_security, policy),
        &bob_member,
        Arc::clone(security_secrets.store_secret_key()),
        *security_secrets.store_secret_key_id(),
    ))
    .expect("runtime security state should load");
    let alice_keys = test_public_keys(&alice_member);
    let bob_keys = test_public_keys(&bob_member);
    let payload = GroupSetupMessage::new(
        vec![alice_member.clone(), bob_member.clone()],
        bootstrap_member_keys([
            bootstrap_member_key(&alice_keys),
            bootstrap_member_key(&bob_keys),
        ]),
        GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
        GroupSetupKey::from_bytes([71; 32]),
    )
    .expect("bootstrap payload should build");

    let err =
        wait_for_test_reply(security.validate_group_setup_member_keys(&payload, &alice_member))
            .expect_err("bootstrap activation permission should reject payload");

    match err {
        DeliverySecurityError::MemberKeyPermissionDenied {
            member_id,
            fingerprint,
            authority_scope: AuthorityScope::BootstrapActivation,
            reason,
        } => {
            assert_eq!(member_id, alice_member);
            assert_eq!(fingerprint, alice_keys.fingerprint());
            assert_eq!(reason, PermissionDenialReason::PolicyDenied);
        }
        other => panic!("unexpected bootstrap validation error: {other:?}"),
    }
}

#[test]
fn bootstrap_prepare_stores_inline_unknown_keys_without_trust_evidence() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let charlie_member = Identifier::from_array(["charlie", "laptop"]);
    let store = sqlite_store(bob_member.clone());
    provision_test_security(store.as_ref(), &bob_member, [alice_member.clone()]);
    let security = load_test_runtime_security(store.clone(), &bob_member);
    let alice_keys = test_public_keys(&alice_member);
    let bob_keys = test_public_keys(&bob_member);
    let charlie_keys = test_public_keys(&charlie_member);
    let charlie_record = MemberPublicKeysRecord::from_public_keys(&charlie_keys);
    let group_id = GroupId(Uuid::from_u128(70_003));
    let payload = GroupSetupMessage::new(
        vec![
            alice_member.clone(),
            bob_member.clone(),
            charlie_member.clone(),
        ],
        bootstrap_member_keys([
            bootstrap_member_key(&alice_keys),
            bootstrap_member_key(&bob_keys),
            bootstrap_member_key(&charlie_keys),
        ]),
        GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
        GroupSetupKey::from_bytes([72; 32]),
    )
    .expect("bootstrap payload should build");

    wait_for_test_reply(security.prepare_security_material_from_group_setup(
        group_id,
        &payload,
        &alice_member,
    ))
    .expect("bootstrap security material should prepare");

    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    let loaded = wait_for_test_reply(transaction.load_member_public_keys(&charlie_record.key_id))
        .expect("observed charlie keys should load");
    let evidence =
        wait_for_test_reply(transaction.load_member_key_trust_evidence(&charlie_record.key_id))
            .expect("trust evidence should load");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
    assert_eq!(loaded, Some(charlie_record));
    assert!(!evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust));
}

#[test]
fn bootstrap_preparation_elides_inline_bundles_above_configured_limit() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let charlie_member = Identifier::from_array(["charlie", "laptop"]);
    let store = sqlite_store(alice_member.clone());
    provision_test_security(
        store.as_ref(),
        &alice_member,
        [bob_member.clone(), charlie_member.clone()],
    );
    let security = load_test_runtime_security(store, &alice_member);
    let members = GroupMembers::from_ordered_members(vec![
        alice_member.clone(),
        bob_member.clone(),
        charlie_member.clone(),
    ])
    .expect("group members should build");

    let prepared = wait_for_test_reply(ReplicationRuntimeComponent::prepare_group_setup(
        &security,
        2,
        GroupId(Uuid::from_u128(70_004)),
        &members,
    ))
    .expect("bootstrap should prepare");

    assert!(
        prepared
            .group_setup()
            .member_keys()
            .owned_entries()
            .all(|(_, member_key)| member_key.public_keys().is_none())
    );
}

#[test]
fn bootstrap_preparation_inlines_bundles_at_configured_limit() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, [bob_member.clone()]);
    let security = load_test_runtime_security(store, &alice_member);
    let members = GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
        .expect("group members should build");

    let prepared = wait_for_test_reply(ReplicationRuntimeComponent::prepare_group_setup(
        &security,
        2,
        GroupId(Uuid::from_u128(70_005)),
        &members,
    ))
    .expect("bootstrap should prepare");

    assert!(
        prepared
            .group_setup()
            .member_keys()
            .owned_entries()
            .all(|(_, member_key)| member_key.public_keys().is_some())
    );
}

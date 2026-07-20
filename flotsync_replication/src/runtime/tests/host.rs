//! Runtime-host and runtime-startup scenarios.

use super::*;

#[test]
fn delivery_runtime_host_updates_shared_group_memberships() {
    let local_member = Identifier::from_array(ALICE_MEMBER_SEGMENTS);
    let mut host = start_host(&local_member);
    let group_id = GroupId(Uuid::from_u128(1));
    let memberships = GroupMemberships::from_groups([(
        group_id,
        GroupMembers::singleton(local_member).expect("group should build"),
    )]);

    host.replace_group_memberships(memberships);

    assert!(host.membership_snapshot().contains_group(&group_id));
    wait_for_test_future(host.shutdown()).expect("host should shut down cleanly");
}

#[test]
fn load_replication_runtime_accepts_store_provisioned_security() {
    let runtime_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let application_id = app_probe_id();
    let store = sqlite_store(alice_member());
    let security = setup_api_test_security_secrets();
    let expected_public_bundle =
        provision_runtime_security_through_setup_api(store.as_ref(), &alice_member(), &security);
    let listener = Arc::new(ListenerStub::default());
    let runtime_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(0));

    let loaded_runtime = wait_for_test_reply(load_replication_runtime_with_runtime_config_toml(
        application_id,
        store,
        listener,
        ReplicationConfig::default(),
        security,
        &runtime_config_toml,
    ))
    .expect("public runtime loading should accept provisioned security");
    let loaded_public_bundle = wait_for_test_reply(loaded_runtime.local_public_key_bundle())
        .expect("runtime should expose setup-provisioned public keys");

    assert_eq!(loaded_public_bundle, expected_public_bundle);
}

#[test]
fn runtime_shutdown_is_graceful_idempotent_and_marks_runtime_unavailable() {
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store, listener);

    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down gracefully");
    wait_for_test_reply(runtime.shutdown()).expect("second shutdown should be a no-op");

    let error = wait_for_test_reply(runtime.local_public_key_bundle())
        .expect_err("runtime API should be unavailable after shutdown");
    assert!(matches!(error, ApiError::RuntimeUnavailable));
}

#[test]
fn dropping_runtime_inside_test_executor_does_not_reenter_local_pool() {
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store, listener);

    wait_for_test_future(async move {
        drop(runtime);
    });
}

#[test]
fn replication_security_secrets_load_or_create_reuses_local_profile() {
    install_local_store_secret_test_store().expect("test local secret store should install");
    let application_id = app_probe_id();
    let profile = LocalStoreSecretProfile::new(format!("runtime-profile-{}", Uuid::new_v4()))
        .expect("profile should build");

    let created = ReplicationSecuritySecrets::load_or_create_local(&application_id, &profile)
        .expect("first load should create local store secret");
    let loaded = ReplicationSecuritySecrets::load_or_create_local(&application_id, &profile)
        .expect("second load should reuse local store secret");

    assert_eq!(created.store_secret_key_id(), loaded.store_secret_key_id());
}

#[test]
fn load_replication_runtime_rejects_missing_local_private_keys() {
    let application_id = app_probe_id();
    let store = sqlite_store(alice_member());
    let listener = Arc::new(ListenerStub::default());

    let loaded_runtime = wait_for_test_reply(load_replication_runtime(
        application_id.clone(),
        store,
        listener,
        ReplicationConfig::default(),
        test_replication_security_secrets(),
    ));
    let Err(error) = loaded_runtime else {
        panic!("public runtime loading should reject missing security provisioning");
    };

    let error = security_load_error(error, &application_id);
    assert!(matches!(
        &error,
        LoadSecurityError::MissingLocalPrivateKeys { member_id }
            if member_id == &alice_member()
    ));
}

#[test]
fn load_replication_runtime_rejects_wrong_store_secret_key() {
    let application_id = app_probe_id();
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let listener = Arc::new(ListenerStub::default());
    let test_security = test_replication_security_secrets();
    let wrong_security = ReplicationSecuritySecrets::new(
        *test_security.store_secret_key_id(),
        Arc::new(StoreSecretKey::from_bytes([42; 32])),
    );

    let loaded_runtime = wait_for_test_reply(load_replication_runtime(
        application_id.clone(),
        store,
        listener,
        ReplicationConfig::default(),
        wrong_security,
    ));
    let Err(error) = loaded_runtime else {
        panic!("public runtime loading should reject wrong store-secret key");
    };

    let error = security_load_error(error, &application_id);
    assert!(matches!(
        &error,
        LoadSecurityError::InvalidLocalPrivateKeys { member_id, .. }
            if member_id == &alice_member()
    ));
}

#[test]
fn load_replication_runtime_rejects_stored_group_security_key_id_mismatch() {
    let application_id = app_probe_id();
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let group_id = GroupId(Uuid::from_u128(50_402));
    persist_alice_group_with_security_material(
        store.as_ref(),
        group_id,
        current_slice_placeholder_group_security_material(group_id),
    );
    let listener = Arc::new(ListenerStub::default());

    let loaded_runtime = wait_for_test_reply(load_replication_runtime(
        application_id.clone(),
        store,
        listener,
        ReplicationConfig::default(),
        test_replication_security_secrets(),
    ));
    let Err(error) = loaded_runtime else {
        panic!("public runtime loading should reject group security key-id mismatch");
    };

    let error = security_load_error(error, &application_id);
    assert!(matches!(
        &error,
        LoadSecurityError::StoredGroupKeyIdMismatch {
            group_id: error_group_id,
            ..
        } if error_group_id == &group_id
    ));
}

#[test]
fn load_replication_runtime_rejects_unsupported_stored_group_security_version() {
    let application_id = app_probe_id();
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let group_id = GroupId(Uuid::from_u128(50_403));
    let store_secret_key_id = *test_replication_security_secrets().store_secret_key_id();
    let mut security_material = current_slice_placeholder_group_security_material_with_key_id(
        group_id,
        store_secret_key_id,
    );
    security_material.encrypted_group_secret.crypto_version = StoreSecretCryptoVersion::new(999);
    persist_alice_group_with_security_material(store.as_ref(), group_id, security_material);
    let listener = Arc::new(ListenerStub::default());

    let loaded_runtime = wait_for_test_reply(load_replication_runtime(
        application_id.clone(),
        store,
        listener,
        ReplicationConfig::default(),
        test_replication_security_secrets(),
    ));
    let Err(error) = loaded_runtime else {
        panic!("public runtime loading should reject unsupported group security version");
    };

    let error = security_load_error(error, &application_id);
    assert!(matches!(
        &error,
        LoadSecurityError::StoredGroupUnsupportedStoreSecretVersion {
            group_id: error_group_id,
            version: 999,
            supported: _,
        } if error_group_id == &group_id
    ));
}

#[test]
fn load_replication_runtime_rejects_invalid_stored_group_security_nonce_length() {
    let application_id = app_probe_id();
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let group_id = GroupId(Uuid::from_u128(50_404));
    let store_secret_key_id = *test_replication_security_secrets().store_secret_key_id();
    let mut security_material = current_slice_placeholder_group_security_material_with_key_id(
        group_id,
        store_secret_key_id,
    );
    security_material.encrypted_group_secret.nonce = vec![7].into_boxed_slice();
    persist_alice_group_with_security_material(store.as_ref(), group_id, security_material);
    let listener = Arc::new(ListenerStub::default());

    let loaded_runtime = wait_for_test_reply(load_replication_runtime(
        application_id.clone(),
        store,
        listener,
        ReplicationConfig::default(),
        test_replication_security_secrets(),
    ));
    let Err(error) = loaded_runtime else {
        panic!("public runtime loading should reject invalid group security nonce length");
    };

    let error = security_load_error(error, &application_id);
    assert!(matches!(
        &error,
        LoadSecurityError::StoredGroupInvalidGroupSecretNonceLength {
            group_id: error_group_id,
            actual: 1,
            ..
        } if error_group_id == &group_id
    ));
}

#[test]
fn load_replication_runtime_allows_unresolved_member_keys_for_stored_groups() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(50_401));
    let store_secret_key_id = *test_replication_security_secrets().store_secret_key_id();
    let member_keys = test_group_member_keys(vec![alice_member.clone(), bob_member]);
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            member_keys: member_keys.clone(),
            local_member_index: MemberIndex::new(0),
            group_schema: GroupSchema::default(),
            version_vector: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material_with_key_id(
                group_id,
                store_secret_key_id,
            ),
        },
    );
    let listener = Arc::new(ListenerStub::default());

    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener);

    wait_for_group_install(&runtime, group_id);
    assert_eq!(
        load_persisted_group(store.as_ref(), group_id).member_keys,
        member_keys
    );
}

#[test]
fn load_replication_runtime_allows_ambiguous_member_keys_when_group_names_exact_key() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, [bob_member.clone()]);
    let alternate_bob_keys =
        MemberPublicKeysRecord::from_public_keys(&test_public_keys(&Identifier::from_array([
            "bob", "phone",
        ])));
    let mut alternate_bob_record = alternate_bob_keys;
    alternate_bob_record.key_id.member_id = bob_member.clone();
    let mut transaction =
        wait_for_test_reply(store.begin_transaction()).expect("transaction should start");
    wait_for_test_reply(transaction.ensure_member_public_keys(alternate_bob_record.clone()))
        .expect("alternate member public keys should store");
    wait_for_test_reply(transaction.ensure_member_key_trust_evidence(
        MemberKeyTrustEvidenceRecord {
            key_id: alternate_bob_record.key_id,
            evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
        },
    ))
    .expect("alternate trust evidence should store");
    wait_for_test_reply(transaction.commit()).expect("transaction should commit");
    let group_id = GroupId(Uuid::from_u128(50_402));
    let store_secret_key_id = *test_replication_security_secrets().store_secret_key_id();
    let member_keys = test_group_member_keys(vec![alice_member.clone(), bob_member]);
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            member_keys: member_keys.clone(),
            local_member_index: MemberIndex::new(0),
            group_schema: GroupSchema::default(),
            version_vector: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material_with_key_id(
                group_id,
                store_secret_key_id,
            ),
        },
    );
    let listener = Arc::new(ListenerStub::default());

    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener);

    wait_for_group_install(&runtime, group_id);
    assert_eq!(
        load_persisted_group(store.as_ref(), group_id).member_keys,
        member_keys
    );
}

#[test]
fn delivery_runtime_host_defaults_to_loopback_local_endpoint_bind_in_tests() {
    let mut host = start_host(&Identifier::from_array(PROBE_MEMBER_SEGMENTS));

    assert!(host.external_udp_bind_addr().ip().is_loopback());
    wait_for_test_future(host.shutdown()).expect("host should shut down cleanly");
}

#[test]
fn runtime_host_treats_static_peer_routes_as_unverified_hints() {
    let remote_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let remote_addr = remote_endpoint_lease.addr(0);
    let bob_member = bob_member();
    let runtime_config_toml = static_peer_route_toml(&bob_member, remote_addr);
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts_and_runtime_config_toml(
        app_alice_id(),
        store,
        listener,
        runtime_config_toml.as_str(),
    );

    assert!(
        !runtime.knows_direct_peer_route_for_test(&bob_member),
        "static route hints must not publish before route establishment verifies them"
    );
}

#[test]
fn runtime_host_verifies_static_route_hint_through_route_establishment() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let group_id = GroupId(Uuid::from_u128(35));
    let members = vec![alice_member.clone(), bob_member.clone()];
    let bob_store = sqlite_store(bob_member.clone());
    provision_test_security(bob_store.as_ref(), &bob_member, [alice_member.clone()]);
    persist_group_membership_for_member(bob_store.as_ref(), group_id, members.clone(), 1);
    let bob_listener = Arc::new(ListenerStub::default());
    let bob_runtime = load_runtime_with_parts(app_bob_id(), bob_store, bob_listener);
    wait_for_group_install(&bob_runtime, group_id);

    let alice_store = sqlite_store(alice_member.clone());
    provision_test_security(alice_store.as_ref(), &alice_member, [bob_member.clone()]);
    persist_group_membership_for_member(alice_store.as_ref(), group_id, members, 0);
    let alice_listener = Arc::new(ListenerStub::default());
    let runtime_config_toml = static_peer_route_toml(
        &bob_member,
        bob_runtime.advertised_loopback_udp_addr_for_test(),
    );
    let alice_runtime = load_runtime_with_parts_and_runtime_config_toml(
        app_alice_id(),
        alice_store,
        alice_listener,
        runtime_config_toml.as_str(),
    );
    wait_for_group_install(&alice_runtime, group_id);

    alice_runtime.wait_for_direct_peer_route_for_test(&bob_member);
}

#[test]
fn runtime_host_can_publish_static_peer_routes_manually_in_tests() {
    let remote_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let remote_addr = remote_endpoint_lease.addr(0);
    let alice_member = alice_member();
    let bob_member = bob_member();
    let runtime_config_toml = static_peer_route_toml(&bob_member, remote_addr);
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, [bob_member.clone()]);
    let security = load_test_runtime_security(store.clone(), &alice_member);
    let listener = Arc::new(ListenerStub::default());
    let mut host =
        kompact::prelude::block_on(DeliveryRuntimeHost::start_with_route_publish_mode_for_test(
            &alice_member,
            store,
            listener,
            ReplicationConfig::default(),
            security,
            Some(runtime_config_toml.as_str()),
            PreconfiguredPeerRoutesPublishMode::ManualForTest,
        ))
        .expect("host should start");
    host.wait_for_runtime_startup();

    host.publish_preconfigured_peer_routes();
    host.wait_for_direct_peer_route(&bob_member);
    wait_for_test_future(host.shutdown()).expect("host should shut down cleanly");
}

#[test]
fn runtime_host_treats_zero_catch_up_batch_size_as_unlimited() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let security = load_test_runtime_security(store.clone(), &alice_member);
    let listener = Arc::new(ListenerStub::default());
    let mut host =
        kompact::prelude::block_on(DeliveryRuntimeHost::start_with_route_publish_mode_for_test(
            &alice_member,
            store,
            listener,
            ReplicationConfig::default(),
            security,
            Some(
                r"
            [flotsync.replication.runtime.catch-up]
            max-updates-per-batch = 0
            ",
            ),
            PreconfiguredPeerRoutesPublishMode::ManualForTest,
        ))
        .expect("zero catch-up batch size should mean unlimited");
    host.wait_for_runtime_startup();
    wait_for_test_future(host.shutdown()).expect("host should shut down cleanly");
}

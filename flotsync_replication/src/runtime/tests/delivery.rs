//! Inbound delivery, causal buffering, and retry scenarios.

use super::*;

#[test]
fn pending_apply_need_retries_after_route_appears() {
    let _runtime_endpoint_leases =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let dataset_id = docs_dataset_id();
    let (alice_fixture, bob_fixture) = load_title_runtime_pair_with_trust(&dataset_id);
    let alice_member = alice_fixture.local_member.clone();
    let bob_member = bob_fixture.local_member.clone();
    let alice_runtime = &alice_fixture.runtime;
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(50_201));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    alice_runtime
        .install_group_for_test(group_id, members.clone())
        .expect("alice group should install");
    bob_runtime
        .install_group_for_test(group_id, members)
        .expect("bob group should install");

    let first_row_id = test_row_id(group_id, dataset_id.clone(), 50_211);
    let second_row_id = test_row_id(group_id, dataset_id.clone(), 50_212);
    let first_read_token =
        snapshot_read_token(alice_runtime.as_ref(), group_id, dataset_id.clone());
    let first_receipt = publish_changes(
        alice_runtime.as_ref(),
        first_read_token,
        vec![RowMutation::Upsert {
            row_id: first_row_id.clone(),
            row: crate::row_values! {
                "title" => "pending predecessor",
            },
        }],
    );
    alice_runtime.publish_direct_peer_route_for_test(
        bob_member.clone(),
        bob_runtime.advertised_loopback_udp_addr_for_test(),
    );
    alice_runtime.wait_for_direct_peer_route_for_test(&bob_member);
    let second_receipt = publish_changes(
        alice_runtime.as_ref(),
        first_receipt.read_token,
        vec![RowMutation::Upsert {
            row_id: second_row_id.clone(),
            row: crate::row_values! {
                "title" => "pending successor",
            },
        }],
    );
    assert_eq!(
        second_receipt.update_id,
        UpdateId {
            node_index: 0,
            version: 2,
        }
    );
    eventually(
        TEST_WAIT_TIMEOUT,
        || {
            load_persisted_update(
                bob_fixture.store.as_ref(),
                group_id,
                second_receipt.update_id,
            )
            .is_some()
        },
        "timed out waiting for successor update to persist as pending",
    );
    assert!(bob_fixture.listener.captured_data_changes().is_empty());

    bob_runtime.publish_direct_peer_route_for_test(
        alice_member,
        alice_runtime.advertised_loopback_udp_addr_for_test(),
    );
    bob_fixture.listener.wait_for_data_change_count(2);
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: first_row_id,
                    title: "pending predecessor".to_owned(),
                }],
            },
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: second_row_id,
                    title: "pending successor".to_owned(),
                }],
            },
        ]
    );
}

#[test]
fn partial_update_batch_retry_narrows_remaining_need() {
    let _runtime_endpoint_leases =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let alice_listener = Arc::new(ListenerStub::default());
    let alice_store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    provision_test_security(alice_store.as_ref(), &alice_member, [bob_member.clone()]);
    let alice_runtime = load_runtime_with_parts_and_runtime_config_toml(
        app_alice_id(),
        alice_store,
        alice_listener,
        r"
        [flotsync.replication.runtime.catch-up]
        max-updates-per-batch = 1
        ",
    );
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    provision_test_security(
        bob_fixture.store.as_ref(),
        &bob_member,
        [alice_member.clone()],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(50_301));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group members should build");
    alice_runtime
        .install_group_for_test(group_id, members.clone())
        .expect("alice group should install");
    bob_runtime
        .install_group_for_test(group_id, members)
        .expect("bob group should install");

    let first_row_id = test_row_id(group_id, dataset_id.clone(), 50_311);
    let second_row_id = test_row_id(group_id, dataset_id.clone(), 50_312);
    let first_read_token =
        snapshot_read_token(alice_runtime.as_ref(), group_id, dataset_id.clone());
    let first_receipt = publish_changes(
        alice_runtime.as_ref(),
        first_read_token,
        vec![RowMutation::Upsert {
            row_id: first_row_id.clone(),
            row: crate::row_values! {
                "title" => "partial first",
            },
        }],
    );
    publish_changes(
        alice_runtime.as_ref(),
        first_receipt.read_token,
        vec![RowMutation::Upsert {
            row_id: second_row_id.clone(),
            row: crate::row_values! {
                "title" => "partial second",
            },
        }],
    );
    publish_direct_peer_routes(&alice_runtime, &alice_member, bob_runtime, &bob_member);
    wait_for_test_reply(bob_runtime.request_summary(SummaryRequest {
        group_id,
        target: alice_member,
    }))
    .expect("summary request should succeed");

    bob_fixture.listener.wait_for_data_change_count(2);
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: first_row_id,
                    title: "partial first".to_owned(),
                }],
            },
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: second_row_id,
                    title: "partial second".to_owned(),
                }],
            },
        ]
    );
}

#[test]
fn update_batch_forwarded_by_non_producer_member_applies() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let probe_member = Identifier::from_array(PROBE_MEMBER_SEGMENTS);
    let dataset_id = docs_dataset_id();
    let probe_fixture = load_runtime_fixture(
        app_probe_id(),
        probe_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let probe_runtime = &probe_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(50_401));
    probe_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![
                alice_member.clone(),
                bob_member.clone(),
                probe_member,
            ])
            .expect("group members should build"),
        )
        .expect("probe group should install");
    let member_count = NonZeroUsize::new(3).expect("group has three members");
    let (row_id, update) = title_update_message(
        group_id,
        dataset_id,
        50_411,
        "forwarded by bob",
        UpdateId {
            node_index: 0,
            version: 1,
        },
        VersionVector::initial(member_count),
    );
    let batch = UpdateBatchMessage {
        group_id,
        updates: vec![update],
    };

    probe_runtime
        .apply_update_batch_for_test(bob_member, batch)
        .expect("non-producer member should be allowed to forward update batch");
    probe_fixture.listener.wait_for_data_change_count(1);
    assert_eq!(
        probe_fixture.listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id,
                title: "forwarded by bob".to_owned(),
            }],
        }]
    );
}

#[test]
fn request_summary_returns_remote_current_version_vector() {
    let _runtime_endpoint_leases =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let alice_fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    provision_test_security(
        alice_fixture.store.as_ref(),
        &alice_member,
        [bob_member.clone()],
    );
    provision_test_security(
        bob_fixture.store.as_ref(),
        &bob_member,
        [alice_member.clone()],
    );
    let alice_runtime = &alice_fixture.runtime;
    let bob_runtime = &bob_fixture.runtime;

    alice_runtime.publish_direct_peer_route_for_test(
        bob_member.clone(),
        bob_runtime.advertised_loopback_udp_addr_for_test(),
    );
    bob_runtime.publish_direct_peer_route_for_test(
        alice_member.clone(),
        alice_runtime.advertised_loopback_udp_addr_for_test(),
    );

    let group_id = wait_for_test_reply(alice_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone(), bob_member.clone()],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    accept_one_creation_invitation(
        &bob_fixture.listener,
        group_id,
        &[alice_member, bob_member.clone()],
    );
    wait_for_group_install(bob_runtime, group_id);

    let summary = wait_for_test_reply(alice_runtime.request_summary(SummaryRequest {
        group_id,
        target: bob_member.clone(),
    }))
    .expect("summary request should succeed");

    assert_eq!(summary.group_id, group_id);
    assert_eq!(summary.responder, bob_member);
    assert_eq!(
        summary.has_versions,
        VersionVector::initial(NonZeroUsize::new(2).expect("two members"))
    );
}

#[test]
fn group_invitation_persists_group_schema() {
    let _runtime_endpoint_leases =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let alice_member = alice_member();
    let bob_member = bob_member();
    let alice_fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        Vec::<(DatasetId, SchemaSource)>::new(),
    );
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        Vec::<(DatasetId, SchemaSource)>::new(),
    );
    provision_test_security(
        alice_fixture.store.as_ref(),
        &alice_member,
        [bob_member.clone()],
    );
    provision_test_security(
        bob_fixture.store.as_ref(),
        &bob_member,
        [alice_member.clone()],
    );
    publish_direct_peer_routes(
        &alice_fixture.runtime,
        &alice_member,
        &bob_fixture.runtime,
        &bob_member,
    );

    let group_schema = docs_group_schema();
    let group_id = wait_for_test_reply(alice_fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone(), bob_member.clone()],
        group_schema: group_schema.clone(),
    }))
    .expect("create_group should succeed");
    accept_one_creation_invitation(&bob_fixture.listener, group_id, &[alice_member, bob_member]);
    wait_for_group_install(&bob_fixture.runtime, group_id);

    let bob_group = load_persisted_group(bob_fixture.store.as_ref(), group_id);
    assert_eq!(bob_group.group_schema, group_schema);
}

#[test]
fn inbound_updates_buffer_until_causal_dependencies_are_met_and_ignore_duplicates() {
    // Causal buffering path:
    // 1. install a two-member group only on Bob,
    // 2. deliver Alice's second update first so it must buffer,
    // 3. deliver the missing first update,
    // 4. assert that Bob drains the buffered update in causal order, and
    // 5. verify a duplicate of the first update is ignored.
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(22));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 23);
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;
    let second_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("second operation should build")
    .expect("second operation should apply")
    .encoded_operation;

    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let first_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: VersionVector::initial(member_count),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let mut second_read_versions = VersionVector::initial(member_count);
    second_read_versions.increment_at(0);
    let second_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: second_read_versions,
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![second_operation],
        }],
    };

    bob_runtime
        .apply_update_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should buffer");
    assert!(bob_fixture.listener.captured_data_changes().is_empty());

    bob_runtime
        .apply_update_for_test(alice_member.clone(), first_message.clone())
        .expect("first update should apply and drain the pending second update");
    bob_fixture.listener.wait_for_data_change_count(2);
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: row_id.clone(),
                    title: "first".to_owned(),
                }],
            },
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: row_id.clone(),
                    title: "second".to_owned(),
                }],
            },
        ]
    );
    bob_runtime
        .apply_update_for_test(alice_member, first_message)
        .expect("duplicate update should be ignored");
    assert_eq!(bob_fixture.listener.captured_data_changes().len(), 2);
}

#[test]
fn migrated_group_updates_follow_read_only_and_closed_application_visibility() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let group_id = GroupId(Uuid::from_u128(50_701));
    fixture
        .runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let successor_group_id = GroupId(Uuid::from_u128(50_702));
    let final_versions = VersionVector::Full(PureVersionVector::from([2, 0]));
    persist_group_lifecycle(
        fixture.store.as_ref(),
        &group_id,
        ReplicationGroupLifecycle::ReadOnly {
            successor_group_id,
            final_versions: final_versions.clone(),
        },
    );

    let row_id = test_row_id(group_id, dataset_id.clone(), 50_703);
    let mut source_dataset = LocalDataset::new(title_schema_static());
    let read_only_update = title_update_message_for_row(
        &mut source_dataset,
        &row_id,
        "read-only update",
        UpdateId {
            version: 1,
            node_index: 0,
        },
        VersionVector::initial(member_count),
    );
    fixture
        .runtime
        .apply_update_for_test(alice_member.clone(), read_only_update)
        .expect("in-cut update should apply");
    fixture.listener.wait_for_data_change_count(1);
    assert!(
        fixture.listener.captured_data_change_read_tokens()[0]
            .group_version(&group_id)
            .is_none()
    );

    persist_group_lifecycle(
        fixture.store.as_ref(),
        &group_id,
        ReplicationGroupLifecycle::Closed {
            successor_group_id,
            final_versions,
        },
    );

    let closed_update = title_update_message_for_row(
        &mut source_dataset,
        &row_id,
        "silent catch-up",
        UpdateId {
            version: 2,
            node_index: 0,
        },
        VersionVector::Full(PureVersionVector::from([1, 0])),
    );
    fixture
        .runtime
        .apply_update_for_test(alice_member.clone(), closed_update)
        .expect("closed in-cut update should apply without a listener event");

    let discarded_update = title_update_message_for_row(
        &mut source_dataset,
        &row_id,
        "discarded",
        UpdateId {
            version: 3,
            node_index: 0,
        },
        VersionVector::Full(PureVersionVector::from([2, 0])),
    );
    fixture
        .runtime
        .apply_update_for_test(alice_member, discarded_update)
        .expect("post-cut update should be ignored without failing delivery");

    assert_eq!(fixture.listener.captured_data_changes().len(), 1);
    assert_eq!(
        load_persisted_group(fixture.store.as_ref(), group_id)
            .version_vector
            .version_at(0),
        2
    );
}

#[test]
fn duplicate_update_batch_delivery_is_ignored() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(22_001));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 22_002);
    let mut source_dataset = LocalDataset::new(schema);
    let operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "batch first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("operation should build")
    .expect("operation should apply")
    .encoded_operation;

    let update = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: VersionVector::initial(NonZeroUsize::new(2).expect("group has two members")),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![operation],
        }],
    };
    let batch = UpdateBatchMessage {
        group_id,
        updates: vec![update],
    };

    bob_runtime
        .apply_update_batch_for_test(alice_member.clone(), batch.clone())
        .expect("first update batch should apply");
    bob_fixture.listener.wait_for_data_change_count(1);
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: row_id.clone(),
                title: "batch first".to_owned(),
            }],
        }]
    );

    bob_runtime
        .apply_update_batch_for_test(alice_member, batch)
        .expect("duplicate update batch should be ignored");
    assert_eq!(bob_fixture.listener.captured_data_changes().len(), 1);
}

#[test]
fn inbound_update_with_out_of_range_producer_index_is_rejected() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(22_101));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let (_, update) = title_update_message(
        group_id,
        dataset_id,
        22_102,
        "invalid producer",
        UpdateId {
            version: 1,
            node_index: 2,
        },
        VersionVector::initial(member_count),
    );

    let error = bob_runtime
        .apply_update_for_test(alice_member, update)
        .expect_err("out-of-range producer index should fail cleanly");
    match error {
        InboundDeliveryError::UpdateProducerIndexNotInGroup { producer_index, .. } => {
            assert_eq!(producer_index, MemberIndex::new(2));
        }
        error => panic!("unexpected inbound update error: {error:?}"),
    }
    assert!(bob_fixture.listener.captured_data_changes().is_empty());
}

#[test]
fn update_batch_failure_after_first_update_keeps_first_notifications() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = &bob_fixture.runtime;
    let group_id = GroupId(Uuid::from_u128(22_201));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let (first_row_id, first_update) = title_update_message(
        group_id,
        dataset_id.clone(),
        22_202,
        "first survives",
        UpdateId {
            version: 1,
            node_index: 0,
        },
        VersionVector::initial(member_count),
    );
    let (_, invalid_update) = title_update_message(
        group_id,
        dataset_id,
        22_203,
        "invalid producer",
        UpdateId {
            version: 1,
            node_index: 2,
        },
        VersionVector::initial(member_count),
    );
    let batch = UpdateBatchMessage {
        group_id,
        updates: vec![first_update, invalid_update],
    };

    let error = bob_runtime
        .apply_update_batch_for_test(alice_member, batch)
        .expect_err("second batch update should fail cleanly");
    match error {
        InboundDeliveryError::UpdateProducerIndexNotInGroup { producer_index, .. } => {
            assert_eq!(producer_index, MemberIndex::new(2));
        }
        error => panic!("unexpected update batch error: {error:?}"),
    }
    assert_eq!(
        bob_fixture.listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: first_row_id,
                title: "first survives".to_owned(),
            }],
        }]
    );
}

#[test]
fn inbound_listener_read_token_preserves_unrelated_hosted_group_progress() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let bob_fixture = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), schema)],
    );
    let bob_runtime = &bob_fixture.runtime;
    let inbound_group_id = GroupId(Uuid::from_u128(23_001));
    let unrelated_group_id = GroupId(Uuid::from_u128(23_002));
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("group should build");
    bob_runtime
        .install_group_for_test(inbound_group_id, members.clone())
        .expect("inbound group should install");
    bob_runtime
        .install_group_for_test(unrelated_group_id, members)
        .expect("unrelated group should install");

    let unrelated_row_id = test_row_id(unrelated_group_id, dataset_id.clone(), 23_010);
    let unrelated_read_token =
        snapshot_read_token(bob_runtime.as_ref(), unrelated_group_id, dataset_id.clone());
    publish_changes(
        bob_runtime.as_ref(),
        unrelated_read_token,
        vec![RowMutation::Upsert {
            row_id: unrelated_row_id,
            row: crate::row_values! { "title" => "local unrelated progress" },
        }],
    );
    bob_fixture.listener.wait_for_data_change_count(1);

    let inbound_row_id = test_row_id(inbound_group_id, dataset_id.clone(), 23_011);
    let mut source_dataset = LocalDataset::new(schema);
    let inbound_operation = apply_local_upsert(
        &mut source_dataset,
        &inbound_row_id,
        crate::row_values! { "title" => "remote inbound progress" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("inbound operation should build")
    .expect("inbound operation should apply")
    .encoded_operation;
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    bob_runtime
        .apply_update_for_test(
            alice_member,
            UpdateMessage {
                group_id: inbound_group_id,
                update_id: UpdateId {
                    version: 1,
                    node_index: 0,
                },
                read_versions: VersionVector::initial(member_count),
                dataset_updates: vec![DatasetUpdateMessage {
                    dataset_id,
                    operations: vec![inbound_operation],
                }],
            },
        )
        .expect("inbound update should apply");
    bob_fixture.listener.wait_for_data_change_count(2);

    let read_tokens = bob_fixture.listener.captured_data_change_read_tokens();
    let inbound_read_token = read_tokens
        .last()
        .expect("inbound event should have a read token");
    let mut expected_inbound_versions = VersionVector::initial(member_count);
    expected_inbound_versions.increment_at(0);
    let mut expected_unrelated_versions = VersionVector::initial(member_count);
    expected_unrelated_versions.increment_at(1);
    assert_eq!(
        inbound_read_token.group_version(&inbound_group_id),
        Some(&expected_inbound_versions)
    );
    assert_eq!(
        inbound_read_token.group_version(&unrelated_group_id),
        Some(&expected_unrelated_versions)
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "This scenario spells out a full conflict timeline; splitting it would hide the causal setup."
)]
fn inbound_update_after_local_delete_updates_tombstone_without_resurrection() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let RuntimeFixture {
        runtime: bob_runtime,
        listener: bob_listener,
        store: bob_store,
        ..
    } = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), schema)],
    );
    let group_id = GroupId(Uuid::from_u128(24));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 25);
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;
    let edit_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("edit operation should build")
    .expect("edit operation should apply")
    .encoded_operation;
    let delete_operation = apply_local_delete(
        &mut source_dataset,
        &row_id,
        UpdateId {
            version: 3,
            node_index: 0,
        },
    )
    .expect("delete operation should build")
    .encoded_operation;

    let member_count = NonZeroUsize::new(2).expect("group has two members");
    bob_runtime
        .apply_update_for_test(
            alice_member.clone(),
            UpdateMessage {
                group_id,
                update_id: UpdateId {
                    version: 1,
                    node_index: 0,
                },
                read_versions: VersionVector::initial(member_count),
                dataset_updates: vec![DatasetUpdateMessage {
                    dataset_id: dataset_id.clone(),
                    operations: vec![first_operation],
                }],
            },
        )
        .expect("first update should apply");
    bob_listener.wait_for_data_change_count(1);
    assert_eq!(
        bob_listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: row_id.clone(),
                title: "first".to_owned(),
            }],
        }]
    );

    let read_token = snapshot_read_token(bob_runtime.as_ref(), group_id, dataset_id.clone());
    publish_changes(
        bob_runtime.as_ref(),
        read_token,
        vec![RowMutation::Delete {
            row_id: row_id.clone(),
        }],
    );
    bob_listener.wait_for_data_change_count(2);
    assert_eq!(
        bob_listener.captured_data_changes()[1],
        CapturedDataChange {
            rows: vec![CapturedRowChange::Delete {
                row_id: row_id.clone(),
            }],
        }
    );
    let deleted_row =
        load_persisted_row_slice(bob_store.as_ref(), group_id, &dataset_id, [row_id.row_key])
            .rows
            .get(&row_id.row_key)
            .cloned()
            .flatten()
            .expect("deleted row should persist as a tombstone");
    assert!(deleted_row.tombstoned);
    drop(bob_runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime =
        load_runtime_with_parts(app_bob_id(), bob_store.clone(), restarted_listener.clone());
    wait_for_group_install(&restarted_runtime, group_id);
    let mut edit_read_versions = VersionVector::initial(member_count);
    edit_read_versions.increment_at(0);
    restarted_runtime
        .apply_update_for_test(
            alice_member.clone(),
            UpdateMessage {
                group_id,
                update_id: UpdateId {
                    version: 2,
                    node_index: 0,
                },
                read_versions: edit_read_versions,
                dataset_updates: vec![DatasetUpdateMessage {
                    dataset_id: dataset_id.clone(),
                    operations: vec![edit_operation],
                }],
            },
        )
        .expect("concurrent edit after local delete should apply to the tombstone");
    assert!(restarted_listener.captured_data_changes().is_empty());

    let edited_tombstone =
        load_persisted_row_slice(bob_store.as_ref(), group_id, &dataset_id, [row_id.row_key])
            .rows
            .get(&row_id.row_key)
            .cloned()
            .flatten()
            .expect("edited tombstone should persist");
    assert!(edited_tombstone.tombstoned);
    let materialised_tombstone =
        flotsync_messages::InMemoryStateData::from_row_snapshots_with_tombstones(
            schema,
            [flotsync_data_types::schema::datamodel::RowRecord {
                row_id: row_id.row_key.0,
                snapshot: edited_tombstone.snapshot,
                tombstoned: edited_tombstone.tombstoned,
            }],
        )
        .expect("tombstone should rehydrate");
    assert_eq!(materialised_tombstone.num_active_rows(), 0);
    let materialised_row = materialised_tombstone
        .get_row(&row_id.row_key.0)
        .expect("tombstoned row should remain addressable");
    let title = materialised_row
        .get_field_value::<str>("title")
        .expect("title should decode");
    assert_eq!(title.as_ref(), "second");

    let mut delete_read_versions = VersionVector::initial(member_count);
    delete_read_versions.increment_at(0);
    delete_read_versions.increment_at(0);
    restarted_runtime
        .apply_update_for_test(
            alice_member,
            UpdateMessage {
                group_id,
                update_id: UpdateId {
                    version: 3,
                    node_index: 0,
                },
                read_versions: delete_read_versions,
                dataset_updates: vec![DatasetUpdateMessage {
                    dataset_id,
                    operations: vec![delete_operation],
                }],
            },
        )
        .expect("delete against an existing tombstone should be idempotent");
    assert!(restarted_listener.captured_data_changes().is_empty());
}

#[test]
fn inbound_update_rejects_operation_change_id_mismatch_before_persisting() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let group_id = GroupId(Uuid::from_u128(26));
    let row_id = test_row_id(group_id, dataset_id.clone(), 27);
    let batch_update_id = UpdateId {
        version: 1,
        node_index: 0,
    };
    let operation_change_id = UpdateId {
        version: 2,
        node_index: 0,
    };
    let mut source_dataset = LocalDataset::new(schema);
    let encoded_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "wrong change id" },
        operation_change_id,
    )
    .expect("operation should build")
    .expect("operation should apply")
    .encoded_operation;
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let update = ReplicationUpdateRecord {
        group_id,
        update_id: batch_update_id,
        sender: alice_member(),
        read_versions: VersionVector::initial(member_count),
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id: dataset_id.clone(),
            operations: vec![encoded_operation],
        }],
        applied_locally: false,
    };
    let schemas = std::collections::HashMap::from([(
        dataset_id.clone(),
        SchemaSource::from(title_schema_static()),
    )]);

    let error =
        validate_update_mapping(&update, &schemas).expect_err("change-id mismatch should reject");

    match error {
        InboundDeliveryError::UpdateOperationIdMismatch {
            group: actual_group_id,
            update: update_id,
            dataset: actual_dataset_id,
            operation_change: actual_operation_change_id,
        } => {
            assert_eq!(actual_group_id, group_id);
            assert_eq!(update_id, batch_update_id);
            assert_eq!(actual_dataset_id, dataset_id);
            assert_eq!(actual_operation_change_id, operation_change_id);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn inbound_update_rejects_self_dependent_read_versions_before_persisting() {
    let dataset_id = docs_dataset_id();
    let group_id = GroupId(Uuid::from_u128(28));
    let update_id = UpdateId {
        version: 1,
        node_index: 0,
    };
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let mut read_versions = VersionVector::initial(member_count);
    read_versions.increment_at(0);
    let update = ReplicationUpdateRecord {
        group_id,
        update_id,
        sender: alice_member(),
        read_versions,
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id,
            operations: Vec::new(),
        }],
        applied_locally: false,
    };

    let error = validate_inbound_update_read_versions(&update)
        .expect_err("self-dependent read versions should be rejected");

    match error {
        InboundDeliveryError::SelfDependentReadVersions {
            group_id: actual_group_id,
            update_id: actual_update_id,
            producer_read_version,
        } => {
            assert_eq!(actual_group_id, group_id);
            assert_eq!(actual_update_id, update_id);
            assert_eq!(producer_read_version, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "The restart scenario keeps pre- and post-restart assertions together for readability."
)]
fn buffered_updates_survive_runtime_restart_and_drain_from_store() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let store = sqlite_store_with_schemas(bob_member.clone(), [(dataset_id.clone(), schema)]);
    let first_listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_bob_id(), store.clone(), first_listener);
    let group_id = GroupId(Uuid::from_u128(35));
    runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 36);
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;
    let second_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("second operation should build")
    .expect("second operation should apply")
    .encoded_operation;
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let first_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: VersionVector::initial(member_count),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let mut second_read_versions = VersionVector::initial(member_count);
    second_read_versions.increment_at(0);
    let second_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: second_read_versions,
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![second_operation],
        }],
    };

    runtime
        .apply_update_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should persist pending state");
    drop(runtime);

    let restarted_listener = Arc::new(ListenerStub::default());
    let restarted_runtime =
        load_runtime_with_parts(app_bob_id(), store.clone(), restarted_listener.clone());
    restarted_runtime
        .apply_update_for_test(alice_member, first_message)
        .expect("missing predecessor should apply and drain the persisted successor");
    restarted_listener.wait_for_data_change_count(2);
    assert_eq!(
        restarted_listener.captured_data_changes(),
        vec![
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: row_id.clone(),
                    title: "first".to_owned(),
                }],
            },
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id,
                    title: "second".to_owned(),
                }],
            },
        ]
    );

    assert!(
        load_persisted_update(
            store.as_ref(),
            group_id,
            UpdateId {
                version: 1,
                node_index: 0,
            },
        )
        .expect("first update should persist")
        .applied_locally
    );
    assert!(
        load_persisted_update(
            store.as_ref(),
            group_id,
            UpdateId {
                version: 2,
                node_index: 0,
            },
        )
        .expect("second update should persist")
        .applied_locally
    );
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "The rollback scenario needs the complete ready-update chain inline to show the failing write boundary."
)]
fn causally_ready_apply_chain_rolls_back_when_store_write_fails() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let sqlite_store =
        sqlite_store_with_schemas(bob_member.clone(), [(dataset_id.clone(), schema)]);
    let store = Arc::new(FailingStore::new(sqlite_store.clone()));
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_bob_id(), store.clone(), listener.clone());
    let group_id = GroupId(Uuid::from_u128(37));
    runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 38);
    let mut source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 1,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;
    let second_operation = apply_local_upsert(
        &mut source_dataset,
        &row_id,
        crate::row_values! { "title" => "second" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("second operation should build")
    .expect("second operation should apply")
    .encoded_operation;
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let first_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 1,
            node_index: 0,
        },
        read_versions: VersionVector::initial(member_count),
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let mut second_read_versions = VersionVector::initial(member_count);
    second_read_versions.increment_at(0);
    let second_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: second_read_versions,
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![second_operation],
        }],
    };

    runtime
        .apply_update_for_test(alice_member.clone(), second_message)
        .expect("out-of-order update should persist pending state");
    store.fail_next_apply_dataset_row_patch(dataset_id.clone());
    let error = runtime
        .apply_update_for_test(alice_member.clone(), first_message.clone())
        .expect_err("store write failure should abort the whole ready chain");
    assert!(matches!(error, InboundDeliveryError::StoreAccess { .. }));
    assert!(listener.captured_data_changes().is_empty());

    assert!(
        load_persisted_update(
            sqlite_store.as_ref(),
            group_id,
            UpdateId {
                version: 1,
                node_index: 0,
            },
        )
        .is_none(),
        "the newly ready predecessor must roll back with the failed transaction"
    );
    assert!(
        !load_persisted_update(
            sqlite_store.as_ref(),
            group_id,
            UpdateId {
                version: 2,
                node_index: 0,
            },
        )
        .expect("pending successor should still exist")
        .applied_locally,
        "the previously buffered successor must stay pending after rollback"
    );
    let row_slice = load_persisted_row_slice(
        sqlite_store.as_ref(),
        group_id,
        &dataset_id,
        [row_id.row_key],
    );
    assert!(!row_slice.dataset_exists);
    assert_eq!(row_slice.rows.get(&row_id.row_key), Some(&None));
    assert_eq!(
        load_persisted_group(sqlite_store.as_ref(), group_id)
            .version_vector
            .version_at(0),
        0
    );

    runtime
        .apply_update_for_test(alice_member, first_message)
        .expect("retry after rollback should succeed");
    listener.wait_for_data_change_count(2);
}

#[test]
fn buffered_updates_reject_conflicting_duplicate_payloads() {
    // Conflicting duplicate protection:
    // 1. buffer one out-of-order update on Bob,
    // 2. deliver a second message with the same UpdateId but different payload,
    // 3. assert that the runtime rejects the conflicting duplicate explicitly.
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let schema = title_schema_static();
    let bob_runtime = load_runtime_fixture(
        app_bob_id(),
        bob_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let bob_runtime = bob_runtime.runtime;
    let group_id = GroupId(Uuid::from_u128(24));
    bob_runtime
        .install_group_for_test(
            group_id,
            GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                .expect("group should build"),
        )
        .expect("group should install");

    let row_id = test_row_id(group_id, dataset_id.clone(), 25);
    let member_count = NonZeroUsize::new(2).expect("group has two members");

    let mut first_source_dataset = LocalDataset::new(schema);
    let first_operation = apply_local_upsert(
        &mut first_source_dataset,
        &row_id,
        crate::row_values! { "title" => "first" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("first operation should build")
    .expect("first operation should apply")
    .encoded_operation;

    let mut conflicting_source_dataset = LocalDataset::new(schema);
    let conflicting_operation = apply_local_upsert(
        &mut conflicting_source_dataset,
        &row_id,
        crate::row_values! { "title" => "conflict" },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("conflicting operation should build")
    .expect("conflicting operation should apply")
    .encoded_operation;

    let buffered_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: {
            let mut read_versions = VersionVector::initial(member_count);
            read_versions.increment_at(0);
            read_versions
        },
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id: dataset_id.clone(),
            operations: vec![first_operation],
        }],
    };
    let conflicting_message = UpdateMessage {
        group_id,
        update_id: UpdateId {
            version: 2,
            node_index: 0,
        },
        read_versions: {
            let mut read_versions = VersionVector::initial(member_count);
            read_versions.increment_at(0);
            read_versions
        },
        dataset_updates: vec![DatasetUpdateMessage {
            dataset_id,
            operations: vec![conflicting_operation],
        }],
    };

    bob_runtime
        .apply_update_for_test(alice_member.clone(), buffered_message)
        .expect("first out-of-order update should buffer");
    let error = bob_runtime
        .apply_update_for_test(alice_member, conflicting_message)
        .expect_err("conflicting duplicate payload should fail");
    match error {
        InboundDeliveryError::ConflictingPersistedUpdate {
            group: actual_group_id,
            update: update_id,
        } => {
            assert_eq!(actual_group_id, group_id);
            assert_eq!(
                update_id,
                UpdateId {
                    version: 2,
                    node_index: 0,
                }
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

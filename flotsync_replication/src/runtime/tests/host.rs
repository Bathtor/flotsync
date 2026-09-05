//! Runtime-host and runtime-startup scenarios.

use super::*;
use crate::ApplicationSynchronisation;
use kompact::prelude::ComponentDefinition as _;
use smallvec::SmallVec;

/// Build one title-only upsert for startup scenarios.
fn title_upsert(row_id: RowId, title: &str) -> RowMutation {
    RowMutation::Upsert {
        row_id,
        row: crate::row_values! { "title" => title },
    }
}

/// Exhaust the single empty group prepared by a focused lifecycle fixture.
fn drain_empty_group(synchronisation: &mut ApplicationSynchronisation) {
    let mut group = synchronisation
        .next_group()
        .expect("the prepared synchronisation should contain one group");
    assert!(
        wait_for_test_reply(group.rows().next_batch())
            .expect("empty group synchronisation should complete")
            .is_none()
    );
}

/// Persist the ordinary one-member readable group used by startup lifecycle tests.
fn persist_default_startup_group(
    store: &dyn ReplicationStore,
    local_member: &MemberIdentity,
    group_id: GroupId,
) {
    persist_group_in_store(
        store,
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(vec![local_member.clone()]),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: VersionVector::initial(
                NonZeroUsize::new(1).expect("the default startup group has one member"),
            ),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material_with_key_id(
                group_id,
                *test_replication_security_secrets().store_secret_key_id(),
            ),
            ..Default::default()
        },
    );
}

/// Load the ordinary staged runtime used by focused startup lifecycle tests.
fn load_staged_startup(store: Arc<dyn ReplicationStore>) -> StagedStartup {
    load_staged_startup_with_config(store, ReplicationConfig::default())
}

/// Load a staged runtime with one scenario-specific replication configuration.
fn load_staged_startup_with_config(
    store: Arc<dyn ReplicationStore>,
    config: ReplicationConfig,
) -> StagedStartup {
    let endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let runtime_config_toml = local_endpoint_toml(endpoint_lease.addr(0));
    let load = wait_for_test_reply(load_replication_runtime_with_runtime_config_toml(
        app_probe_id(),
        &TITLE_APPLICATION_SCHEMAS,
        store,
        None,
        Arc::new(ListenerStub::default()),
        config,
        test_replication_security_secrets(),
        &runtime_config_toml,
    ))
    .expect("runtime preparation should succeed");
    let ReplicationRuntimeLoad::Synchronising(synchronisation) = load else {
        panic!("a readable stored group should require application synchronisation");
    };
    StagedStartup {
        _endpoint_lease: endpoint_lease,
        synchronisation,
    }
}

/// Build one concurrent-access classification for snapshot retry tests.
fn snapshot_scan_failure(scope: StoreErrorScope) -> StoreErrorClassification {
    StoreErrorClassification::UNKNOWN
        .with_scope(scope)
        .with_class(StoreErrorClass::ConcurrentAccess)
        .with_resolution(StoreErrorResolution::Retry)
}

/// Staged runtime and socket reservation kept together for lifecycle tests.
struct StagedStartup {
    /// Reservation which prevents another test from claiming the configured endpoint.
    _endpoint_lease: ReservedSocketLease,
    /// Application synchronisation returned by the standard test startup boundary.
    synchronisation: ApplicationSynchronisation,
}

#[test]
fn startup_placeholder_listener_logs_or_panics_as_configured() {
    let logging_listener = StartupEventPolicy::Log.create_listener();
    wait_for_test_reply(
        logging_listener.on_event(ReplicationEvent::MigrationProposals {
            proposals: SmallVec::default(),
        }),
    )
    .expect("logging placeholder should discard an unexpected event");

    let strict_listener = StartupEventPolicy::Panic.create_listener();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(
            strict_listener.on_event(ReplicationEvent::MigrationProposals {
                proposals: SmallVec::default(),
            }),
        );
    }));
    assert!(
        panic.is_err(),
        "strict placeholder should panic on an event"
    );
}

#[test]
fn duplicate_readable_groups_return_a_structured_startup_error() {
    let alice_member = alice_member();
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    let group_id = GroupId(Uuid::from_u128(70_000));
    let one_member_group =
        inactive_group_record(group_id, vec![alice_member.clone()], docs_group_schema());
    let incompatible_duplicate = inactive_group_record(
        group_id,
        vec![alice_member.clone(), bob_member()],
        docs_group_schema(),
    );
    store.override_loaded_groups(vec![one_member_group, incompatible_duplicate]);
    let store: Arc<dyn ReplicationStore> = store;

    let result = wait_for_test_reply(prepare_application_state(
        &alice_member,
        &TITLE_APPLICATION_SCHEMAS,
        &store,
        NonZeroUsize::MIN,
    ));

    let Err(RuntimeStartupError::InvalidGroup { source, .. }) = result else {
        panic!("duplicate stored groups should return an invalid-group error");
    };
    assert!(matches!(
        *source,
        GroupInstallError::DuplicateStoredGroup {
            group_id: duplicate_group_id,
        } if duplicate_group_id == group_id
    ));
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the end-to-end scenario keeps multi-group metadata, row paging, tombstone filtering, listener hand-off, and post-start use in one coherent assertion flow"
)]
fn non_empty_store_replays_application_snapshot_before_runtime_activation() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let seed_runtime = load_runtime_with_parts_and_application_schemas(
        app_probe_id(),
        &TWO_TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        Arc::new(ListenerStub::default()),
    );
    let group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        group_schema: two_title_group_schema(),
        ..Default::default()
    }))
    .expect("seed group should be created");
    let dataset_id = docs_dataset_id();
    let first_row_id = test_row_id(group_id, dataset_id.clone(), 70_001);
    let second_row_id = test_row_id(group_id, dataset_id.clone(), 70_002);
    let third_row_id = test_row_id(group_id, dataset_id.clone(), 70_003);
    let deleted_row_id = test_row_id(group_id, dataset_id, 70_004);
    let notes_row_id = test_row_id(group_id, notes_dataset_id(), 70_005);
    let initial_token = seed_runtime.group_read_token_for_test(group_id);
    let insert_receipt = publish_changes(
        seed_runtime.as_ref(),
        initial_token,
        vec![
            title_upsert(first_row_id.clone(), "first"),
            title_upsert(second_row_id.clone(), "second"),
            title_upsert(third_row_id.clone(), "third"),
            title_upsert(deleted_row_id.clone(), "deleted"),
            title_upsert(notes_row_id.clone(), "note"),
        ],
    );
    let delete_receipt = publish_changes(
        seed_runtime.as_ref(),
        insert_receipt.read_token,
        vec![RowMutation::Delete {
            row_id: deleted_row_id,
        }],
    );
    let empty_group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("empty seed group should be created");
    let empty_group_token = seed_runtime.group_read_token_for_test(empty_group_id);
    let replay_group_token = delete_receipt.read_token.clone();
    wait_for_test_reply(seed_runtime.shutdown()).expect("seed runtime should shut down");

    let runtime_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let runtime_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(0));
    let listener = Arc::new(ListenerStub::default());
    let mut application_token = ApplicationReadToken::from(replay_group_token.clone());
    application_token.merge_applied(&empty_group_token);
    let config = ReplicationConfig {
        application_synchronisation_batch_size: NonZeroUsize::new(2)
            .expect("test batch size should be non-zero"),
        ..Default::default()
    };
    let load = wait_for_test_reply(load_replication_runtime_with_runtime_config_toml(
        app_probe_id(),
        &TWO_TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        Some(application_token),
        listener.clone(),
        config,
        test_replication_security_secrets(),
        &runtime_config_toml,
    ))
    .expect("runtime preparation should succeed");
    let ReplicationRuntimeLoad::Synchronising(mut synchronisation) = load else {
        panic!("a readable stored group should require application synchronisation");
    };

    let mut expected_group_ids = [group_id, empty_group_id];
    expected_group_ids.sort();
    assert_eq!(
        synchronisation
            .final_read_token()
            .group_read_token(&group_id),
        Some(replay_group_token.clone())
    );
    assert_eq!(
        synchronisation
            .final_read_token()
            .group_read_token(&empty_group_id),
        Some(empty_group_token.clone())
    );
    assert!(listener.captured_data_changes().is_empty());

    let mut replayed_group_ids = Vec::new();
    let mut replayed_rows = Vec::new();
    let mut batch_shapes = Vec::new();
    while let Some(mut group) = synchronisation.next_group() {
        let current_group_id = group.group_id();
        replayed_group_ids.push(current_group_id);
        if current_group_id == group_id {
            assert_eq!(group.read_token(), &replay_group_token);
        } else {
            assert_eq!(current_group_id, empty_group_id);
            assert_eq!(group.read_token(), &empty_group_token);
        }
        while let Some(batch) = wait_for_test_reply(group.rows().next_batch())
            .expect("synchronisation batch should load")
        {
            assert!(batch.row_count() <= 2);
            let batch_dataset = batch
                .rows()
                .next()
                .expect("emitted synchronisation batches must not be empty")
                .row_id()
                .dataset_id
                .clone();
            batch_shapes.push((batch_dataset.clone(), batch.row_count()));
            for row in batch.rows() {
                assert_eq!(
                    row.row_id().dataset_id,
                    batch_dataset,
                    "one synchronisation batch must not cross dataset boundaries"
                );
                let title = row
                    .get_field_value::<str>("title")
                    .expect("snapshot row title should decode")
                    .into_owned();
                replayed_rows.push((row.row_id().clone(), title));
            }
        }
    }
    assert_eq!(replayed_group_ids, expected_group_ids);
    assert_eq!(
        replayed_rows,
        vec![
            (first_row_id, "first".to_owned()),
            (second_row_id, "second".to_owned()),
            (third_row_id, "third".to_owned()),
            (notes_row_id, "note".to_owned()),
        ]
    );
    assert_eq!(
        batch_shapes,
        vec![
            (docs_dataset_id(), 2),
            (docs_dataset_id(), 1),
            (notes_dataset_id(), 1),
        ]
    );
    assert!(listener.captured_data_changes().is_empty());

    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("exhausted synchronisation should activate the runtime");
    assert!(
        runtime
            .group_state()
            .expect("runtime group state should be available")
            .group(&group_id)
            .is_some()
    );
    let post_start_row_id = test_row_id(group_id, docs_dataset_id(), 70_006);
    publish_changes(
        runtime.as_ref(),
        replay_group_token,
        vec![title_upsert(post_start_row_id, "after startup")],
    );
    listener.wait_for_data_change_count(1);
    wait_for_test_reply(runtime.shutdown()).expect("activated runtime should shut down");
}

#[test]
fn inbound_work_queued_during_synchronisation_is_delivered_once_after_completion() {
    let runtime_endpoint_lease =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let alice_member = alice_member();
    let bob_member = bob_member();
    let alice_store = sqlite_store(alice_member.clone());
    let bob_store = sqlite_store(bob_member.clone());
    provision_test_security(alice_store.as_ref(), &alice_member, [bob_member.clone()]);
    provision_test_security(bob_store.as_ref(), &bob_member, [alice_member.clone()]);
    let group_id = GroupId(Uuid::from_u128(70_051));
    let security = load_test_runtime_security(bob_store.clone(), &bob_member);
    let security_material = security
        .seal_group_secret(group_id.0, &test_group_key(group_id))
        .expect("test group secret should seal");
    let member_count = NonZeroUsize::new(2).expect("test group has two members");
    persist_group_in_store(
        bob_store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(vec![alice_member.clone(), bob_member.clone()]),
            local_member_index: MemberIndex::new(1),
            group_schema: docs_group_schema(),
            version_vector: VersionVector::initial(member_count),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material,
            ..Default::default()
        },
    );
    let alice_listener = Arc::new(ListenerStub::default());
    let alice_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(0));
    let alice_runtime = load_runtime_with_parts_and_runtime_config_toml(
        app_alice_id(),
        &TITLE_APPLICATION_SCHEMAS,
        alice_store.clone(),
        alice_listener,
        &alice_config_toml,
    );
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("test group members should be valid");
    alice_runtime
        .install_group_for_test(group_id, members)
        .expect("sender group should install");

    let listener = Arc::new(ListenerStub::default());
    let runtime_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(1));
    let load = wait_for_test_reply(
        load_replication_runtime_typed_with_observed_startup_for_test(
            app_bob_id(),
            &TITLE_APPLICATION_SCHEMAS,
            bob_store.clone(),
            listener.clone(),
            ReplicationConfig::default(),
            security,
            Some(&runtime_config_toml),
        ),
    )
    .expect("runtime preparation should succeed");
    let TypedReplicationRuntimeLoad::Synchronising(mut synchronisation) = load else {
        panic!("a readable stored group should require application synchronisation");
    };

    alice_runtime.publish_direct_peer_route_for_test(
        bob_member.clone(),
        synchronisation.advertised_loopback_udp_addr_for_test(),
    );
    let row_id = test_row_id(group_id, docs_dataset_id(), 70_052);
    let read_token = alice_runtime.group_read_token_for_test(group_id);
    publish_changes(
        alice_runtime.as_ref(),
        read_token,
        vec![title_upsert(row_id.clone(), "queued while synchronising")],
    );
    synchronisation.wait_for_group_broadcast_inbound_for_test();
    assert!(listener.captured_data_changes().is_empty());
    drain_empty_group(&mut synchronisation);

    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("exhausted synchronisation should activate the runtime");
    listener.wait_for_data_change_count(1);
    assert_eq!(
        listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id,
                title: "queued while synchronising".to_owned(),
            }],
        }]
    );
    wait_for_test_reply(runtime.shutdown()).expect("activated runtime should shut down");
    wait_for_test_reply(alice_runtime.shutdown()).expect("sender runtime should shut down");
}

#[test]
fn incomplete_application_synchronisation_rejects_completion_and_shuts_down() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(70_101));
    persist_default_startup_group(store.as_ref(), &alice_member, group_id);
    let startup = load_staged_startup(store.clone());
    let synchronisation = startup.synchronisation;

    let Err(error) = wait_for_test_reply(synchronisation.complete()) else {
        panic!("completion before provider exhaustion should fail");
    };
    assert!(matches!(
        error,
        LoadError::SynchronisationIncomplete { application_id }
            if application_id == app_probe_id()
    ));
    let groups = load_persisted_groups(store.as_ref());
    assert_eq!(groups.len(), 1);

    let retry_startup = load_staged_startup(store.clone());
    let retry = retry_startup.synchronisation;
    wait_for_test_reply(retry.shutdown()).expect("explicit synchronisation abort should shut down");

    let dropped_startup = load_staged_startup(store.clone());
    let dropped = dropped_startup.synchronisation;
    drop(dropped);

    let post_drop_startup = load_staged_startup(store.clone());
    let post_drop = post_drop_startup.synchronisation;
    wait_for_test_reply(post_drop.shutdown())
        .expect("post-drop synchronisation should shut down cleanly");
}

#[test]
fn terminal_provider_release_failure_leaves_provider_invalid() {
    let alice_member = alice_member();
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(70_121));
    persist_default_startup_group(store.as_ref(), &alice_member, group_id);
    let staged = load_staged_startup(store.clone());
    let mut synchronisation = staged.synchronisation;
    store.fail_next_read_release();

    let mut group = synchronisation
        .next_group()
        .expect("the prepared synchronisation should contain one group");
    let Err(first_error) = wait_for_test_reply(group.rows().next_batch()) else {
        panic!("terminal transaction release should fail");
    };
    assert!(matches!(
        first_error,
        RowProviderError::ProviderExternal { .. }
    ));
    let Err(retry_error) = wait_for_test_reply(group.rows().next_batch()) else {
        panic!("an invalid provider must reject a later read");
    };
    assert!(matches!(retry_error, RowProviderError::ProviderFailed));
    drop(group);

    let Err(error) = wait_for_test_reply(synchronisation.complete()) else {
        panic!("an invalid provider must reject completion");
    };
    assert!(matches!(
        error,
        LoadError::SynchronisationIncomplete { application_id }
            if application_id == app_probe_id()
    ));
}

#[test]
fn operation_scoped_snapshot_scan_failure_can_retry_in_existing_transaction() {
    let alice_member = alice_member();
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    provision_test_security(store.as_ref(), &alice_member, []);
    let seed_runtime = load_runtime_with_parts_and_application_schemas(
        app_probe_id(),
        &TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        Arc::new(ListenerStub::default()),
    );
    let group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("retry-test group should be created");
    let first_row_id = test_row_id(group_id, docs_dataset_id(), 70_125_001);
    let second_row_id = test_row_id(group_id, docs_dataset_id(), 70_125_002);
    let third_row_id = test_row_id(group_id, docs_dataset_id(), 70_125_003);
    let read_token = seed_runtime.group_read_token_for_test(group_id);
    publish_changes(
        seed_runtime.as_ref(),
        read_token,
        vec![
            title_upsert(first_row_id.clone(), "first"),
            title_upsert(second_row_id.clone(), "second"),
            title_upsert(third_row_id.clone(), "third"),
        ],
    );
    wait_for_test_reply(seed_runtime.shutdown()).expect("seed runtime should shut down");

    let config = ReplicationConfig {
        application_synchronisation_batch_size: NonZeroUsize::new(1)
            .expect("retry test batch size should be non-zero"),
        ..Default::default()
    };
    let staged = load_staged_startup_with_config(store.clone(), config);
    let mut synchronisation = staged.synchronisation;

    let mut group = synchronisation
        .next_group()
        .expect("the prepared synchronisation should contain one group");
    let first_batch = wait_for_test_reply(group.rows().next_batch())
        .expect("the first bounded batch should load")
        .expect("the first bounded batch should contain a row");
    assert_eq!(
        first_batch
            .rows()
            .map(|row| row.row_id().clone())
            .collect::<Vec<_>>(),
        vec![first_row_id.clone()]
    );

    store.fail_next_snapshot_scan(snapshot_scan_failure(StoreErrorScope::Operation));
    let first_error = wait_for_test_reply(group.rows().next_batch())
        .expect_err("the injected operation failure should surface");
    assert!(matches!(
        first_error,
        RowProviderError::ProviderExternal { .. }
    ));
    let mut remaining_rows = Vec::new();
    while let Some(batch) = wait_for_test_reply(group.rows().next_batch())
        .expect("the operation-scoped failure should be retryable")
    {
        remaining_rows.extend(batch.rows().map(|row| row.row_id().clone()));
    }
    assert_eq!(remaining_rows, vec![second_row_id, third_row_id]);

    let scan_requests = store.snapshot_scan_requests();
    assert!(
        scan_requests.len() >= 3,
        "the successful batch, failed scan, and retry should all be recorded"
    );
    assert_eq!(scan_requests[0].after, None);
    assert_eq!(
        scan_requests[1].after,
        Some(first_row_id.row_key),
        "the failed scan should continue after the first delivered row"
    );
    assert_eq!(
        scan_requests[2], scan_requests[1],
        "retrying an operation-scoped failure must preserve the scan cursor"
    );
    drop(group);

    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("successful retry should permit runtime activation");
    wait_for_test_reply(runtime.shutdown()).expect("activated runtime should shut down");
}

#[test]
fn compatibility_drain_failure_explicitly_releases_synchronisation() {
    let alice_member = alice_member();
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(70_125_101));
    persist_default_startup_group(store.as_ref(), &alice_member, group_id);
    let security = load_test_runtime_security(store.clone(), &alice_member);
    let releases_before_load = store.read_release_count();
    let endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let runtime_config_toml = local_endpoint_toml(endpoint_lease.addr(0));
    store.fail_next_snapshot_scan(snapshot_scan_failure(StoreErrorScope::Operation));

    let result = wait_for_test_reply(load_replication_runtime_typed_with_security_for_test(
        app_probe_id(),
        &TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        Arc::new(ListenerStub::default()),
        ReplicationConfig::default(),
        security,
        Some(&runtime_config_toml),
    ));
    let Err(error) = result else {
        panic!("compatibility draining should return the injected scan failure");
    };
    assert!(matches!(error, LoadError::Runtime { .. }));
    assert_eq!(
        store.read_release_count(),
        releases_before_load + 1,
        "the retained store cut must be released before the drain error returns"
    );
}

#[test]
fn transaction_scoped_snapshot_scan_failure_invalidates_provider() {
    let alice_member = alice_member();
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(70_126));
    persist_default_startup_group(store.as_ref(), &alice_member, group_id);
    let staged = load_staged_startup(store.clone());
    let mut synchronisation = staged.synchronisation;
    store.fail_next_snapshot_scan(snapshot_scan_failure(StoreErrorScope::Transaction));

    let mut group = synchronisation
        .next_group()
        .expect("the prepared synchronisation should contain one group");
    let first_error = wait_for_test_reply(group.rows().next_batch())
        .expect_err("the injected transaction failure should surface");
    assert!(matches!(
        first_error,
        RowProviderError::ProviderExternal { .. }
    ));
    let retry_error = wait_for_test_reply(group.rows().next_batch())
        .expect_err("a transaction-scoped failure should invalidate the provider");
    assert!(matches!(retry_error, RowProviderError::ProviderFailed));
    drop(group);

    let Err(completion_error) = wait_for_test_reply(synchronisation.complete()) else {
        panic!("an invalid provider must reject completion");
    };
    assert!(matches!(
        completion_error,
        LoadError::SynchronisationIncomplete { application_id }
            if application_id == app_probe_id()
    ));
}

#[test]
fn explicit_synchronisation_shutdown_surfaces_provider_release_failure() {
    let alice_member = alice_member();
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(70_131));
    persist_default_startup_group(store.as_ref(), &alice_member, group_id);
    let staged = load_staged_startup(store.clone());
    let synchronisation = staged.synchronisation;
    store.fail_next_read_release();

    let error = wait_for_test_reply(synchronisation.shutdown())
        .expect_err("explicit shutdown should surface provider release failure");
    let LoadError::Runtime {
        application_id,
        source,
    } = error
    else {
        panic!("unexpected shutdown error: {error:?}");
    };
    assert_eq!(application_id, app_probe_id());
    assert!(matches!(
        source.downcast_ref::<RowProviderError>(),
        Some(RowProviderError::ProviderExternal { .. })
    ));
}

#[test]
fn runtime_start_failure_shuts_down_staged_host_and_allows_fresh_retry() {
    let alice_member = alice_member();
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(70_151));
    persist_default_startup_group(store.as_ref(), &alice_member, group_id);
    let staged = load_staged_startup(store.clone());
    let mut synchronisation = staged.synchronisation;
    drain_empty_group(&mut synchronisation);
    store.fail_next_read_transaction();

    let completion = flotsync_io::test_support::wait_for_future(
        Duration::from_secs(15),
        synchronisation.complete(),
        "timed out waiting for injected runtime startup failure",
    );
    let Err(error) = completion else {
        panic!("runtime component startup read should fail");
    };
    let LoadError::Runtime {
        application_id,
        source,
    } = error
    else {
        panic!("unexpected completion error: {error:?}");
    };
    assert_eq!(application_id, app_probe_id());
    assert!(matches!(
        source.downcast_ref::<RuntimeHostError>(),
        Some(RuntimeHostError::StartComponent { component, .. })
            if *component == CatchUpManagerComponent::type_name()
    ));

    let retry_staged = load_staged_startup(store);
    let mut retry = retry_staged.synchronisation;
    drain_empty_group(&mut retry);
    let retry_runtime = wait_for_test_reply(retry.complete())
        .expect("fresh retry should complete into a live runtime");
    wait_for_test_reply(retry_runtime.shutdown()).expect("retry runtime should shut down cleanly");
}

#[test]
fn stored_state_without_readable_groups_starts_ready() {
    let runtime_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let group_id = GroupId(Uuid::from_u128(70_201));
    let successor_group_id = GroupId(Uuid::from_u128(70_202));
    let versions = VersionVector::initial(NonZeroUsize::new(1).expect("one group member"));
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(vec![alice_member]),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: versions.clone(),
            lifecycle: ReplicationGroupLifecycle::Closed {
                successor_group_id,
                final_versions: versions,
            },
            security_material: current_slice_placeholder_group_security_material_with_key_id(
                group_id,
                *test_replication_security_secrets().store_secret_key_id(),
            ),
            ..Default::default()
        },
    );
    let runtime_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(0));
    let load = wait_for_test_reply(load_replication_runtime_with_runtime_config_toml(
        app_probe_id(),
        &TITLE_APPLICATION_SCHEMAS,
        store.clone(),
        None,
        Arc::new(ListenerStub::default()),
        ReplicationConfig::default(),
        test_replication_security_secrets(),
        &runtime_config_toml,
    ))
    .expect("closed group state should load");
    let ReplicationRuntimeLoad::Ready(runtime) = load else {
        panic!("closed groups should not require application synchronisation");
    };
    assert!(
        runtime
            .group_state()
            .expect("runtime group state should be available")
            .group(&group_id)
            .is_some()
    );
    wait_for_test_reply(runtime.shutdown()).expect("ready runtime should shut down");
}

#[test]
fn load_replication_runtime_accepts_store_provisioned_security() {
    let runtime_endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let application_id = app_probe_id();
    let security = setup_api_test_security_secrets();
    let provisioner = wait_for_test_future(SqliteReplicationStoreProvisioner::in_memory())
        .expect("store provisioner should build");
    let identity_setup = wait_for_test_reply(provision_local_identity(
        &provisioner,
        alice_member(),
        &security,
    ))
    .expect("identity should provision");
    assert_eq!(identity_setup.member_id(), &alice_member());
    let expected_public_bundle = identity_setup.into_public_bundle();
    let store = Arc::new(
        wait_for_test_future(provisioner.into_replication_store())
            .expect("provisioned store should activate"),
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(0));

    let loaded_runtime = wait_for_test_reply(load_replication_runtime_with_runtime_config_toml(
        application_id,
        ApplicationSchemas::EMPTY,
        store.clone(),
        None,
        listener,
        ReplicationConfig::default(),
        security,
        &runtime_config_toml,
    ))
    .expect("public runtime loading should accept provisioned security");
    let crate::ReplicationRuntimeLoad::Ready(loaded_runtime) = loaded_runtime else {
        panic!("empty store should not require application synchronisation");
    };
    let loaded_public_bundle = wait_for_test_reply(loaded_runtime.local_public_key_bundle())
        .expect("runtime should expose setup-provisioned public keys");

    assert_eq!(loaded_public_bundle, expected_public_bundle);
    wait_for_test_reply(loaded_runtime.shutdown()).expect("runtime should shut down gracefully");
    wait_for_test_future(store.close()).expect("test SQLite store should close");
}

#[test]
fn runtime_shutdown_is_graceful_idempotent_and_marks_runtime_unavailable() {
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener);

    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down gracefully");
    wait_for_test_reply(runtime.shutdown()).expect("second shutdown should be a no-op");

    let error = wait_for_test_reply(runtime.local_public_key_bundle())
        .expect_err("runtime API should be unavailable after shutdown");
    assert!(matches!(error, ApiError::RuntimeUnavailable));
}

#[test]
fn diagnostics_share_the_runtime_allocation_and_lifecycle() {
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener);
    let runtime_api: Arc<dyn ReplicationApi> = runtime.clone();

    let diagnostics = runtime_api.diagnostics();

    assert_eq!(
        Arc::as_ptr(&runtime).cast::<()>(),
        Arc::as_ptr(&diagnostics).cast::<()>(),
        "replication and diagnostics trait objects should share one allocation"
    );
    let snapshot = wait_for_test_reply(diagnostics.peer_routes())
        .expect("live runtime should return peer-route diagnostics");
    assert!(snapshot.local_endpoint.is_some());
    assert!(snapshot.routes.is_empty());

    wait_for_test_reply(runtime.shutdown()).expect("runtime should shut down gracefully");
    let error = wait_for_test_reply(diagnostics.peer_routes())
        .expect_err("diagnostics should be unavailable after shared runtime shutdown");
    assert!(matches!(error, ApiError::RuntimeUnavailable));
}

#[test]
fn diagnostics_arc_keeps_the_concrete_runtime_alive() {
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener);
    let runtime_weak = Arc::downgrade(&runtime);
    let diagnostics = runtime.diagnostics();

    drop(runtime);

    assert!(runtime_weak.upgrade().is_some());
    wait_for_test_reply(diagnostics.peer_routes())
        .expect("diagnostics Arc should keep the shared runtime alive");
    drop(diagnostics);
    assert!(runtime_weak.upgrade().is_none());
}

#[test]
fn dropping_runtime_inside_test_executor_does_not_reenter_local_pool() {
    let store = sqlite_store(alice_member());
    provision_test_security(store.as_ref(), &alice_member(), []);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener);

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
fn load_replication_runtime_reports_unsupported_identity_without_private_keys() {
    let application_id = app_probe_id();
    let inner_store = sqlite_store(alice_member());
    provision_test_security(inner_store.as_ref(), &alice_member(), []);
    let store = Arc::new(FailingStore::new(inner_store.clone()).with_hidden_local_private_keys());
    let listener = Arc::new(ListenerStub::default());

    let loaded_runtime = wait_for_test_reply(load_replication_runtime(
        application_id.clone(),
        ApplicationSchemas::EMPTY,
        store.clone(),
        None,
        listener,
        ReplicationConfig::default(),
        test_replication_security_secrets(),
    ));
    let Err(error) = loaded_runtime else {
        panic!("runtime loading should reject an identity without private keys");
    };

    let error = security_load_error(error, &application_id);
    let LoadSecurityError::Other { source, .. } = error else {
        panic!("unsupported broken store state should remain an internal error: {error:?}");
    };
    assert!(matches!(
        source.downcast_ref::<DeliverySecurityError>(),
        Some(DeliverySecurityError::MissingLocalPrivateKeys { member_id })
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
        ApplicationSchemas::EMPTY,
        store.clone(),
        None,
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
        ApplicationSchemas::EMPTY,
        store.clone(),
        None,
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
        ApplicationSchemas::EMPTY,
        store.clone(),
        None,
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
        ApplicationSchemas::EMPTY,
        store.clone(),
        None,
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
            ..Default::default()
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
        MemberPublicKeysRecord::from_public_keys(&test_public_keys(&MemberIdentity::from_array([
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
            ..Default::default()
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
    let mut host = start_host(&MemberIdentity::from_array(PROBE_MEMBER_SEGMENTS));

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
        ApplicationSchemas::EMPTY,
        store.clone(),
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
    let bob_runtime = load_runtime_with_parts(app_bob_id(), bob_store.clone(), bob_listener);
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
        ApplicationSchemas::EMPTY,
        alice_store.clone(),
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
    let group_state = load_group_state_for_test(&alice_member, ApplicationSchemas::EMPTY, &store);
    let mut host =
        kompact::prelude::block_on(DeliveryRuntimeHost::start_with_route_publish_mode_for_test(
            &alice_member,
            group_state,
            store.clone(),
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
    let group_state = load_group_state_for_test(&alice_member, ApplicationSchemas::EMPTY, &store);
    let mut host =
        kompact::prelude::block_on(DeliveryRuntimeHost::start_with_route_publish_mode_for_test(
            &alice_member,
            group_state,
            store.clone(),
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

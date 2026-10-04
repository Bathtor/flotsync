//! Runtime-host and runtime-startup scenarios.

use super::*;
use crate::{
    ApplicationSynchronisation,
    SingleGroupSynchronisation,
    StoreErrorClassificationSource as _,
};
use flotsync_utils::testing::assert_inferred_send;
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
    let group = wait_for_test_reply(synchronisation.next_group())
        .expect("the prepared group should load")
        .expect("the prepared synchronisation should contain one group");
    let SingleGroupSynchronisation::Snapshot(mut group) = group else {
        panic!("a startup without an application token should yield a snapshot");
    };
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
    let PreparedRuntimeStartup {
        endpoint_lease,
        load,
    } = prepare_runtime_startup(store, None, config);
    let ReplicationRuntimeLoad::Synchronising(synchronisation) = load else {
        panic!("a readable stored group should require application synchronisation");
    };
    StagedStartup {
        _endpoint_lease: endpoint_lease,
        synchronisation,
    }
}

/// Prepare a runtime from one caller-selected application position.
fn prepare_runtime_startup(
    store: Arc<dyn ReplicationStore>,
    application_read_token: Option<ApplicationReadToken>,
    config: ReplicationConfig,
) -> PreparedRuntimeStartup {
    let endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
    let runtime_config_toml = local_endpoint_toml(endpoint_lease.addr(0));
    let builder = ReplicationRuntime::builder(app_probe_id())
        .application_schemas(&TITLE_APPLICATION_SCHEMAS)
        .store(store)
        .listener(Arc::new(ListenerStub::default()))
        .security_secrets(test_replication_security_secrets())
        .maybe_application_read_token(application_read_token)
        .config(config)
        .runtime_config_toml(&runtime_config_toml);
    let load = wait_for_test_reply(builder.load()).expect("runtime preparation should succeed");
    PreparedRuntimeStartup {
        endpoint_lease,
        load,
    }
}

/// Build one retryable concurrent-access classification for provider tests.
fn retryable_store_failure(scope: StoreErrorScope) -> StoreErrorClassification {
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

/// Prepared runtime and its reserved endpoint retained together by startup tests.
struct PreparedRuntimeStartup {
    /// Reservation which keeps the configured endpoint exclusive until shutdown.
    endpoint_lease: ReservedSocketLease,
    /// Ready runtime or application reconciliation prepared from the store cut.
    load: ReplicationRuntimeLoad,
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
        None,
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
fn exact_application_position_starts_ready_without_reconciliation() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let seed_builder = runtime_builder(
        app_probe_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );
    let seed_runtime = load_runtime(seed_builder);
    let group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("exact-position group should be created");
    let row_id = test_row_id(group_id, docs_dataset_id(), 70_000_101);
    let initial_token = seed_runtime.group_read_token_for_test(group_id);
    let receipt = publish_changes(
        seed_runtime.as_ref(),
        initial_token,
        vec![title_upsert(row_id, "current")],
    );
    let application_token = ApplicationReadToken::from(receipt.read_token);
    wait_for_test_reply(seed_runtime.shutdown()).expect("seed runtime should shut down");

    let PreparedRuntimeStartup {
        endpoint_lease: _endpoint_lease,
        load,
    } = prepare_runtime_startup(
        store.clone(),
        Some(application_token),
        ReplicationConfig::default(),
    );
    let ReplicationRuntimeLoad::Ready(runtime) = load else {
        panic!("an exact application position should start ready");
    };
    wait_for_test_reply(runtime.shutdown()).expect("exact-position runtime should shut down");
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one incremental scenario verifies coalescing, deletion, transient-row omission, row-free exhaustion semantics, and token advancement together"
)]
fn behind_application_position_yields_one_coalesced_group_change() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let seed_builder = runtime_builder(
        app_probe_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );
    let seed_runtime = load_runtime(seed_builder);
    let group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("incremental group should be created");
    let updated_row_id = test_row_id(group_id, docs_dataset_id(), 70_000_201);
    let deleted_row_id = test_row_id(group_id, docs_dataset_id(), 70_000_202);
    let transient_row_id = test_row_id(group_id, docs_dataset_id(), 70_000_203);
    let initial_token = seed_runtime.group_read_token_for_test(group_id);
    let source_receipt = publish_changes(
        seed_runtime.as_ref(),
        initial_token,
        vec![
            title_upsert(updated_row_id.clone(), "source"),
            title_upsert(deleted_row_id.clone(), "delete me"),
        ],
    );
    let second_receipt = publish_changes(
        seed_runtime.as_ref(),
        source_receipt.read_token.clone(),
        vec![title_upsert(updated_row_id.clone(), "intermediate")],
    );
    let third_receipt = publish_changes(
        seed_runtime.as_ref(),
        second_receipt.read_token,
        vec![
            title_upsert(updated_row_id.clone(), "current"),
            title_upsert(transient_row_id, "temporary"),
        ],
    );
    let target_receipt = publish_changes(
        seed_runtime.as_ref(),
        third_receipt.read_token,
        vec![
            RowMutation::Delete {
                row_id: deleted_row_id.clone(),
            },
            RowMutation::Delete {
                row_id: test_row_id(group_id, docs_dataset_id(), 70_000_203),
            },
        ],
    );
    let target_token = target_receipt.read_token.clone();
    let mut application_token = ApplicationReadToken::from(source_receipt.read_token);
    wait_for_test_reply(seed_runtime.shutdown()).expect("seed runtime should shut down");

    let PreparedRuntimeStartup {
        endpoint_lease: _endpoint_lease,
        load,
    } = prepare_runtime_startup(
        store.clone(),
        Some(application_token.clone()),
        ReplicationConfig::default(),
    );
    let ReplicationRuntimeLoad::Synchronising(mut synchronisation) = load else {
        panic!("a behind application position should require reconciliation");
    };
    let next_group = synchronisation.next_group();
    assert_inferred_send(&next_group);
    let group = wait_for_test_reply(next_group)
        .expect("incremental preparation should succeed")
        .expect("the behind group should be yielded");
    let SingleGroupSynchronisation::Changes(mut group) = group else {
        panic!("complete retained history should permit incremental changes");
    };
    assert_eq!(
        group.position().lineage(),
        DataChangeLineage::Update,
        "startup coalescing should use the ordinary update lineage"
    );
    assert_eq!(group.position().group_read_token(), &target_token);
    let position = group.position().clone();
    let batch = wait_for_test_reply(group.rows().next_batch())
        .expect("coalesced changes should load")
        .expect("the group should contain visible changes");
    assert_eq!(batch.len(), 2);
    let actual = batch
        .iter()
        .map(|change| {
            let title = change.row().map(|row| {
                row.get_field_value::<str>("title")
                    .expect("incremental title should decode")
                    .into_owned()
            });
            (change.row_id().clone(), title)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (updated_row_id, Some("current".to_owned())),
            (deleted_row_id, None),
        ]
    );
    assert!(
        wait_for_test_reply(group.rows().next_batch())
            .expect("change provider exhaustion should succeed")
            .is_none()
    );
    application_token.apply_data_change(&position);
    assert_eq!(
        application_token.group_read_token(&group_id),
        Some(target_token)
    );
    drop(group);
    assert!(
        wait_for_test_reply(synchronisation.next_group())
            .expect("reconciliation exhaustion should succeed")
            .is_none()
    );
    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("incremental reconciliation should activate the runtime");
    wait_for_test_reply(runtime.shutdown()).expect("incremental runtime should shut down");
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the multi-producer history fixture and exact range assertions belong together"
)]
fn incremental_history_loads_complete_ranges_for_multiple_producers() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let probe_member = MemberIdentity::from_array(PROBE_MEMBER_SEGMENTS);
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    provision_test_security(
        store.as_ref(),
        &alice_member,
        [bob_member.clone(), probe_member.clone()],
    );
    let seed_builder = runtime_builder(
        app_probe_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    )
    .application_schemas(&TITLE_APPLICATION_SCHEMAS);
    let seed_runtime = load_runtime(seed_builder);
    let group_id = GroupId(Uuid::from_u128(70_000_221));
    let members = GroupMembers::from_ordered_members(vec![
        alice_member,
        bob_member.clone(),
        probe_member.clone(),
    ])
    .expect("history fixture members should be canonical");
    seed_runtime
        .install_group_for_test(group_id, members)
        .expect("history fixture group should install");
    let source_token = seed_runtime.group_read_token_for_test(group_id);
    let member_count = NonZeroUsize::new(3).expect("history fixture has three members");
    let initial_versions = VersionVector::initial(member_count);
    let mut bob_second_versions = initial_versions.clone();
    bob_second_versions.increment_at(1);
    let mut probe_second_versions = initial_versions.clone();
    probe_second_versions.increment_at(2);
    let updates = [
        (
            bob_member.clone(),
            title_update_message(
                group_id,
                docs_dataset_id(),
                70_000_222,
                "bob one",
                UpdateId {
                    node_index: 1,
                    version: 1,
                },
                initial_versions.clone(),
            ),
        ),
        (
            bob_member,
            title_update_message(
                group_id,
                docs_dataset_id(),
                70_000_223,
                "bob two",
                UpdateId {
                    node_index: 1,
                    version: 2,
                },
                bob_second_versions,
            ),
        ),
        (
            probe_member.clone(),
            title_update_message(
                group_id,
                docs_dataset_id(),
                70_000_224,
                "probe one",
                UpdateId {
                    node_index: 2,
                    version: 1,
                },
                initial_versions,
            ),
        ),
        (
            probe_member,
            title_update_message(
                group_id,
                docs_dataset_id(),
                70_000_225,
                "probe two",
                UpdateId {
                    node_index: 2,
                    version: 2,
                },
                probe_second_versions,
            ),
        ),
    ];
    let mut expected_row_ids = Vec::new();
    for (sender, (row_id, update)) in updates {
        expected_row_ids.push(row_id);
        seed_runtime
            .apply_update_for_test(sender, update)
            .expect("history fixture update should apply");
    }
    let target_token = seed_runtime.group_read_token_for_test(group_id);
    wait_for_test_reply(seed_runtime.shutdown()).expect("seed runtime should shut down");

    let config = ReplicationConfig {
        application_synchronisation_batch_size: NonZeroUsize::new(1)
            .expect("history test batch size should be non-zero"),
        ..Default::default()
    };
    let requests_before = store.replication_update_load_requests().len();
    let PreparedRuntimeStartup {
        endpoint_lease: _endpoint_lease,
        load,
    } = prepare_runtime_startup(
        store.clone(),
        Some(ApplicationReadToken::from(source_token)),
        config,
    );
    let ReplicationRuntimeLoad::Synchronising(mut synchronisation) = load else {
        panic!("the behind multi-producer group should require reconciliation");
    };
    let group = wait_for_test_reply(synchronisation.next_group())
        .expect("multi-producer history should prepare")
        .expect("the behind group should be yielded");
    let SingleGroupSynchronisation::Changes(mut changes) = group else {
        panic!("complete multi-producer history should remain incremental");
    };
    assert_eq!(changes.position().group_read_token(), &target_token);
    let batch = wait_for_test_reply(changes.rows().next_batch())
        .expect("multi-producer changes should load")
        .expect("multi-producer changes should not be empty");
    assert_eq!(
        batch
            .iter()
            .map(|change| change.row_id().clone())
            .collect::<Vec<_>>(),
        expected_row_ids
    );
    assert!(
        wait_for_test_reply(changes.rows().next_batch())
            .expect("multi-producer changes should exhaust")
            .is_none()
    );
    drop(changes);

    let requests = store.replication_update_load_requests();
    let page_requests = &requests[requests_before..];
    assert_eq!(page_requests.len(), 6);
    for (producer_index, producer_pages) in [1, 2].into_iter().zip(page_requests.chunks(3)) {
        let expected = ReplicationUpdateLoadRequest {
            group_id,
            filter: ReplicationUpdateFilter::ProducerRange {
                producer_index: MemberIndex::new(producer_index),
                start_version: 1,
                end_version: 2,
            },
            limit: NonZeroUsize::new(1),
        };
        assert!(producer_pages.iter().all(|request| *request == expected));
    }
    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("multi-producer reconciliation should activate the runtime");
    wait_for_test_reply(runtime.shutdown()).expect("multi-producer runtime should shut down");
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the mixed application cut verifies independent exact, empty-incremental, full-snapshot, and retirement planning in one startup"
)]
fn mixed_application_positions_reconcile_each_group_independently() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let seed_builder = runtime_builder(
        app_probe_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );
    let seed_runtime = load_runtime(seed_builder);
    let exact_group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("exact group should be created");
    let incremental_group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("incremental group should be created");
    let snapshot_group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("snapshot group should be created");
    let exact_token = seed_runtime.group_read_token_for_test(exact_group_id);
    let incremental_source = seed_runtime.group_read_token_for_test(incremental_group_id);
    let transient_row_id = test_row_id(incremental_group_id, docs_dataset_id(), 70_000_251);
    let insert_receipt = publish_changes(
        seed_runtime.as_ref(),
        incremental_source.clone(),
        vec![title_upsert(transient_row_id.clone(), "temporary")],
    );
    let incremental_target = publish_changes(
        seed_runtime.as_ref(),
        insert_receipt.read_token,
        vec![RowMutation::Delete {
            row_id: transient_row_id,
        }],
    )
    .read_token;
    wait_for_test_reply(seed_runtime.shutdown()).expect("seed runtime should shut down");

    let retired_group_id = GroupId(Uuid::from_u128(70_000_252));
    let retired_token = GroupReadToken::from_group_version(
        retired_group_id,
        VersionVector::initial(NonZeroUsize::MIN),
    );
    let application_token =
        ApplicationReadToken::from_group_tokens([exact_token, incremental_source, retired_token]);
    let PreparedRuntimeStartup {
        endpoint_lease: _endpoint_lease,
        load,
    } = prepare_runtime_startup(
        store.clone(),
        Some(application_token),
        ReplicationConfig::default(),
    );
    let ReplicationRuntimeLoad::Synchronising(mut synchronisation) = load else {
        panic!("the mixed application position should require reconciliation");
    };

    let mut actual = Vec::new();
    while let Some(group) = wait_for_test_reply(synchronisation.next_group())
        .expect("mixed group reconciliation should load")
    {
        match group {
            SingleGroupSynchronisation::Snapshot(mut snapshot) => {
                let group_id = snapshot.group_id();
                assert!(
                    wait_for_test_reply(snapshot.rows().next_batch())
                        .expect("empty unknown-group snapshot should drain")
                        .is_none()
                );
                actual.push((group_id, "snapshot"));
            }
            SingleGroupSynchronisation::Changes(mut changes) => {
                assert_eq!(changes.position().group_read_token(), &incremental_target);
                assert!(
                    wait_for_test_reply(changes.rows().next_batch())
                        .expect("row-free change collection should drain")
                        .is_none(),
                    "creation and deletion after the source position should emit no row"
                );
                actual.push((incremental_group_id, "changes"));
            }
            SingleGroupSynchronisation::Retired(retired) => {
                actual.push((retired.group_id(), "retired"));
            }
        }
    }
    let mut expected = vec![
        (incremental_group_id, "changes"),
        (snapshot_group_id, "snapshot"),
        (retired_group_id, "retired"),
    ];
    expected.sort_by_key(|(group_id, _)| *group_id);
    assert_eq!(actual, expected);
    assert!(
        actual
            .iter()
            .all(|(group_id, _)| *group_id != exact_group_id),
        "the exact group should not produce reconciliation work"
    );
    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("mixed reconciliation should activate the runtime");
    wait_for_test_reply(runtime.shutdown()).expect("mixed-position runtime should shut down");
}

#[test]
fn closed_predecessor_and_unknown_successor_reconcile_independently() {
    let alice_member = alice_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, []);
    let old_group_id = GroupId(Uuid::from_u128(70_000_301));
    let new_group_id = GroupId(Uuid::from_u128(70_000_302));
    let versions = VersionVector::initial(NonZeroUsize::MIN);
    let security_key_id = *test_replication_security_secrets().store_secret_key_id();
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id: old_group_id,
            member_keys: test_group_member_keys(vec![alice_member.clone()]),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: versions.clone(),
            lifecycle: ReplicationGroupLifecycle::Closed {
                successor_group_id: new_group_id,
                final_versions: versions.clone(),
            },
            security_material: current_slice_placeholder_group_security_material_with_key_id(
                old_group_id,
                security_key_id,
            ),
            ..Default::default()
        },
    );
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id: new_group_id,
            member_keys: test_group_member_keys(vec![alice_member]),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: versions.clone(),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material_with_key_id(
                new_group_id,
                security_key_id,
            ),
            ..Default::default()
        },
    );
    let old_token = GroupReadToken::from_group_version(old_group_id, versions);
    let mut application_token = ApplicationReadToken::from(old_token);
    let PreparedRuntimeStartup {
        endpoint_lease: _endpoint_lease,
        load,
    } = prepare_runtime_startup(
        store.clone(),
        Some(application_token.clone()),
        ReplicationConfig::default(),
    );
    let ReplicationRuntimeLoad::Synchronising(mut synchronisation) = load else {
        panic!("retirement and a new readable group should require reconciliation");
    };

    let first = wait_for_test_reply(synchronisation.next_group())
        .expect("retirement should load")
        .expect("the closed predecessor should be yielded");
    let SingleGroupSynchronisation::Retired(retired) = first else {
        panic!("the closed predecessor should be retired");
    };
    assert_eq!(retired.group_id(), old_group_id);
    application_token.retire_group(&old_group_id);

    let second = wait_for_test_reply(synchronisation.next_group())
        .expect("successor snapshot should load")
        .expect("the new readable successor should be yielded");
    let SingleGroupSynchronisation::Snapshot(mut snapshot) = second else {
        panic!("an unknown readable successor should receive a full snapshot");
    };
    assert_eq!(snapshot.group_id(), new_group_id);
    assert!(
        wait_for_test_reply(snapshot.rows().next_batch())
            .expect("empty successor snapshot should drain")
            .is_none()
    );
    application_token.merge_applied(snapshot.read_token());
    drop(snapshot);
    assert_eq!(&application_token, synchronisation.final_read_token());
    assert!(
        wait_for_test_reply(synchronisation.next_group())
            .expect("lifecycle reconciliation should exhaust")
            .is_none()
    );
    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("lifecycle reconciliation should activate the runtime");
    wait_for_test_reply(runtime.shutdown()).expect("reconciled runtime should shut down");
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one fallback scenario compares missing history, ahead, concurrent, and incompatible positions against the same store cut"
)]
fn unsafe_application_positions_fall_back_to_group_snapshots() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let store = sqlite_store(alice_member.clone());
    provision_test_security(store.as_ref(), &alice_member, [bob_member.clone()]);
    let one_member = NonZeroUsize::MIN;
    let two_members = NonZeroUsize::new(2).expect("two members is non-zero");
    let missing_history_id = GroupId(Uuid::from_u128(70_000_401));
    let ahead_id = GroupId(Uuid::from_u128(70_000_402));
    let concurrent_id = GroupId(Uuid::from_u128(70_000_403));
    let incompatible_id = GroupId(Uuid::from_u128(70_000_404));
    let one_current = VersionVector::initial(one_member).with_update_applied(UpdateId {
        node_index: 0,
        version: 1,
    });
    let concurrent_current = VersionVector::initial(two_members).with_update_applied(UpdateId {
        node_index: 0,
        version: 1,
    });
    let security_key_id = *test_replication_security_secrets().store_secret_key_id();
    for group_id in [missing_history_id, ahead_id, incompatible_id] {
        persist_group_in_store(
            store.as_ref(),
            ReplicationGroupRecord {
                group_id,
                member_keys: test_group_member_keys(vec![alice_member.clone()]),
                local_member_index: MemberIndex::new(0),
                group_schema: docs_group_schema(),
                version_vector: one_current.clone(),
                lifecycle: ReplicationGroupLifecycle::Open,
                security_material: current_slice_placeholder_group_security_material_with_key_id(
                    group_id,
                    security_key_id,
                ),
                ..Default::default()
            },
        );
    }
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id: concurrent_id,
            member_keys: test_group_member_keys(vec![alice_member, bob_member]),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: concurrent_current,
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material_with_key_id(
                concurrent_id,
                security_key_id,
            ),
            ..Default::default()
        },
    );
    let ahead = one_current.with_update_applied(UpdateId {
        node_index: 0,
        version: 2,
    });
    let concurrent = VersionVector::initial(two_members).with_update_applied(UpdateId {
        node_index: 1,
        version: 1,
    });
    let application_token = ApplicationReadToken::from_group_tokens([
        GroupReadToken::from_group_version(missing_history_id, VersionVector::initial(one_member)),
        GroupReadToken::from_group_version(ahead_id, ahead),
        GroupReadToken::from_group_version(concurrent_id, concurrent),
        GroupReadToken::from_group_version(incompatible_id, VersionVector::initial(two_members)),
    ]);
    let PreparedRuntimeStartup {
        endpoint_lease: _endpoint_lease,
        load,
    } = prepare_runtime_startup(
        store.clone(),
        Some(application_token),
        ReplicationConfig::default(),
    );
    let ReplicationRuntimeLoad::Synchronising(mut synchronisation) = load else {
        panic!("unsafe positions should require snapshot reconciliation");
    };
    let expected_ids = [missing_history_id, ahead_id, concurrent_id, incompatible_id];
    for expected_id in expected_ids {
        let group = wait_for_test_reply(synchronisation.next_group())
            .expect("fallback classification should succeed")
            .expect("every unsafe group should be yielded");
        let SingleGroupSynchronisation::Snapshot(mut snapshot) = group else {
            panic!("every unsafe position should select a group snapshot");
        };
        assert_eq!(snapshot.group_id(), expected_id);
        assert!(
            wait_for_test_reply(snapshot.rows().next_batch())
                .expect("empty fallback snapshot should drain")
                .is_none()
        );
    }
    assert!(
        wait_for_test_reply(synchronisation.next_group())
            .expect("fallback reconciliation should exhaust")
            .is_none()
    );
    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("fallback reconciliation should activate the runtime");
    wait_for_test_reply(runtime.shutdown()).expect("fallback runtime should shut down");
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
    let seed_builder = runtime_builder(
        app_probe_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    )
    .application_schemas(&TWO_TITLE_APPLICATION_SCHEMAS);
    let seed_runtime = load_runtime(seed_builder);
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
    let config = ReplicationConfig {
        application_synchronisation_batch_size: NonZeroUsize::new(2)
            .expect("test batch size should be non-zero"),
        ..Default::default()
    };
    let builder = ReplicationRuntime::builder(app_probe_id())
        .application_schemas(&TWO_TITLE_APPLICATION_SCHEMAS)
        .store(store.clone())
        .listener(listener.clone())
        .security_secrets(test_replication_security_secrets())
        .config(config)
        .runtime_config_toml(&runtime_config_toml);
    let load = wait_for_test_reply(builder.load()).expect("runtime preparation should succeed");
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
    while let Some(group) = wait_for_test_reply(synchronisation.next_group())
        .expect("the next snapshot group should load")
    {
        let SingleGroupSynchronisation::Snapshot(mut group) = group else {
            panic!("startup without an application token should only yield snapshots");
        };
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
    let alice_builder = runtime_builder(app_alice_id(), alice_store.clone(), alice_listener)
        .application_schemas(&TITLE_APPLICATION_SCHEMAS)
        .runtime_config_toml(&alice_config_toml);
    let alice_runtime = load_runtime(alice_builder);
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("test group members should be valid");
    alice_runtime
        .install_group_for_test(group_id, members)
        .expect("sender group should install");

    let listener = Arc::new(ListenerStub::default());
    let runtime_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(1));
    let builder = ReplicationRuntime::builder(app_bob_id())
        .application_schemas(&TITLE_APPLICATION_SCHEMAS)
        .store(bob_store.clone())
        .listener(listener.clone())
        .runtime_config_toml(&runtime_config_toml);
    let inputs = builder
        .into_validated_with_security(security)
        .expect("test runtime inputs should be complete");
    let load =
        wait_for_test_reply(load_replication_runtime_typed_with_observed_startup_for_test(inputs))
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
#[allow(
    clippy::too_many_lines,
    reason = "the end-to-end scenario captures a real delivery, reloads a cut containing it, reinjects that delivery during synchronisation, and uses a later update as the processing barrier"
)]
fn update_represented_by_synchronisation_cut_is_not_redelivered_live() {
    let runtime_endpoint_lease =
        reserve_sockets(&[ReservedSocketKind::UdpSocket, ReservedSocketKind::UdpSocket]);
    let alice_member = alice_member();
    let bob_member = bob_member();
    let alice_store = sqlite_store(alice_member.clone());
    let bob_store = sqlite_store(bob_member.clone());
    provision_test_security(alice_store.as_ref(), &alice_member, [bob_member.clone()]);
    provision_test_security(bob_store.as_ref(), &bob_member, [alice_member.clone()]);
    let group_id = GroupId(Uuid::from_u128(70_071));
    let member_count = NonZeroUsize::new(2).expect("duplicate fixture has two members");
    let security_material = load_test_runtime_security(bob_store.clone(), &bob_member)
        .seal_group_secret(group_id.0, &test_group_key(group_id))
        .expect("test group secret should seal");
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

    let alice_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(0));
    let alice_builder = runtime_builder(
        app_alice_id(),
        alice_store.clone(),
        Arc::new(ListenerStub::default()),
    )
    .application_schemas(&TITLE_APPLICATION_SCHEMAS)
    .runtime_config_toml(&alice_config_toml);
    let alice_runtime = load_runtime(alice_builder);
    let members =
        GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
            .expect("duplicate fixture members should be valid");
    alice_runtime
        .install_group_for_test(group_id, members)
        .expect("sender group should install");

    let bob_config_toml = local_endpoint_toml(runtime_endpoint_lease.addr(1));
    let first_listener = Arc::new(ListenerStub::default());
    let first_builder = ReplicationRuntime::builder(app_bob_id())
        .application_schemas(&TITLE_APPLICATION_SCHEMAS)
        .store(bob_store.clone())
        .listener(first_listener.clone())
        .runtime_config_toml(&bob_config_toml);
    let first_security = load_test_runtime_security(bob_store.clone(), &bob_member);
    let first_inputs = first_builder
        .into_validated_with_security(first_security)
        .expect("test runtime inputs should be complete");
    let first_load = wait_for_test_reply(
        load_replication_runtime_typed_with_observed_startup_for_test(first_inputs),
    )
    .expect("first receiver runtime should prepare");
    let TypedReplicationRuntimeLoad::Synchronising(first_synchronisation) = first_load else {
        panic!("the receiver group should initially require a full snapshot");
    };
    let first_bob_runtime =
        wait_for_test_reply(first_synchronisation.complete_discarding_synchronisation())
            .expect("first receiver runtime should activate");
    alice_runtime.publish_direct_peer_route_for_test(
        bob_member.clone(),
        first_bob_runtime.advertised_loopback_udp_addr_for_test(),
    );

    let represented_row_id = test_row_id(group_id, docs_dataset_id(), 70_072);
    let alice_source = alice_runtime.group_read_token_for_test(group_id);
    publish_changes(
        alice_runtime.as_ref(),
        alice_source,
        vec![title_upsert(
            represented_row_id.clone(),
            "represented by cut",
        )],
    );
    let duplicate_indication = first_bob_runtime.capture_group_broadcast_inbound_for_test();
    first_listener.wait_for_data_change_count(1);
    wait_for_test_reply(first_bob_runtime.shutdown())
        .expect("first receiver runtime should shut down");

    let live_listener = Arc::new(ListenerStub::default());
    let reload_builder = ReplicationRuntime::builder(app_bob_id())
        .application_schemas(&TITLE_APPLICATION_SCHEMAS)
        .store(bob_store.clone())
        .listener(live_listener.clone())
        .runtime_config_toml(&bob_config_toml);
    let reload_security = load_test_runtime_security(bob_store.clone(), &bob_member);
    let reload_inputs = reload_builder
        .into_validated_with_security(reload_security)
        .expect("test runtime inputs should be complete");
    let reload = wait_for_test_reply(
        load_replication_runtime_typed_with_observed_startup_for_test(reload_inputs),
    )
    .expect("receiver runtime should reload from the represented cut");
    let TypedReplicationRuntimeLoad::Synchronising(mut synchronisation) = reload else {
        panic!("a full reload should expose the represented row snapshot");
    };
    synchronisation.inject_group_broadcast_inbound_for_test(duplicate_indication);
    let group = wait_for_test_reply(synchronisation.next_group())
        .expect("represented snapshot should load")
        .expect("represented group should be yielded");
    let SingleGroupSynchronisation::Snapshot(mut snapshot) = group else {
        panic!("a reload without an application token should yield a snapshot");
    };
    let snapshot_batch = wait_for_test_reply(snapshot.rows().next_batch())
        .expect("represented snapshot rows should load")
        .expect("represented snapshot should contain the row");
    assert_eq!(
        snapshot_batch
            .rows()
            .map(|row| row.row_id().clone())
            .collect::<Vec<_>>(),
        vec![represented_row_id]
    );
    assert!(
        wait_for_test_reply(snapshot.rows().next_batch())
            .expect("represented snapshot should exhaust")
            .is_none()
    );
    drop(snapshot);
    let bob_runtime = wait_for_test_reply(synchronisation.complete_discarding_synchronisation())
        .expect("receiver runtime should activate after applying the represented cut");
    alice_runtime.publish_direct_peer_route_for_test(
        bob_member,
        bob_runtime.advertised_loopback_udp_addr_for_test(),
    );

    let post_cut_row_id = test_row_id(group_id, docs_dataset_id(), 70_073);
    let alice_read_token = alice_runtime.group_read_token_for_test(group_id);
    publish_changes(
        alice_runtime.as_ref(),
        alice_read_token,
        vec![title_upsert(post_cut_row_id.clone(), "after cut")],
    );
    eventually(
        TEST_WAIT_TIMEOUT,
        || {
            live_listener
                .captured_data_changes()
                .iter()
                .flat_map(|change| &change.rows)
                .any(|change| {
                    matches!(
                        change,
                        CapturedRowChange::Upsert { row_id, .. } if row_id == &post_cut_row_id
                    )
                })
        },
        "timed out waiting for the post-cut update processing barrier",
    );
    assert_eq!(
        live_listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: post_cut_row_id,
                title: "after cut".to_owned(),
            }],
        }],
        "the duplicate update represented by the startup cut must not be delivered live"
    );
    wait_for_test_reply(bob_runtime.shutdown()).expect("receiver runtime should shut down");
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

    let group = wait_for_test_reply(synchronisation.next_group())
        .expect("the prepared group should load")
        .expect("the prepared synchronisation should contain one group");
    let SingleGroupSynchronisation::Snapshot(mut group) = group else {
        panic!("the lifecycle fixture should yield a snapshot");
    };
    let Err(first_error) = wait_for_test_reply(group.rows().next_batch()) else {
        panic!("terminal transaction release should fail");
    };
    assert!(matches!(first_error, RowProviderError::Store { .. }));
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
fn operation_scoped_incremental_preparation_failure_retries_the_same_group() {
    let alice_member = alice_member();
    let inner_store = sqlite_store(alice_member.clone());
    let store = Arc::new(FailingStore::new(inner_store.clone()));
    provision_test_security(store.as_ref(), &alice_member, []);
    let seed_builder = runtime_builder(
        app_probe_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    );
    let seed_runtime = load_runtime(seed_builder);
    let group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("incremental retry group should be created");
    let source_token = seed_runtime.group_read_token_for_test(group_id);
    let target_receipt = publish_changes(
        seed_runtime.as_ref(),
        source_token.clone(),
        vec![title_upsert(
            test_row_id(group_id, docs_dataset_id(), 70_125_051),
            "after source",
        )],
    );
    wait_for_test_reply(seed_runtime.shutdown()).expect("seed runtime should shut down");

    let PreparedRuntimeStartup {
        endpoint_lease: _endpoint_lease,
        load,
    } = prepare_runtime_startup(
        store.clone(),
        Some(ApplicationReadToken::from(source_token)),
        ReplicationConfig::default(),
    );
    let ReplicationRuntimeLoad::Synchronising(mut synchronisation) = load else {
        panic!("a behind group should require incremental preparation");
    };
    let loads_before_retry = store.replication_update_load_count();
    let classification = retryable_store_failure(StoreErrorScope::Operation);
    store.fail_next_replication_update_load(classification);

    let Err(first_error) = wait_for_test_reply(synchronisation.next_group()) else {
        panic!("the injected operation failure should surface");
    };
    assert_eq!(
        first_error.store_error_classification(),
        Some(classification)
    );
    assert!(matches!(first_error, RowProviderError::Store { .. }));
    assert_eq!(
        store.replication_update_load_count(),
        loads_before_retry + 1
    );

    let group = wait_for_test_reply(synchronisation.next_group())
        .expect("the operation-scoped failure should be retryable")
        .expect("the same incremental group should remain pending");
    let SingleGroupSynchronisation::Changes(mut changes) = group else {
        panic!("the retried group should retain its incremental classification");
    };
    assert_eq!(
        changes.position().group_read_token(),
        &target_receipt.read_token
    );
    assert_eq!(
        store.replication_update_load_count(),
        loads_before_retry + 2,
        "retry should repeat the failed range load in the retained transaction"
    );
    while wait_for_test_reply(changes.rows().next_batch())
        .expect("retried incremental rows should drain")
        .is_some()
    {
        // The test only needs to prove successful preparation and exhaustion.
    }
    drop(changes);
    let runtime = wait_for_test_reply(synchronisation.complete())
        .expect("successful incremental retry should permit activation");
    wait_for_test_reply(runtime.shutdown()).expect("retried runtime should shut down");
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
    store.fail_next_snapshot_scan(retryable_store_failure(StoreErrorScope::Operation));

    let builder = ReplicationRuntime::builder(app_probe_id())
        .application_schemas(&TITLE_APPLICATION_SCHEMAS)
        .store(store.clone())
        .listener(Arc::new(ListenerStub::default()))
        .runtime_config_toml(&runtime_config_toml);
    let inputs = builder
        .into_validated_with_security(security)
        .expect("test runtime inputs should be complete");
    let result = wait_for_test_reply(load_replication_runtime_typed_with_security_for_test(
        inputs,
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
    let seed_runtime = load_runtime(runtime_builder(
        app_probe_id(),
        store.clone(),
        Arc::new(ListenerStub::default()),
    ));
    let group_id = wait_for_test_reply(seed_runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
        ..Default::default()
    }))
    .expect("failure fixture group should be created");
    let initial_token = seed_runtime.group_read_token_for_test(group_id);
    publish_changes(
        seed_runtime.as_ref(),
        initial_token,
        vec![
            title_upsert(
                test_row_id(group_id, docs_dataset_id(), 70_126_001),
                "first page",
            ),
            title_upsert(
                test_row_id(group_id, docs_dataset_id(), 70_126_002),
                "failing page",
            ),
        ],
    );
    wait_for_test_reply(seed_runtime.shutdown()).expect("seed runtime should shut down");

    let config = ReplicationConfig {
        application_synchronisation_batch_size: NonZeroUsize::new(1)
            .expect("failure fixture batch size must be non-zero"),
        ..Default::default()
    };
    let staged = load_staged_startup_with_config(store.clone(), config);
    let mut synchronisation = staged.synchronisation;
    let classification = retryable_store_failure(StoreErrorScope::Transaction);

    let group = wait_for_test_reply(synchronisation.next_group())
        .expect("the prepared group should load")
        .expect("the prepared synchronisation should contain one group");
    let SingleGroupSynchronisation::Snapshot(mut group) = group else {
        panic!("the failure fixture should yield a snapshot");
    };
    let first_batch = wait_for_test_reply(group.rows().next_batch())
        .expect("the first SQLite page should load")
        .expect("the first SQLite page should contain one row");
    assert_eq!(first_batch.row_count(), 1);
    store.fail_next_snapshot_scan(classification);
    let first_error = wait_for_test_reply(group.rows().next_batch())
        .expect_err("the injected transaction failure should surface");
    assert_eq!(
        first_error.store_error_classification(),
        Some(classification)
    );
    assert!(matches!(first_error, RowProviderError::Store { .. }));
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
        Some(RowProviderError::Store { .. })
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
    let builder = ReplicationRuntime::builder(app_probe_id())
        .application_schemas(&TITLE_APPLICATION_SCHEMAS)
        .store(store.clone())
        .listener(Arc::new(ListenerStub::default()))
        .security_secrets(test_replication_security_secrets())
        .runtime_config_toml(&runtime_config_toml);
    let load = wait_for_test_reply(builder.load()).expect("closed group state should load");
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

    let builder = ReplicationRuntime::builder(application_id)
        .store(store.clone())
        .listener(listener)
        .security_secrets(security)
        .runtime_config_toml(&runtime_config_toml);
    let loaded_runtime = wait_for_test_reply(builder.load())
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
    let builder = runtime_builder(app_alice_id(), store.clone(), listener);
    let runtime = load_runtime(builder);

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
    let builder = runtime_builder(app_alice_id(), store.clone(), listener);
    let runtime = load_runtime(builder);
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
    let builder = runtime_builder(app_alice_id(), store.clone(), listener);
    let runtime = load_runtime(builder);
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
    let builder = runtime_builder(app_alice_id(), store.clone(), listener);
    let runtime = load_runtime(builder);

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

    let builder = ReplicationRuntime::builder(application_id.clone())
        .store(store.clone())
        .listener(listener)
        .security_secrets(test_replication_security_secrets());
    let loaded_runtime = wait_for_test_reply(builder.load());
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

/// Return the required builder fields reported before runtime loading begins.
fn missing_runtime_builder_fields(builder: ReplicationRuntimeBuilder) -> Box<[&'static str]> {
    let result = wait_for_test_reply(builder.load());
    let Err(LoadError::MissingBuilderInputs { missing_fields, .. }) = result else {
        panic!("incomplete runtime builder should report its missing inputs: {result:?}");
    };
    missing_fields
}

#[test]
fn runtime_builder_reports_all_missing_required_inputs_together() {
    let builder = ReplicationRuntime::builder(app_probe_id());
    let missing_fields = missing_runtime_builder_fields(builder);

    assert_eq!(
        missing_fields.as_ref(),
        ["store", "listener", "security_secrets"]
    );
}

#[test]
fn runtime_builder_reports_each_missing_required_input_before_loading() {
    let store_owner = sqlite_store(alice_member());
    let concrete_store = Arc::clone(&*store_owner);
    let store: Arc<dyn ReplicationStore> = concrete_store;
    let listener: Arc<dyn ReplicationEventListener> = Arc::new(ListenerStub::default());
    let security = test_replication_security_secrets();
    let missing_store = ReplicationRuntime::builder(app_probe_id())
        .listener(listener.clone())
        .security_secrets(security.clone());
    let missing_listener = ReplicationRuntime::builder(app_probe_id())
        .store(store.clone())
        .security_secrets(security.clone());
    let missing_security = ReplicationRuntime::builder(app_probe_id())
        .store(store)
        .listener(listener);
    let cases = [
        ("store", missing_store),
        ("listener", missing_listener),
        ("security_secrets", missing_security),
    ];

    for (expected, builder) in cases {
        let missing_fields = missing_runtime_builder_fields(builder);
        assert_eq!(missing_fields.as_ref(), [expected]);
    }
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

    let builder = ReplicationRuntime::builder(application_id.clone())
        .store(store.clone())
        .listener(listener)
        .security_secrets(wrong_security);
    let loaded_runtime = wait_for_test_reply(builder.load());
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

    let builder = ReplicationRuntime::builder(application_id.clone())
        .store(store.clone())
        .listener(listener)
        .security_secrets(test_replication_security_secrets());
    let loaded_runtime = wait_for_test_reply(builder.load());
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

    let builder = ReplicationRuntime::builder(application_id.clone())
        .store(store.clone())
        .listener(listener)
        .security_secrets(test_replication_security_secrets());
    let loaded_runtime = wait_for_test_reply(builder.load());
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

    let builder = ReplicationRuntime::builder(application_id.clone())
        .store(store.clone())
        .listener(listener)
        .security_secrets(test_replication_security_secrets());
    let loaded_runtime = wait_for_test_reply(builder.load());
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

    let builder = runtime_builder(app_alice_id(), store.clone(), listener);
    let runtime = load_runtime(builder);

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

    let builder = runtime_builder(app_alice_id(), store.clone(), listener);
    let runtime = load_runtime(builder);

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
    let builder = runtime_builder(app_alice_id(), store.clone(), listener)
        .runtime_config_toml(runtime_config_toml.as_str());
    let runtime = load_runtime(builder);

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
    let bob_builder = runtime_builder(app_bob_id(), bob_store.clone(), bob_listener);
    let bob_runtime = load_runtime(bob_builder);
    wait_for_group_install(&bob_runtime, group_id);

    let alice_store = sqlite_store(alice_member.clone());
    provision_test_security(alice_store.as_ref(), &alice_member, [bob_member.clone()]);
    persist_group_membership_for_member(alice_store.as_ref(), group_id, members, 0);
    let alice_listener = Arc::new(ListenerStub::default());
    let runtime_config_toml = static_peer_route_toml(
        &bob_member,
        bob_runtime.advertised_loopback_udp_addr_for_test(),
    );
    let alice_builder = runtime_builder(app_alice_id(), alice_store.clone(), alice_listener)
        .runtime_config_toml(runtime_config_toml.as_str());
    let alice_runtime = load_runtime(alice_builder);
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
    let args = DeliveryRuntimeHostPrepareArgs {
        local_member: &alice_member,
        group_memberships: group_state,
        store: store.clone(),
        config: ReplicationConfig::default(),
        security,
        runtime_config_fragments: smallvec::smallvec![runtime_config_toml],
    };
    let mut host = kompact::prelude::block_on(DeliveryRuntimeHost::prepare(args))
        .expect("host should prepare");
    kompact::prelude::block_on(host.activate_runtime(listener)).expect("host should start");
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
    let args = DeliveryRuntimeHostPrepareArgs {
        local_member: &alice_member,
        group_memberships: group_state,
        store: store.clone(),
        config: ReplicationConfig::default(),
        security,
        runtime_config_fragments: smallvec::smallvec![
            r"
                [flotsync.replication.runtime.catch-up]
                max-updates-per-batch = 0
                "
            .to_owned()
        ],
    };
    let mut host = kompact::prelude::block_on(DeliveryRuntimeHost::prepare(args))
        .expect("host should prepare");
    kompact::prelude::block_on(host.activate_runtime(listener))
        .expect("zero catch-up batch size should mean unlimited");
    host.wait_for_runtime_startup();
    wait_for_test_future(host.shutdown()).expect("host should shut down cleanly");
}

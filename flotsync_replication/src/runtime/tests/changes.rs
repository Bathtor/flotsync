//! Local change publication, snapshots, and rebase scenarios.

use super::*;

#[test]
fn publish_changes_persists_applied_update_and_snapshot_state() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id.clone(), 34);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "stored publish",
            },
        }],
    );

    let persisted_group = load_persisted_group(fixture.store.as_ref(), group_id);
    assert_eq!(persisted_group.version_vector.version_at(0), 1);
    let persisted_update =
        load_persisted_update(fixture.store.as_ref(), group_id, receipt.update_id)
            .expect("published update should persist");
    assert!(persisted_update.applied_locally);
    let row_slice = load_persisted_row_slice(
        fixture.store.as_ref(),
        group_id,
        &dataset_id,
        [row_id.row_key],
    );
    assert!(row_slice.dataset_exists);
    assert!(
        row_slice
            .rows
            .get(&row_id.row_key)
            .cloned()
            .flatten()
            .is_some()
    );
}

#[test]
fn publish_changes_linear_string_update_with_two_insert_hunks_reuses_operation_id() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id.clone(), 121_000);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let insert_receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "a",
            },
        }],
    );

    let update_receipt = publish_changes(
        fixture.runtime.as_ref(),
        insert_receipt.read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "xay",
            },
        }],
    );

    assert_eq!(update_receipt.update_id.version, 2);
    assert_eq!(
        snapshot_string_field(
            fixture.runtime.as_ref(),
            group_id,
            dataset_id,
            &row_id,
            "title",
        ),
        "xay"
    );
}

#[test]
fn request_summary_reports_local_versions() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: test_row_id(group_id, dataset_id, 120_001),
            row: crate::row_values! {
                "title" => "local summary",
            },
        }],
    );

    let summary = wait_for_test_reply(fixture.runtime.request_summary(SummaryRequest {
        group_id,
        target: alice_member.clone(),
    }))
    .expect("summary request should succeed");

    assert_eq!(summary.responder, alice_member);
    assert_eq!(
        summary.has_versions,
        VersionVector::initial(NonZeroUsize::new(1).expect("one member"))
            .with_update_applied(receipt.update_id)
    );
}

#[test]
fn snapshot_rows_streams_visible_rows_and_optional_tombstones() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    let active_row_id = test_row_id(group_id, dataset_id.clone(), 35);
    let deleted_row_id = test_row_id(group_id, dataset_id.clone(), 36);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![
            RowMutation::Upsert {
                row_id: active_row_id.clone(),
                row: crate::row_values! {
                    "title" => "still visible",
                },
            },
            RowMutation::Upsert {
                row_id: deleted_row_id.clone(),
                row: crate::row_values! {
                    "title" => "now deleted",
                },
            },
        ],
    );
    publish_changes(
        fixture.runtime.as_ref(),
        receipt.read_token,
        vec![RowMutation::Delete {
            row_id: deleted_row_id.clone(),
        }],
    );

    let mut visible_rows = drain_snapshot_rows(
        fixture.runtime.as_ref(),
        SnapshotRowsRequest {
            group_id,
            datasets: HashSet::from([dataset_id.clone()]),
            max_rows_per_batch: NonZeroUsize::new(1).expect("batch size should be non-zero"),
            include_tombstones: false,
        },
    );
    sort_captured_rows(&mut visible_rows);

    assert_eq!(
        visible_rows,
        vec![CapturedRowChange::Upsert {
            row_id: active_row_id.clone(),
            title: "still visible".to_owned(),
        }]
    );

    let mut all_rows = drain_snapshot_rows(
        fixture.runtime.as_ref(),
        SnapshotRowsRequest {
            group_id,
            datasets: HashSet::from([dataset_id]),
            max_rows_per_batch: NonZeroUsize::new(1).expect("batch size should be non-zero"),
            include_tombstones: true,
        },
    );
    sort_captured_rows(&mut all_rows);

    assert_eq!(
        all_rows,
        vec![
            CapturedRowChange::Upsert {
                row_id: active_row_id,
                title: "still visible".to_owned(),
            },
            CapturedRowChange::Delete {
                row_id: deleted_row_id,
            },
        ]
    );
}

#[test]
fn publish_changes_emits_local_data_changed_event_before_reply() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id, 39);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, docs_dataset_id());
    publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "local event",
            },
        }],
    );

    assert_eq!(
        fixture.listener.captured_data_changes(),
        vec![CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id,
                title: "local event".to_owned(),
            }],
        }]
    );
}

#[test]
fn change_group_membership_emits_inline_snapshot_upserts_for_new_group() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let store = wait_for_test_future(SqliteReplicationStore::in_memory_with_schema_sources(
        alice_member.clone(),
        [(
            dataset_id.clone(),
            SchemaSource::from(title_schema_shared()),
        )],
    ))
    .expect("store should build");
    let store = Arc::new(store);
    provision_test_security(store.as_ref(), &alice_member, [bob_member.clone()]);
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());
    let old_group_id = wait_for_test_reply(runtime.create_group(CreateGroupRequest {
        members: vec![alice_member.clone()],
        group_schema: docs_group_schema(),
    }))
    .expect("create_group should succeed");
    let old_row_id = test_row_id(old_group_id, dataset_id.clone(), 40);

    let read_token = snapshot_read_token(runtime.as_ref(), old_group_id, dataset_id.clone());
    publish_changes(
        runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: old_row_id.clone(),
            row: crate::row_values! {
                "title" => "membership snapshot",
            },
        }],
    );
    listener.wait_for_data_change_count(1);
    let previous_event_count = listener.captured_data_changes().len();

    let migration_id = wait_for_test_reply(runtime.change_group_membership(
        ChangeGroupMembershipRequest {
            group_id: old_group_id,
            add_members: HashSet::from([bob_member]),
            remove_members: HashSet::new(),
            group_name: None,
            message: None,
        },
    ))
    .expect("membership change should succeed");

    listener.wait_for_data_change_count(previous_event_count + 1);
    let data_changes = listener.captured_data_changes();
    let migration_change = &data_changes[previous_event_count];
    let new_row_id = RowId {
        group_id: migration_id.new_group_id,
        dataset_id,
        row_key: old_row_id.row_key,
    };
    assert_eq!(
        migration_change,
        &CapturedDataChange {
            rows: vec![CapturedRowChange::Upsert {
                row_id: new_row_id,
                title: "membership snapshot".to_owned(),
            }],
        }
    );
    let old_group = load_persisted_group(store.as_ref(), old_group_id);
    assert_eq!(
        old_group.lifecycle,
        ReplicationGroupLifecycle::Closed {
            successor_group_id: migration_id.new_group_id,
            final_versions: old_group.version_vector.clone(),
        }
    );
    let migration_read_token = listener
        .captured_data_change_read_tokens()
        .last()
        .cloned()
        .expect("migration listener event should carry a read token");
    assert!(migration_read_token.group_version(&old_group_id).is_none());
    assert!(
        migration_read_token
            .group_version(&migration_id.new_group_id)
            .is_some()
    );
    assert!(
        wait_for_test_reply(runtime.snapshot_rows(SnapshotRowsRequest {
            group_id: old_group_id,
            datasets: HashSet::from([docs_dataset_id()]),
            max_rows_per_batch: NonZeroUsize::new(1).unwrap(),
            include_tombstones: false,
        }))
        .is_err()
    );
    assert!(
        wait_for_test_reply(runtime.request_summary(SummaryRequest {
            group_id: old_group_id,
            target: alice_member.clone(),
        }))
        .is_err()
    );
}

#[test]
fn read_only_group_allows_reads_but_rejects_application_writes() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let group_id = GroupId(Uuid::from_u128(50_601));
    let successor_group_id = GroupId(Uuid::from_u128(50_602));
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    let versions = VersionVector::initial(NonZeroUsize::new(1).unwrap());
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(vec![alice_member.clone()]),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: versions.clone(),
            lifecycle: ReplicationGroupLifecycle::ReadOnly {
                successor_group_id,
                final_versions: versions.clone(),
            },
            security_material: current_slice_placeholder_group_security_material(group_id),
        },
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store, listener);

    let snapshot = wait_for_test_reply(runtime.snapshot_rows(SnapshotRowsRequest {
        group_id,
        datasets: HashSet::from([dataset_id.clone()]),
        max_rows_per_batch: NonZeroUsize::new(1).unwrap(),
        include_tombstones: false,
    }))
    .expect("read-only group should remain snapshot-readable");
    assert!(snapshot.read_token.group_version(&group_id).is_none());
    drop(snapshot);
    wait_for_test_reply(runtime.request_summary(SummaryRequest {
        group_id,
        target: alice_member.clone(),
    }))
    .expect("read-only group should permit application summaries");
    assert!(
        wait_for_test_reply(runtime.publish_changes(PublishChangesRequest {
            read_token: ReadToken::from_group_versions(HashMap::from([(group_id, versions)])),
            changes: vec![RowMutation::Upsert {
                row_id: test_row_id(group_id, dataset_id, 50_603),
                row: crate::row_values! { "title" => "rejected" },
            }],
        }))
        .is_err()
    );
    assert!(
        wait_for_test_reply(
            runtime.change_group_membership(ChangeGroupMembershipRequest {
                group_id,
                add_members: HashSet::new(),
                remove_members: HashSet::new(),
                group_name: None,
                message: None,
            })
        )
        .is_err()
    );
}

#[test]
fn rebased_local_upsert_applies_only_staged_field_to_current_dataset() {
    let group_id = GroupId(Uuid::from_u128(710));
    let dataset_id = docs_dataset_id();
    let row_id = test_row_id(group_id, dataset_id, 42);
    let mut base_dataset = LocalDataset::new(title_note_schema_shared());
    let mut current_dataset = LocalDataset::new(title_note_schema_shared());

    let insert_update_id = UpdateId {
        version: 1,
        node_index: 0,
    };
    for dataset in [&mut base_dataset, &mut current_dataset] {
        apply_local_upsert(
            dataset,
            &row_id,
            crate::row_values! {
                "title" => "original title",
                "note" => "original note",
            },
            insert_update_id,
        )
        .expect("insert should apply")
        .expect("insert should produce an operation");
    }
    apply_local_upsert(
        &mut current_dataset,
        &row_id,
        crate::row_values! {
            "note" => "newer note",
        },
        UpdateId {
            version: 2,
            node_index: 0,
        },
    )
    .expect("note update should apply")
    .expect("note update should produce an operation");

    apply_rebased_local_upsert(
        &mut base_dataset,
        &mut current_dataset,
        &row_id,
        crate::row_values! {
            "title" => "original title updated",
        },
        UpdateId {
            version: 3,
            node_index: 0,
        },
    )
    .expect("rebased title update should apply")
    .expect("rebased title update should produce an operation");

    let row = current_dataset
        .data
        .get_row(&row_id.row_key.0)
        .expect("row should exist");
    assert_eq!(
        row.get_field_value::<str>("title")
            .expect("title should decode")
            .as_ref(),
        "original title updated"
    );
    assert_eq!(
        row.get_field_value::<str>("note")
            .expect("note should decode")
            .as_ref(),
        "newer note"
    );
}

#[test]
fn publish_changes_rebases_stale_field_patch_without_overwriting_newer_fields() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), title_note_schema_shared())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema_from_schema(title_note_schema_shared()),
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id.clone(), 41);

    let initial_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, dataset_id.clone());
    let insert_receipt = publish_changes(
        fixture.runtime.as_ref(),
        initial_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "original title",
                "note" => "original note",
            },
        }],
    );
    let stale_edit_token = insert_receipt.read_token.clone();

    let note_receipt = publish_changes(
        fixture.runtime.as_ref(),
        insert_receipt.read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "note" => "newer note",
            },
        }],
    );
    assert_eq!(note_receipt.update_id.version, 2);

    let title_receipt = publish_changes(
        fixture.runtime.as_ref(),
        stale_edit_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "original title updated",
            },
        }],
    );
    assert_eq!(title_receipt.update_id.version, 3);

    assert_eq!(
        snapshot_string_field(
            fixture.runtime.as_ref(),
            group_id,
            dataset_id.clone(),
            &row_id,
            "title",
        ),
        "original title updated"
    );
    assert_eq!(
        snapshot_string_field(
            fixture.runtime.as_ref(),
            group_id,
            dataset_id,
            &row_id,
            "note"
        ),
        "newer note"
    );
}

#[test]
fn publish_changes_error_display_includes_local_operation_source() {
    let alice_member = alice_member();
    let dataset_id = docs_dataset_id();
    let schema = Arc::new(Schema::from_fields([
        Field::linear_string("title"),
        Field::monotonic_counter("edit_count"),
    ]));
    let fixture = load_runtime_fixture(
        app_alice_id(),
        alice_member.clone(),
        [(dataset_id.clone(), schema.clone())],
    );
    let group_id = wait_for_test_reply(fixture.runtime.create_group(CreateGroupRequest {
        members: vec![alice_member],
        group_schema: docs_group_schema_from_schema(schema),
    }))
    .expect("create_group should succeed");
    let row_id = test_row_id(group_id, dataset_id, 40);

    let read_token = snapshot_read_token(fixture.runtime.as_ref(), group_id, docs_dataset_id());
    let receipt = publish_changes(
        fixture.runtime.as_ref(),
        read_token,
        vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "counted row",
                "edit_count" => 5_u64,
            },
        }],
    );

    let error = wait_for_test_reply(fixture.runtime.publish_changes(PublishChangesRequest {
        read_token: receipt.read_token,
        changes: vec![RowMutation::Upsert {
            row_id,
            row: crate::row_values! {
                "edit_count" => 4_u64,
            },
        }],
    }))
    .expect_err("counter decrease should fail publish");
    let message = error.to_string();

    assert!(
        message.contains("monotonic and cannot decrease from 5 to 4"),
        "{message}"
    );
}

#[test]
fn publish_changes_rejects_reserved_local_update_version() {
    let alice_member = alice_member();
    let bob_member = bob_member();
    let dataset_id = docs_dataset_id();
    let group_id = GroupId(Uuid::from_u128(40_101));
    let member_count = NonZeroUsize::new(2).expect("group has two members");
    let version_vector = VersionVector::initial(member_count).with_update_applied(UpdateId {
        node_index: 0,
        version: MAX_VERSION_VALUE,
    });
    let store = sqlite_store_with_schemas(
        alice_member.clone(),
        [(dataset_id.clone(), title_schema_static())],
    );
    persist_group_in_store(
        store.as_ref(),
        ReplicationGroupRecord {
            group_id,
            member_keys: test_group_member_keys(vec![alice_member.clone(), bob_member]),
            local_member_index: MemberIndex::new(0),
            group_schema: docs_group_schema(),
            version_vector: version_vector.clone(),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material(group_id),
        },
    );
    let listener = Arc::new(ListenerStub::default());
    let runtime = load_runtime_with_parts(app_alice_id(), store.clone(), listener.clone());
    let row_id = test_row_id(group_id, dataset_id, 40_102);
    let read_token = ReadToken::from_group_versions(HashMap::from([(group_id, version_vector)]));

    let error = wait_for_test_reply(runtime.publish_changes(PublishChangesRequest {
        read_token,
        changes: vec![RowMutation::Upsert {
            row_id,
            row: crate::row_values! {
                "title" => "too late",
            },
        }],
    }))
    .expect_err("reserved local update version should fail publish");

    match error {
        ApiError::ApiExternal { source } => match source.downcast_ref::<PublishChangesError>() {
            Some(PublishChangesError::ExhaustedUpdateIds {
                group_id: exhausted_group_id,
            }) => assert_eq!(*exhausted_group_id, group_id),
            other => panic!("unexpected publish error source: {other:?}"),
        },
        error => panic!("unexpected API error: {error:?}"),
    }
    assert_eq!(
        load_persisted_group(store.as_ref(), group_id).version_vector,
        VersionVector::initial(member_count).with_update_applied(UpdateId {
            node_index: 0,
            version: MAX_VERSION_VALUE,
        })
    );
    assert_eq!(
        load_persisted_update(
            store.as_ref(),
            group_id,
            UpdateId {
                node_index: 0,
                version: u64::MAX,
            },
        ),
        None
    );
    assert!(listener.captured_data_changes().is_empty());
}

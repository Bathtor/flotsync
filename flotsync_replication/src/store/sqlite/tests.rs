//! SQLite store tests.
use super::*;
use crate::{
    api::{
        DatasetRowStatePatch,
        DatasetRowStateWrite,
        GroupInvitation,
        GroupSchema,
        InitialDatasetValueRows,
        InitialGroupValueRows,
        InitialSnapshot,
        InitialSnapshotMetadata,
        InitialValueRow,
        MemberKeyTrustEvidenceKind,
        MemberKeyTrustEvidenceRecord,
        MemberPublicKeysRecord,
        MigrationId,
        MigrationProposal,
        PendingGroupDecisionRecord,
        ReplicationRowStateRecord,
        ReplicationUpdateFilter,
        SnapshotRef,
        current_slice_placeholder_group_security_material,
    },
    test_support::test_public_member_keys,
};
use flotsync_core::member::{Identifier, MAX_IDENTIFIER_SEGMENTS};
use flotsync_data_types::{
    Field,
    RowValues,
    Schema,
    TableOperations,
    schema::datamodel::RowOperation,
};
use flotsync_messages::codecs::datamodel::encode_schema_operation;
use itertools::Itertools;
use std::{
    assert_matches,
    collections::{HashMap, HashSet},
    time::Duration,
};

const STORE_FUTURE_TIMEOUT: Duration = Duration::from_secs(5);

fn wait_for_store_future<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    flotsync_io::test_support::wait_for_future(
        STORE_FUTURE_TIMEOUT,
        future,
        "timed out waiting for sqlite store future",
    )
}

fn in_memory_store(local_member: MemberIdentity) -> SqliteReplicationStore {
    wait_for_store_future(SqliteReplicationStore::in_memory(local_member))
        .expect("store should build")
}

fn in_memory_store_with_schema_sources<I, S>(
    local_member: MemberIdentity,
    schema_sources: I,
) -> SqliteReplicationStore
where
    I: IntoIterator<Item = (DatasetId, S)>,
    S: Into<SchemaSource>,
{
    wait_for_store_future(SqliteReplicationStore::in_memory_with_schema_sources(
        local_member,
        schema_sources,
    ))
    .expect("store should build")
}

fn docs_dataset_id() -> DatasetId {
    DatasetId::try_new("docs").expect("dataset id should build")
}

fn local_member() -> MemberIdentity {
    Identifier::from_array(["app", "alice"])
}

fn remote_member() -> MemberIdentity {
    Identifier::from_array(["app", "bob"])
}

fn third_member() -> MemberIdentity {
    Identifier::from_array(["app", "carol"])
}

fn initial_versions(member_count: usize) -> VersionVector {
    VersionVector::initial(NonZeroUsize::new(member_count).expect("member count is non-zero"))
}

fn metadata_snapshot(primary_group_id: GroupId, equivalent_group_id: GroupId) -> InitialSnapshot {
    InitialSnapshot::Metadata(InitialSnapshotMetadata {
        primary_ref: SnapshotRef {
            group_id: primary_group_id,
            versions: initial_versions(2),
        },
        equivalent_refs: smallvec::smallvec![SnapshotRef {
            group_id: equivalent_group_id,
            versions: initial_versions(3),
        }],
        record_count: Some(7),
    })
}

fn inline_snapshot() -> InitialSnapshot {
    let row = RowValues::try_from_fields(
        title_schema().as_ref(),
        crate::row_values! {
            "title" => "stored",
        }
        .fields,
    )
    .expect("inline snapshot fixture row should match docs schema");
    InitialSnapshot::Inline(InitialGroupValueRows {
        datasets: vec![InitialDatasetValueRows {
            dataset_id: docs_dataset_id(),
            rows: vec![InitialValueRow {
                row_key: RowKey(Uuid::from_u128(30_001)),
                row,
            }],
        }],
    })
}

fn creation_invitation_decision(group_id: GroupId) -> PendingGroupDecisionRecord {
    PendingGroupDecisionRecord::GroupInvitation(GroupInvitation::new_creation(
        group_id,
        vec![local_member(), remote_member()],
        GroupSchema::default(),
        InitialSnapshot::Empty,
        Some("shared docs".to_owned()),
        Some("join".to_owned()),
    ))
}

fn migration_invitation_decision(migration_id: MigrationId) -> PendingGroupDecisionRecord {
    PendingGroupDecisionRecord::GroupInvitation(GroupInvitation::new_migration(
        migration_id,
        vec![local_member(), remote_member(), third_member()],
        docs_group_schema(),
        inline_snapshot(),
        None,
        Some("migration invite".to_owned()),
    ))
}

fn migration_proposal_decision(migration_id: MigrationId) -> PendingGroupDecisionRecord {
    PendingGroupDecisionRecord::MigrationProposal(MigrationProposal {
        migration_id,
        final_versions: initial_versions(2).with_version_at(0, 3),
        proposed_members: vec![local_member(), remote_member(), third_member()],
        group_schema: GroupSchema::default(),
        initial_snapshot: metadata_snapshot(migration_id.old_group_id, migration_id.new_group_id),
        group_name: Some("new docs".to_owned()),
        message: None,
    })
}

#[test]
fn pending_group_work_state_decodes_stable_sql_values() {
    assert_matches!(
        PendingGroupWorkState::try_from("decision".to_owned()),
        Ok(PendingGroupWorkState::AwaitingDecision)
    );
    assert_matches!(
        PendingGroupWorkState::try_from("activation".to_owned()),
        Ok(PendingGroupWorkState::AcceptedActivation)
    );
    assert_matches!(
        PendingGroupWorkState::try_from("unknown".to_owned()),
        Err(PendingGroupSqlKeyError::UnknownWorkState { raw }) if raw == "unknown"
    );
}

#[test]
fn pending_group_decisions_round_trip_through_sqlite_payloads() {
    let store = in_memory_store(local_member());
    let migration_id = MigrationId {
        old_group_id: GroupId(Uuid::from_u128(10_001)),
        new_group_id: GroupId(Uuid::from_u128(10_002)),
    };
    let proposal_migration_id = MigrationId {
        old_group_id: migration_id.old_group_id,
        new_group_id: GroupId(Uuid::from_u128(10_003)),
    };
    let records = vec![
        creation_invitation_decision(GroupId(Uuid::from_u128(10_000))),
        migration_invitation_decision(migration_id),
        migration_proposal_decision(proposal_migration_id),
    ];

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .insert_replication_group(sample_group(migration_id.old_group_id))
            .await
            .expect("old group should store");
        for record in records.iter().cloned() {
            let (material, _) = sample_group(record.group_id()).into_parts();
            transaction
                .ensure_replication_group_material(material)
                .await
                .expect("target group material should store");
            transaction
                .upsert_pending_group_decision(record)
                .await
                .expect("pending decision should store");
        }
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let loaded = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let loaded = transaction
            .load_pending_group_decisions()
            .await
            .expect("pending decisions should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        loaded
    });

    assert_eq!(loaded.len(), records.len());
    for record in records {
        assert!(
            loaded.contains(&record),
            "loaded decisions should contain {record:?}"
        );
    }
}

#[test]
fn pending_group_activation_remove_is_idempotent() {
    let store = in_memory_store(local_member());
    let migration_id = MigrationId {
        old_group_id: GroupId(Uuid::from_u128(11_001)),
        new_group_id: GroupId(Uuid::from_u128(11_002)),
    };
    let record = migration_proposal_decision(migration_id).into_activation();
    let key = record.key();

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .insert_replication_group(sample_group(migration_id.old_group_id))
            .await
            .expect("old group should store");
        let (material, _) = sample_group(migration_id.new_group_id).into_parts();
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("target group material should store");
        transaction
            .upsert_pending_group_activation(record.clone())
            .await
            .expect("pending activation should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let loaded = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let loaded = transaction
            .load_pending_group_activations()
            .await
            .expect("pending activations should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        loaded
    });
    assert_eq!(loaded, vec![record]);

    let (first_removed, second_removed) = wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        let first_removed = transaction
            .remove_pending_group_activation(key)
            .await
            .expect("first remove should succeed");
        let second_removed = transaction
            .remove_pending_group_activation(key)
            .await
            .expect("second remove should succeed");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
        (first_removed, second_removed)
    });
    assert!(first_removed);
    assert!(!second_removed);
}

#[test]
fn pending_group_work_accepts_replay_and_rejects_conflicting_target_work() {
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(11_100));
    let original = creation_invitation_decision(group_id);
    let mut conflicting = original.clone();
    let PendingGroupDecisionRecord::GroupInvitation(invitation) = &mut conflicting else {
        panic!("creation invitation fixture must contain an invitation");
    };
    invitation.message = Some("different invitation".to_owned());

    let conflict = wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        let (material, _) = sample_group(group_id).into_parts();
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("target group material should store");
        transaction
            .upsert_pending_group_decision(original.clone())
            .await
            .expect("pending decision should store");
        transaction
            .upsert_pending_group_decision(original.clone())
            .await
            .expect("exact pending decision replay should be idempotent");
        let conflict = transaction
            .upsert_pending_group_decision(conflicting)
            .await
            .expect_err("conflicting target work should be rejected");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
        conflict
    });

    assert!(is_conflicting_pending_group_work(&conflict, group_id));
    let loaded = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let loaded = transaction
            .load_pending_group_decision(&group_id)
            .await
            .expect("pending decision should load");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        loaded
    });
    assert_eq!(loaded, Some(original));
}

#[test]
fn pending_group_decision_transitions_to_one_activation_for_target_group() {
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(11_101));
    let decision = creation_invitation_decision(group_id);
    let activation = decision.clone().into_activation();

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        let (material, _) = sample_group(group_id).into_parts();
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("target group material should store");
        transaction
            .upsert_pending_group_decision(decision)
            .await
            .expect("pending decision should store");
        transaction
            .upsert_pending_group_activation(activation.clone())
            .await
            .expect("pending decision should transition to activation");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let (loaded_decision, loaded_activation) = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let loaded_decision = transaction
            .load_pending_group_decision(&group_id)
            .await
            .expect("pending decision lookup should succeed");
        let loaded_activation = transaction
            .load_pending_group_activation(&group_id)
            .await
            .expect("pending activation lookup should succeed");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        (loaded_decision, loaded_activation)
    });
    assert_eq!(loaded_decision, None);
    assert_eq!(loaded_activation, Some(activation));
}

#[test]
fn inactive_group_material_is_not_active_and_cannot_own_data_state() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        in_memory_store_with_schema_sources(local_member(), [(dataset_id.clone(), schema.clone())]);
    let group_id = GroupId(Uuid::from_u128(11_102));
    let row_key = RowKey(Uuid::from_u128(11_103));
    let group = sample_group(group_id);
    let (material, _) = group.clone().into_parts();

    wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .ensure_replication_group_material(material)
            .await
            .expect("inactive group material should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");
    });

    let (active_group, active_groups, stored_material) = wait_for_store_future(async {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .expect("read transaction should open");
        let active_group = transaction
            .load_replication_group(&group_id)
            .await
            .expect("active group lookup should succeed");
        let active_groups = transaction
            .load_replication_groups()
            .await
            .expect("active groups should load");
        let stored_material = transaction
            .load_replication_group_material(&group_id)
            .await
            .expect("group material lookup should succeed");
        transaction
            .release()
            .await
            .expect("read transaction should release");
        (active_group, active_groups, stored_material)
    });
    assert_eq!(active_group, None);
    assert!(active_groups.is_empty());
    assert!(stored_material.is_some());

    let (row_error, update_error) = wait_for_store_future(async {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("transaction should open");
        let row_error = transaction
            .apply_dataset_row_patch(DatasetRowStatePatch {
                group_id,
                dataset_id: dataset_id.clone(),
                actions: vec![DatasetRowStateWrite::UpsertActive {
                    row_key,
                    snapshot: title_snapshot(&schema, row_key, "inactive"),
                }],
                last_changed_versions: sample_last_changed_versions(),
            })
            .await
            .expect_err("inactive material must not own row state");
        let update_error = transaction
            .append_replication_update(ReplicationUpdateRecord {
                group_id,
                update_id: UpdateId {
                    node_index: 0,
                    version: 1,
                },
                sender: local_member(),
                read_versions: VersionVector::initial(group.member_count()),
                dataset_updates: Vec::new(),
                applied_locally: false,
            })
            .await
            .expect_err("inactive material must not own update state");
        transaction
            .rollback()
            .await
            .expect("transaction should roll back");
        (row_error, update_error)
    });
    assert!(matches!(row_error, StoreError::StoreExternal { .. }));
    assert!(matches!(update_error, StoreError::StoreExternal { .. }));
}

#[test]
fn stored_member_identity_rejects_overlong_identifier() {
    let raw = std::iter::repeat_n("s", MAX_IDENTIFIER_SEGMENTS + 1).join(".");

    let error = decode_member_identity(&raw).unwrap_err();

    assert_matches!(error, StoreError::StoreExternal { .. });
    let StoreError::StoreExternal { source } = error;
    let sqlite_error = source
        .downcast_ref::<SqliteStoreError>()
        .expect("store external error should preserve sqlite store error");

    assert_matches!(
        sqlite_error,
        SqliteStoreError::InvalidMemberIdentity {
            source: IdentifierParseError::ParseTooManySegmentsError {
                actual,
                ..
            },
            ..
        } if *actual == MAX_IDENTIFIER_SEGMENTS + 1
    );
}

fn title_schema() -> Arc<Schema> {
    Arc::new(Schema::from_fields([Field::linear_string("title")]))
}

fn docs_group_schema() -> GroupSchema {
    GroupSchema::new(HashMap::from([(
        docs_dataset_id(),
        SchemaSource::from(title_schema()),
    )]))
}

fn sample_encrypted_secret(seed: u8) -> EncryptedStoreSecret {
    EncryptedStoreSecret {
        crypto_version: StoreSecretCryptoVersion::new(1),
        key_id: StoreSecretKeyId::from_u128_for_test(u128::from(seed)),
        nonce: Vec::from([seed, seed.wrapping_add(1)]).into_boxed_slice(),
        ciphertext: Box::from([seed, seed.wrapping_add(1), seed.wrapping_add(2)]),
    }
}

fn sample_group(group_id: GroupId) -> ReplicationGroupRecord {
    let member_keys = [local_member(), remote_member()]
        .into_iter()
        .map(|member_id| MemberKeyId {
            fingerprint: test_public_member_keys(&member_id).fingerprint(),
            member_id,
        })
        .collect::<Vec<_>>();
    let member_keys = GroupMemberKeys::from_ordered_member_keys(member_keys)
        .expect("test group member keys should build");
    let mut version_vector = VersionVector::initial(NonZeroUsize::new(2).unwrap());
    version_vector.increment_at(0);
    ReplicationGroupRecord {
        group_id,
        member_keys,
        local_member_index: MemberIndex::new(0),
        group_schema: docs_group_schema(),
        version_vector,
        lifecycle: ReplicationGroupLifecycle::Open,
        security_material: current_slice_placeholder_group_security_material(group_id),
    }
}

fn is_conflicting_member_security_material(
    error: &StoreError,
    object: &'static str,
    member_id: &MemberIdentity,
) -> bool {
    match error {
        StoreError::StoreExternal { source } => matches!(
            source.downcast_ref::<SqliteStoreError>(),
            Some(SqliteStoreError::ConflictingMemberSecurityMaterial {
                object: stored_object,
                member_id: stored_member_id,
            }) if *stored_object == object && stored_member_id == member_id
        ),
    }
}

fn is_conflicting_pending_group_work(error: &StoreError, group_id: GroupId) -> bool {
    match error {
        StoreError::StoreExternal { source } => matches!(
            source.downcast_ref::<SqliteStoreError>(),
            Some(SqliteStoreError::ConflictingPendingGroupWork {
                group_id: stored_group_id,
            }) if *stored_group_id == group_id
        ),
    }
}

fn sample_last_changed_versions() -> VersionVector {
    let mut version_vector = VersionVector::initial(NonZeroUsize::new(2).unwrap());
    version_vector.increment_at(0);
    version_vector
}

fn insert_row_patch(
    group_id: GroupId,
    dataset_id: &DatasetId,
    row_key: RowKey,
    operation: &flotsync_messages::SchemaOperation<'_>,
) -> DatasetRowStatePatch {
    let RowOperation::Insert { snapshot, .. } = &operation.operation else {
        panic!("expected insert operation");
    };
    DatasetRowStatePatch {
        group_id,
        dataset_id: dataset_id.clone(),
        actions: vec![DatasetRowStateWrite::UpsertActive {
            row_key,
            snapshot: snapshot.clone().into_owned(),
        }],
        last_changed_versions: sample_last_changed_versions(),
    }
}

fn title_snapshot(
    schema: &Arc<Schema>,
    row_key: RowKey,
    title: &str,
) -> ReplicationRowStateSnapshot {
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            row_key.0,
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial(title)
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    let RowOperation::Insert { snapshot, .. } = operation.operation else {
        panic!("expected insert operation");
    };
    snapshot.into_owned()
}

fn encoded_insert_snapshot(
    title: &str,
    schema: &Arc<Schema>,
) -> flotsync_messages::datamodel::SchemaOperation {
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            Uuid::from_u128(30_001),
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial(title)
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode")
}

#[test]
fn dropping_open_sqlite_transaction_releases_store() {
    let store = Arc::new(in_memory_store(local_member()));
    let transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    drop(transaction);

    let (probe_result_tx, probe_result_rx) = std::sync::mpsc::channel();
    let store = store.clone();
    std::thread::spawn(move || {
        let probe_result = wait_for_store_future(store.begin_transaction()).map(|transaction| {
            wait_for_store_future(transaction.rollback())
                .expect("probe transaction should roll back");
        });
        let _ = probe_result_tx.send(probe_result);
    });

    let probe_result = probe_result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("dropping an open transaction should release the SQLite store promptly");
    probe_result.expect("dropped transaction should leave the SQLite store usable");
}

#[test]
fn sqlite_store_roundtrips_replication_group_lifecycle() {
    let store = in_memory_store(local_member());
    let group_id = GroupId(Uuid::from_u128(99));
    let successor_group_id = GroupId(Uuid::from_u128(100));
    let group = sample_group(group_id);
    let final_versions = group.version_vector.clone();
    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group))
        .expect("group should insert");
    wait_for_store_future(transaction.update_replication_group_lifecycle(
        &group_id,
        ReplicationGroupLifecycle::ReadOnly {
            successor_group_id,
            final_versions: final_versions.clone(),
        },
    ))
    .expect("read-only lifecycle should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let stored = wait_for_store_future(transaction.load_replication_group(&group_id))
        .expect("group should load")
        .expect("group should exist");
    assert_eq!(
        stored.lifecycle,
        ReplicationGroupLifecycle::ReadOnly {
            successor_group_id,
            final_versions: final_versions.clone(),
        }
    );
    wait_for_store_future(transaction.release()).expect("read should release");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.update_replication_group_lifecycle(
        &group_id,
        ReplicationGroupLifecycle::Closed {
            successor_group_id,
            final_versions: final_versions.clone(),
        },
    ))
    .expect("closed lifecycle should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let stored = wait_for_store_future(transaction.load_replication_group(&group_id))
        .expect("group should load")
        .expect("group should exist");
    assert_eq!(
        stored.lifecycle,
        ReplicationGroupLifecycle::Closed {
            successor_group_id,
            final_versions,
        }
    );
    wait_for_store_future(transaction.release()).expect("read should release");
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "This store roundtrip test keeps related group, dataset, and update assertions in one fixture."
)]
fn sqlite_store_roundtrips_group_dataset_and_update_records() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        in_memory_store_with_schema_sources(local_member(), [(dataset_id.clone(), schema.clone())]);
    let group_id = GroupId(Uuid::from_u128(101));
    let row_key = RowKey(Uuid::from_u128(202));
    let group = sample_group(group_id);
    let mut updated_version_vector = group.version_vector.clone();
    updated_version_vector.increment_at(1);
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            row_key.0,
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial("hello")
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    let encoded_operation =
        encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode");
    let row_patch = insert_row_patch(group_id, &dataset_id, row_key, &operation);
    let expected_row = match &row_patch.actions[0] {
        DatasetRowStateWrite::UpsertActive { row_key, snapshot } => ReplicationRowStateRecord {
            row_id: *row_key,
            snapshot: snapshot.clone(),
            tombstoned: false,
            last_changed_versions: row_patch.last_changed_versions.clone(),
        },
        DatasetRowStateWrite::UpsertTombstone { .. } => panic!("expected active row patch"),
    };
    let update = ReplicationUpdateRecord {
        group_id,
        update_id: UpdateId {
            node_index: 0,
            version: 1,
        },
        sender: local_member(),
        read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id: dataset_id.clone(),
            operations: vec![encoded_operation.clone()],
        }],
        applied_locally: false,
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group.clone()))
        .expect("group should store");
    wait_for_store_future(
        transaction
            .update_replication_group_version_vector(&group_id, updated_version_vector.clone()),
    )
    .expect("group version vector should update");
    wait_for_store_future(transaction.apply_dataset_row_patch(row_patch))
        .expect("row patch should store");
    wait_for_store_future(transaction.append_replication_update(update.clone()))
        .expect("update should store");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let loaded_group = wait_for_store_future(transaction.load_replication_group(&group_id))
        .expect("group should load")
        .expect("group should exist");
    assert_eq!(loaded_group.group_id, group.group_id);
    assert_eq!(loaded_group.member_keys, group.member_keys);
    assert_eq!(loaded_group.group_schema, group.group_schema);
    assert_eq!(loaded_group.security_material, group.security_material);
    assert_eq!(
        loaded_group.version_vector.iter().collect::<Vec<_>>(),
        updated_version_vector.iter().collect::<Vec<_>>()
    );

    let missing_row_key = RowKey(Uuid::from_u128(203));
    let requested_row_keys = [row_key, missing_row_key];
    let mut requested_row_keys = requested_row_keys.iter();
    let loaded_snapshot = wait_for_store_future(transaction.load_dataset_rows(
        &group_id,
        &dataset_id,
        &mut requested_row_keys,
    ))
    .expect("row slice should load");
    assert!(loaded_snapshot.dataset_exists);
    assert_eq!(loaded_snapshot.rows.len(), 2);
    assert_eq!(
        loaded_snapshot.rows.get(&row_key).cloned().flatten(),
        Some(expected_row)
    );
    assert_eq!(loaded_snapshot.rows.get(&missing_row_key), Some(&None));

    let loaded_update =
        wait_for_store_future(transaction.load_replication_update(&group_id, update.update_id))
            .expect("update should load")
            .expect("update should exist");
    assert_eq!(loaded_update, update);
    assert!(matches!(
        wait_for_store_future(
            transaction.load_replication_updates(
                &group_id,
                ReplicationUpdateFilter::PendingApply,
                None,
            )
        )
        .expect("updates should load")
        .as_slice(),
        [only] if only == &update
    ));
}

#[test]
fn sqlite_store_loads_replication_groups_by_requested_ids() {
    let store = in_memory_store(local_member());
    let first_group_id = GroupId(Uuid::from_u128(10_001));
    let second_group_id = GroupId(Uuid::from_u128(10_002));
    let missing_group_id = GroupId(Uuid::from_u128(10_003));
    let first_group = sample_group(first_group_id);
    let second_group = sample_group(second_group_id);

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(first_group))
        .expect("first group should store");
    wait_for_store_future(transaction.insert_replication_group(second_group))
        .expect("second group should store");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let requested_group_ids = HashSet::from([first_group_id, missing_group_id]);
    let mut write_transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let loaded_groups = wait_for_store_future(
        write_transaction.load_replication_groups_for_ids(&requested_group_ids),
    )
    .expect("requested groups should load through write transaction");
    assert_eq!(
        loaded_groups
            .iter()
            .map(|group| group.group_id)
            .collect::<Vec<_>>(),
        vec![first_group_id]
    );
    wait_for_store_future(write_transaction.rollback()).expect("rollback should succeed");

    let mut read_transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("transaction should start");
    let loaded_groups = wait_for_store_future(
        read_transaction.load_replication_groups_for_ids(&requested_group_ids),
    )
    .expect("requested groups should load through read transaction");
    assert_eq!(
        loaded_groups
            .iter()
            .map(|group| group.group_id)
            .collect::<Vec<_>>(),
        vec![first_group_id]
    );
    wait_for_store_future(read_transaction.release()).expect("release should succeed");
}

#[test]
fn sqlite_store_roundtrips_local_member_private_keys() {
    let store = in_memory_store(local_member());
    let record = LocalMemberPrivateKeysRecord {
        member_id: local_member(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: sample_encrypted_secret(42),
        },
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
        .expect("private keys should store");
    wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
        .expect("same private keys should be accepted");
    let loaded =
        wait_for_store_future(transaction.load_local_member_private_keys(&record.member_id))
            .expect("private keys should load")
            .expect("private keys should exist");
    assert_eq!(loaded, record);
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut read_transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("transaction should start");
    let loaded =
        wait_for_store_future(read_transaction.load_local_member_private_keys(&record.member_id))
            .expect("private keys should load through read transaction")
            .expect("private keys should exist");
    assert_eq!(loaded, record);
    wait_for_store_future(read_transaction.release()).expect("release should succeed");
}

#[test]
fn sqlite_store_rejects_conflicting_local_member_private_keys() {
    let store = in_memory_store(local_member());
    let record = LocalMemberPrivateKeysRecord {
        member_id: local_member(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: sample_encrypted_secret(42),
        },
    };
    let conflicting_record = LocalMemberPrivateKeysRecord {
        member_id: record.member_id.clone(),
        private_keys: EncryptedLocalMemberPrivateKeys {
            secret: sample_encrypted_secret(43),
        },
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.ensure_local_member_private_keys(record.clone()))
        .expect("private keys should store");
    let error =
        wait_for_store_future(transaction.ensure_local_member_private_keys(conflicting_record))
            .expect_err("conflicting private keys should fail");
    assert!(is_conflicting_member_security_material(
        &error,
        "local member private keys",
        &record.member_id,
    ));
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

#[test]
fn sqlite_store_roundtrips_member_public_keys_and_trust_evidence() {
    let store = in_memory_store(local_member());
    let record =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&remote_member()));
    let evidence = MemberKeyTrustEvidenceRecord {
        key_id: record.key_id.clone(),
        evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.ensure_member_public_keys(record.clone()))
        .expect("member public keys should store");
    wait_for_store_future(transaction.ensure_member_public_keys(record.clone()))
        .expect("same member public keys should be accepted");
    wait_for_store_future(transaction.ensure_member_key_trust_evidence(evidence.clone()))
        .expect("trust evidence should store");
    wait_for_store_future(transaction.ensure_member_key_trust_evidence(evidence.clone()))
        .expect("same trust evidence should be accepted");
    let loaded = wait_for_store_future(transaction.load_member_public_keys(&record.key_id))
        .expect("member public keys should load")
        .expect("member public keys should exist");
    assert_eq!(loaded, record);
    let loaded_for_member = wait_for_store_future(
        transaction.load_member_public_keys_for_member(&record.key_id.member_id),
    )
    .expect("member public keys should load by member");
    assert_eq!(loaded_for_member, vec![record.clone()]);
    let loaded_evidence =
        wait_for_store_future(transaction.load_member_key_trust_evidence(&record.key_id))
            .expect("trust evidence should load");
    assert!(loaded_evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust));
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut read_transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("transaction should start");
    let loaded = wait_for_store_future(read_transaction.load_member_public_keys(&record.key_id))
        .expect("member public keys should load through read transaction")
        .expect("member public keys should exist");
    assert_eq!(loaded, record);
    wait_for_store_future(read_transaction.release()).expect("release should succeed");
}

#[test]
fn sqlite_store_rejects_member_public_keys_with_mismatched_fingerprint() {
    let store = in_memory_store(local_member());
    let mut record =
        MemberPublicKeysRecord::from_public_keys(&test_public_member_keys(&remote_member()));
    record.key_id.fingerprint = test_public_member_keys(&local_member()).fingerprint();

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let error = wait_for_store_future(transaction.ensure_member_public_keys(record.clone()))
        .expect_err("mismatched member public keys should fail");
    assert!(is_conflicting_member_security_material(
        &error,
        "member public keys fingerprint",
        &record.key_id.member_id,
    ));
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

#[test]
fn sqlite_store_roundtrips_blocked_key_fingerprints() {
    let store = in_memory_store(local_member());
    let fingerprint = test_public_member_keys(&remote_member()).fingerprint();

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    assert!(
        !wait_for_store_future(transaction.is_key_fingerprint_blocked(&fingerprint))
            .expect("blocked fingerprint should load")
    );
    wait_for_store_future(transaction.ensure_blocked_key_fingerprint(fingerprint))
        .expect("blocked fingerprint should store");
    wait_for_store_future(transaction.ensure_blocked_key_fingerprint(fingerprint))
        .expect("same blocked fingerprint should be accepted");
    assert!(
        wait_for_store_future(transaction.is_key_fingerprint_blocked(&fingerprint))
            .expect("blocked fingerprint should load")
    );
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

#[test]
fn sqlite_store_filters_replication_updates_by_producer_range() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        in_memory_store_with_schema_sources(local_member(), [(dataset_id.clone(), schema.clone())]);
    let group_id = GroupId(Uuid::from_u128(10_011));
    let group = sample_group(group_id);
    let encoded_operation = encoded_insert_snapshot("range query", &schema);
    let update = |node_index, version, sender| ReplicationUpdateRecord {
        group_id,
        update_id: UpdateId {
            node_index,
            version,
        },
        sender,
        read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id: dataset_id.clone(),
            operations: vec![encoded_operation.clone()],
        }],
        applied_locally: true,
    };
    let alice_v1 = update(0, 1, local_member());
    let alice_v2 = update(0, 2, local_member());
    let alice_v3 = update(0, 3, local_member());
    let bob_v1 = update(1, 1, remote_member());

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group)).expect("group should store");
    for update in [
        alice_v1.clone(),
        alice_v2.clone(),
        alice_v3.clone(),
        bob_v1.clone(),
    ] {
        wait_for_store_future(transaction.append_replication_update(update))
            .expect("update should store");
    }

    let limited_alice = wait_for_store_future(transaction.load_replication_updates(
        &group_id,
        ReplicationUpdateFilter::ProducerRange {
            producer_index: MemberIndex::new(0),
            start_version: 2,
            end_version: 3,
        },
        NonZeroUsize::new(1),
    ))
    .expect("range should load");
    assert_eq!(limited_alice, vec![alice_v2.clone()]);

    let limited_alice_ids = wait_for_store_future(transaction.load_replication_update_ids(
        &group_id,
        ReplicationUpdateFilter::ProducerRange {
            producer_index: MemberIndex::new(0),
            start_version: 2,
            end_version: 3,
        },
        NonZeroUsize::new(1),
    ))
    .expect("range ids should load");
    assert_eq!(limited_alice_ids, vec![alice_v2.update_id]);

    let full_alice = wait_for_store_future(transaction.load_replication_updates(
        &group_id,
        ReplicationUpdateFilter::ProducerRange {
            producer_index: MemberIndex::new(0),
            start_version: 2,
            end_version: 3,
        },
        NonZeroUsize::new(4),
    ))
    .expect("range should load");
    assert_eq!(full_alice, vec![alice_v2, alice_v3]);

    let bob = wait_for_store_future(transaction.load_replication_updates(
        &group_id,
        ReplicationUpdateFilter::ProducerRange {
            producer_index: MemberIndex::new(1),
            start_version: 1,
            end_version: 3,
        },
        NonZeroUsize::new(4),
    ))
    .expect("range should load");
    assert_eq!(bob, vec![bob_v1]);
    wait_for_store_future(transaction.rollback()).expect("rollback should succeed");
}

#[test]
fn sqlite_store_roundtrips_tombstoned_dataset_rows() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        in_memory_store_with_schema_sources(local_member(), [(dataset_id.clone(), schema.clone())]);
    let group_id = GroupId(Uuid::from_u128(104));
    let row_key = RowKey(Uuid::from_u128(204));
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            row_key.0,
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial("deleted")
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    let RowOperation::Insert { snapshot, .. } = &operation.operation else {
        panic!("expected insert operation");
    };
    let stored_row = ReplicationRowStateRecord {
        row_id: row_key,
        snapshot: snapshot.clone().into_owned(),
        tombstoned: true,
        last_changed_versions: sample_last_changed_versions(),
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(transaction.apply_dataset_row_patch(DatasetRowStatePatch {
        group_id,
        dataset_id: dataset_id.clone(),
        actions: vec![DatasetRowStateWrite::UpsertTombstone {
            row_key,
            snapshot: stored_row.snapshot.clone(),
        }],
        last_changed_versions: stored_row.last_changed_versions.clone(),
    }))
    .expect("row patch should store");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let requested_row_keys = [row_key];
    let mut requested_row_keys = requested_row_keys.iter();
    let loaded_rows = wait_for_store_future(transaction.load_dataset_rows(
        &group_id,
        &dataset_id,
        &mut requested_row_keys,
    ))
    .expect("row slice should load");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    assert_eq!(
        loaded_rows.rows.get(&row_key).cloned().flatten(),
        Some(stored_row)
    );
}

#[test]
fn sqlite_store_scans_dataset_rows_in_key_order() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        in_memory_store_with_schema_sources(local_member(), [(dataset_id.clone(), schema.clone())]);
    let group_id = GroupId(Uuid::from_u128(106));
    let first_row_key = RowKey(Uuid::from_u128(206));
    let second_row_key = RowKey(Uuid::from_u128(207));

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(transaction.apply_dataset_row_patch(DatasetRowStatePatch {
        group_id,
        dataset_id: dataset_id.clone(),
        actions: vec![
            DatasetRowStateWrite::UpsertActive {
                row_key: second_row_key,
                snapshot: title_snapshot(&schema, second_row_key, "second"),
            },
            DatasetRowStateWrite::UpsertActive {
                row_key: first_row_key,
                snapshot: title_snapshot(&schema, first_row_key, "first"),
            },
        ],
        last_changed_versions: sample_last_changed_versions(),
    }))
    .expect("rows should store");
    wait_for_store_future(transaction.commit()).expect("transaction should commit");

    let mut transaction =
        wait_for_store_future(store.begin_read_transaction()).expect("read should start");
    let first_batch = wait_for_store_future(transaction.scan_dataset_row_batch(
        &group_id,
        &dataset_id,
        None,
        NonZeroUsize::new(1).expect("limit should be non-zero"),
    ))
    .expect("first batch should scan");
    let second_batch = wait_for_store_future(transaction.scan_dataset_row_batch(
        &group_id,
        &dataset_id,
        first_batch.next_after,
        NonZeroUsize::new(1).expect("limit should be non-zero"),
    ))
    .expect("second batch should scan");
    wait_for_store_future(transaction.release()).expect("read should release");

    assert_eq!(first_batch.rows[0].row_id, first_row_key);
    assert_eq!(first_batch.next_after, Some(first_row_key));
    assert_eq!(second_batch.rows[0].row_id, second_row_key);
    assert_eq!(second_batch.next_after, Some(second_row_key));
}

#[test]
fn sqlite_store_rejects_tombstone_to_active_row_transition() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        in_memory_store_with_schema_sources(local_member(), [(dataset_id.clone(), schema.clone())]);
    let group_id = GroupId(Uuid::from_u128(105));
    let row_key = RowKey(Uuid::from_u128(205));
    let tombstone_snapshot = title_snapshot(&schema, row_key, "deleted");
    let active_snapshot = title_snapshot(&schema, row_key, "resurrected");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(sample_group(group_id)))
        .expect("group should store");
    wait_for_store_future(transaction.apply_dataset_row_patch(DatasetRowStatePatch {
        group_id,
        dataset_id: dataset_id.clone(),
        actions: vec![DatasetRowStateWrite::UpsertTombstone {
            row_key,
            snapshot: tombstone_snapshot,
        }],
        last_changed_versions: sample_last_changed_versions(),
    }))
    .expect("missing-to-tombstone upsert should store");

    let error = wait_for_store_future(transaction.apply_dataset_row_patch(DatasetRowStatePatch {
        group_id,
        dataset_id,
        actions: vec![DatasetRowStateWrite::UpsertActive {
            row_key,
            snapshot: active_snapshot,
        }],
        last_changed_versions: sample_last_changed_versions(),
    }))
    .expect_err("tombstone-to-active upsert should fail");
    wait_for_store_future(transaction.rollback()).expect("transaction should roll back");

    assert!(matches!(
        error,
        StoreError::StoreExternal { ref source }
            if source.to_string().contains("cannot transition from tombstone to active")
    ));
}

#[test]
fn sqlite_store_rejects_duplicate_group_insert() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store = in_memory_store_with_schema_sources(local_member(), [(dataset_id, schema)]);
    let group_id = GroupId(Uuid::from_u128(303));
    let group = sample_group(group_id);

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group.clone()))
        .expect("group should store");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let error = wait_for_store_future(transaction.insert_replication_group(group))
        .expect_err("duplicate group insert should fail");
    assert!(matches!(error, StoreError::StoreExternal { .. }));
}

#[test]
fn sqlite_store_rejects_duplicate_update_insert_but_allows_applied_toggle() {
    let dataset_id = docs_dataset_id();
    let schema = title_schema();
    let store =
        in_memory_store_with_schema_sources(local_member(), [(dataset_id.clone(), schema.clone())]);
    let group_id = GroupId(Uuid::from_u128(404));
    let group = sample_group(group_id);
    let mut source_data = flotsync_messages::InMemoryStateData::new(schema.clone());
    let operation = source_data
        .insert_row(
            UpdateId {
                node_index: 0,
                version: 1,
            },
            Uuid::from_u128(505),
            vec![
                schema
                    .columns
                    .get("title")
                    .expect("title field should exist")
                    .initial("goodbye")
                    .expect("field value should build"),
            ],
        )
        .expect("row insert should succeed");
    let encoded_operation =
        encode_schema_operation(&operation, schema.as_ref()).expect("operation should encode");
    let update = ReplicationUpdateRecord {
        group_id,
        update_id: UpdateId {
            node_index: 0,
            version: u64::MAX - 1,
        },
        sender: local_member(),
        read_versions: VersionVector::initial(NonZeroUsize::new(2).unwrap()),
        dataset_updates: vec![DatasetUpdateRecord {
            dataset_id,
            operations: vec![encoded_operation],
        }],
        applied_locally: false,
    };

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    wait_for_store_future(transaction.insert_replication_group(group)).expect("group should store");
    wait_for_store_future(transaction.append_replication_update(update.clone()))
        .expect("update should store");
    let duplicate_error =
        wait_for_store_future(transaction.append_replication_update(update.clone()))
            .expect_err("duplicate update insert should fail");
    assert!(matches!(duplicate_error, StoreError::StoreExternal { .. }));
    wait_for_store_future(transaction.mark_replication_update_applied(&group_id, update.update_id))
        .expect("applied toggle should succeed");
    wait_for_store_future(transaction.commit()).expect("commit should succeed");

    let mut transaction =
        wait_for_store_future(store.begin_transaction()).expect("transaction should start");
    let loaded_update =
        wait_for_store_future(transaction.load_replication_update(&group_id, update.update_id))
            .expect("update should load")
            .expect("update should exist");
    assert!(loaded_update.applied_locally);
    assert_eq!(loaded_update.update_id.version, u64::MAX - 1);
}

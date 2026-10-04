//! Focused validation of incremental evidence and current-row contracts.

use super::{
    super::planning::{IncrementalCandidate, ReadableGroupSynchronisation},
    incremental::{
        CreationAtPosition,
        IncrementalPreparationError,
        UpdateRangePosition,
        UpdateRangeProgress,
        creation_at_position,
        ensure_known_dataset,
        prepare_incremental_group,
        validate_current_row_slice,
        validate_update_metadata,
    },
};
use crate::{
    api::{
        DatasetId,
        DatasetUpdateRecord,
        GroupMemberKeys,
        GroupReadToken,
        MemberKeyId,
        ReplicationGroupLifecycle,
        ReplicationGroupRecord,
        ReplicationStateRowBatch,
        ReplicationStore,
        RowKey,
        current_slice_placeholder_group_security_material,
    },
    test_support::{
        docs_dataset_id,
        docs_group_schema,
        provisioned_sqlite_store,
        test_public_member_keys,
        wait_for_test_future,
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::schema::Schema;
use flotsync_messages::{codecs::datamodel::OperationCodecError, datamodel::SchemaOperation};
use std::{collections::HashSet, num::NonZeroUsize, sync::Arc};
use uuid::Uuid;

/// Build one incremental candidate with a single current producer update.
fn incremental_candidate() -> IncrementalCandidate {
    let group_id = GroupId(Uuid::from_u128(10));
    let target_versions = VersionVector::initial(NonZeroUsize::MIN).with_update_applied(UpdateId {
        node_index: 0,
        version: 1,
    });
    IncrementalCandidate {
        group: ReadableGroupSynchronisation {
            group_id,
            read_token: GroupReadToken::from_group_version(group_id, target_versions),
            group_schema: Arc::new(crate::test_support::docs_group_schema()),
        },
        from_versions: VersionVector::initial(NonZeroUsize::MIN),
    }
}

/// Build one stored update for focused validation tests.
fn stored_update(
    candidate: &IncrementalCandidate,
    dataset_updates: Vec<crate::api::DatasetUpdateRecord>,
) -> crate::api::ReplicationUpdateRecord {
    crate::api::ReplicationUpdateRecord {
        group_id: candidate.group.group_id,
        update_id: UpdateId {
            node_index: 0,
            version: 1,
        },
        sender: MemberIdentity::from_array(["test", "member"]),
        read_versions: candidate.from_versions.clone(),
        dataset_updates,
        applied_locally: true,
    }
}

#[test]
fn row_creation_position_names_every_valid_outcome() {
    let versions = VersionVector::initial(NonZeroUsize::MIN).with_update_applied(UpdateId {
        node_index: 0,
        version: 1,
    });
    let group_id = GroupId(Uuid::from_u128(1));
    let dataset_id = DatasetId::try_from_static("docs").expect("dataset id should be valid");
    let row_key = RowKey(Uuid::from_u128(2));

    assert!(matches!(
        creation_at_position(None, &versions, group_id, &dataset_id, row_key),
        Ok(CreationAtPosition::Unknown)
    ));
    assert!(matches!(
        creation_at_position(
            Some(UpdateId::INITIAL_STATE_ORIGIN),
            &versions,
            group_id,
            &dataset_id,
            row_key,
        ),
        Ok(CreationAtPosition::Included)
    ));
    assert!(matches!(
        creation_at_position(
            Some(UpdateId {
                node_index: 0,
                version: 1,
            }),
            &versions,
            group_id,
            &dataset_id,
            row_key,
        ),
        Ok(CreationAtPosition::Included)
    ));
    assert!(matches!(
        creation_at_position(
            Some(UpdateId {
                node_index: 0,
                version: 2,
            }),
            &versions,
            group_id,
            &dataset_id,
            row_key,
        ),
        Ok(CreationAtPosition::NotIncluded)
    ));
}

#[test]
fn row_creation_position_rejects_a_creator_outside_the_group() {
    let versions = VersionVector::initial(NonZeroUsize::MIN);
    let group_id = GroupId(Uuid::from_u128(3));
    let dataset_id = DatasetId::try_from_static("docs").expect("dataset id should be valid");
    let row_key = RowKey(Uuid::from_u128(4));

    let result = creation_at_position(
        Some(UpdateId {
            node_index: 1,
            version: 1,
        }),
        &versions,
        group_id,
        &dataset_id,
        row_key,
    );

    assert!(matches!(
        result,
        Err(IncrementalPreparationError::CreatorOutsideGroup {
            group: actual_group_id,
            dataset: actual_dataset_id,
            row: actual_row_key,
            created_by: UpdateId {
                node_index: 1,
                version: 1,
            },
            member_count: 1,
        }) if actual_group_id == group_id
            && actual_dataset_id == dataset_id
            && actual_row_key == row_key
    ));
}

#[test]
fn stored_history_distinguishes_missing_evidence_from_malformed_records() {
    let candidate = incremental_candidate();
    let range = flotsync_core::versions::VersionVectorGap {
        member_index: 0,
        start_version: 1,
        end_version: 1,
    };
    let progress = UpdateRangeProgress::new(candidate.group.group_id, range);
    assert!(!progress.is_complete(), "an empty range needs a snapshot");

    let mut malformed = stored_update(&candidate, Vec::new());
    malformed.group_id = GroupId(Uuid::from_u128(11));
    assert!(matches!(
        UpdateRangeProgress::new(candidate.group.group_id, range)
            .accept(malformed.group_id, malformed.update_id),
        Err(IncrementalPreparationError::UnexpectedUpdate { .. })
    ));

    let mut self_dependent = stored_update(&candidate, Vec::new());
    self_dependent.read_versions.increment_at(0);
    assert!(matches!(
        validate_update_metadata(
            &candidate,
            self_dependent.group_id,
            self_dependent.update_id,
            &self_dependent.read_versions,
            self_dependent.applied_locally,
        ),
        Err(IncrementalPreparationError::SelfDependentReadVersions {
            producer_read_version: 1,
            ..
        })
    ));

    let first = stored_update(&candidate, Vec::new());
    let mut extra = first.clone();
    extra.update_id.version = 2;
    let mut progress = UpdateRangeProgress::new(candidate.group.group_id, range);
    assert!(matches!(
        progress.accept(first.group_id, first.update_id),
        Ok(UpdateRangePosition::Expected)
    ));
    assert!(progress.is_complete());
    assert!(matches!(
        progress.accept(extra.group_id, extra.update_id),
        Err(IncrementalPreparationError::UnexpectedUpdate { .. })
    ));

    let three_versions = flotsync_core::versions::VersionVectorGap {
        member_index: 0,
        start_version: 1,
        end_version: 3,
    };
    let mut progress = UpdateRangeProgress::new(candidate.group.group_id, three_versions);
    assert!(matches!(
        progress.accept(candidate.group.group_id, first.update_id),
        Ok(UpdateRangePosition::Expected)
    ));
    let version_three = UpdateId {
        node_index: 0,
        version: 3,
    };
    assert!(matches!(
        progress.accept(candidate.group.group_id, version_three),
        Ok(UpdateRangePosition::Gap)
    ));
    assert!(!progress.is_complete());

    let version_two = UpdateId {
        node_index: 0,
        version: 2,
    };
    let mut missing_first = UpdateRangeProgress::new(candidate.group.group_id, three_versions);
    assert!(matches!(
        missing_first.accept(candidate.group.group_id, version_two),
        Ok(UpdateRangePosition::Gap)
    ));
    let mut missing_last = UpdateRangeProgress::new(candidate.group.group_id, three_versions);
    missing_last
        .accept(candidate.group.group_id, first.update_id)
        .unwrap();
    missing_last
        .accept(candidate.group.group_id, version_two)
        .unwrap();
    assert!(!missing_last.is_complete());
}

#[test]
fn stored_history_rejects_unknown_datasets() {
    let candidate = incremental_candidate();
    let unknown_dataset =
        DatasetId::try_from_static("unknown").expect("unknown fixture dataset id should be valid");
    assert!(matches!(
        ensure_known_dataset(
            &candidate,
            &unknown_dataset,
            UpdateId {
                node_index: 0,
                version: 1,
            },
        ),
        Err(IncrementalPreparationError::UnknownDataset { .. })
    ));
}

#[test]
fn stored_history_page_reports_malformed_operation_with_update_context() {
    let candidate = incremental_candidate();
    let group_id = candidate.group.group_id;
    let member = MemberIdentity::from_array(["test", "member"]);
    let store = provisioned_sqlite_store(&member);
    let member_key = MemberKeyId {
        fingerprint: test_public_member_keys(&member).fingerprint(),
        member_id: member,
    };
    let group = ReplicationGroupRecord {
        group_id,
        member_keys: GroupMemberKeys::from_ordered_member_keys([member_key])
            .expect("one member key should form a group"),
        local_member_index: MemberIndex::new(0),
        group_schema: docs_group_schema(),
        version_vector: candidate.group.read_token.version().clone(),
        lifecycle: ReplicationGroupLifecycle::Open,
        security_material: current_slice_placeholder_group_security_material(group_id),
        ..Default::default()
    };
    let update = stored_update(
        &candidate,
        vec![DatasetUpdateRecord {
            dataset_id: docs_dataset_id(),
            operations: vec![SchemaOperation::default()],
        }],
    );
    let update_id = update.update_id;
    let mut write = wait_for_test_future(store.begin_transaction()).expect("write should open");
    wait_for_test_future(write.insert_replication_group(group)).expect("group should persist");
    wait_for_test_future(write.append_replication_update(update)).expect("update should persist");
    wait_for_test_future(write.commit()).expect("write should commit");

    let mut read = wait_for_test_future(store.begin_read_transaction()).expect("read should open");
    let result = wait_for_test_future(prepare_incremental_group(
        read.as_mut(),
        &candidate,
        NonZeroUsize::MIN,
    ));
    assert!(matches!(
        result,
        Err(IncrementalPreparationError::DecodeOperation {
            group,
            dataset,
            update,
            source: OperationCodecError::Codec { .. },
        }) if group == group_id && dataset == docs_dataset_id() && update == update_id
    ));
    wait_for_test_future(read.release()).expect("read should release");
    wait_for_test_future(store.close()).expect("store should close");
}

#[test]
fn current_row_slice_contract_violations_are_errors() {
    let candidate = incremental_candidate();
    let group_id = candidate.group.group_id;
    let dataset_id = crate::test_support::docs_dataset_id();
    let empty_rows = || ReplicationStateRowBatch::new(&Schema::empty());
    let wrong_group = crate::api::DatasetRowStateSlice {
        group_id: GroupId(Uuid::from_u128(12)),
        dataset_id: dataset_id.clone(),
        dataset_exists: true,
        state_rows: empty_rows(),
        missing_row_keys: HashSet::new(),
    };
    assert!(matches!(
        validate_current_row_slice(group_id, &dataset_id, 0, &wrong_group),
        Err(IncrementalPreparationError::UnexpectedRowSlice { .. })
    ));

    let missing_dataset = crate::api::DatasetRowStateSlice {
        group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists: false,
        state_rows: empty_rows(),
        missing_row_keys: HashSet::new(),
    };
    assert!(matches!(
        validate_current_row_slice(group_id, &dataset_id, 0, &missing_dataset),
        Err(IncrementalPreparationError::MissingCurrentDataset { .. })
    ));

    let missing_rows = crate::api::DatasetRowStateSlice {
        group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists: true,
        state_rows: empty_rows(),
        missing_row_keys: HashSet::from([RowKey(Uuid::from_u128(13))]),
    };
    assert!(matches!(
        validate_current_row_slice(group_id, &dataset_id, 1, &missing_rows),
        Err(IncrementalPreparationError::MissingCurrentRows { .. })
    ));

    let wrong_count = crate::api::DatasetRowStateSlice {
        group_id,
        dataset_id: dataset_id.clone(),
        dataset_exists: true,
        state_rows: empty_rows(),
        missing_row_keys: HashSet::new(),
    };
    assert!(matches!(
        validate_current_row_slice(group_id, &dataset_id, 1, &wrong_count),
        Err(IncrementalPreparationError::UnexpectedCurrentRowCount { .. })
    ));
}

//! Focused validation of incremental evidence and current-row contracts.

use super::{
    super::planning::{IncrementalCandidate, ReadableGroupSynchronisation},
    incremental::{
        AffectedRows,
        CreationAtPosition,
        IncrementalEvidence,
        IncrementalPreparationError,
        collect_affected_row_ids,
        creation_at_position,
        validate_current_row_slice,
        validate_retained_update_range,
    },
};
use crate::api::{DatasetId, GroupReadToken, ReplicationStateRowBatch, RowKey};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::schema::Schema;
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

/// Build one retained update for focused validation tests.
fn retained_update(
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
fn retained_history_distinguishes_missing_evidence_from_malformed_records() {
    let candidate = incremental_candidate();
    let range = flotsync_core::versions::VersionVectorGap {
        member_index: 0,
        start_version: 1,
        end_version: 1,
    };
    assert!(matches!(
        validate_retained_update_range(&candidate, range, Vec::new()),
        Ok(IncrementalEvidence::SnapshotRequired)
    ));

    let mut malformed = retained_update(&candidate, Vec::new());
    malformed.group_id = GroupId(Uuid::from_u128(11));
    assert!(matches!(
        validate_retained_update_range(&candidate, range, vec![malformed]),
        Err(IncrementalPreparationError::UnexpectedUpdate { .. })
    ));

    let mut self_dependent = retained_update(&candidate, Vec::new());
    self_dependent.read_versions.increment_at(0);
    assert!(matches!(
        validate_retained_update_range(&candidate, range, vec![self_dependent]),
        Err(IncrementalPreparationError::SelfDependentReadVersions {
            producer_read_version: 1,
            ..
        })
    ));

    let first = retained_update(&candidate, Vec::new());
    let mut extra = first.clone();
    extra.update_id.version = 2;
    assert!(matches!(
        validate_retained_update_range(&candidate, range, vec![first, extra]),
        Err(IncrementalPreparationError::UnexpectedUpdate { .. })
    ));
}

#[test]
fn affected_row_collection_rejects_unknown_datasets_and_malformed_operations() {
    let candidate = incremental_candidate();
    let unknown_dataset =
        DatasetId::try_from_static("unknown").expect("unknown fixture dataset id should be valid");
    let unknown_update = retained_update(
        &candidate,
        vec![crate::api::DatasetUpdateRecord {
            dataset_id: unknown_dataset,
            operations: Vec::new(),
        }],
    );
    let mut affected_rows = AffectedRows::new();
    assert!(matches!(
        collect_affected_row_ids(&candidate, &mut affected_rows, unknown_update),
        Err(IncrementalPreparationError::UnknownDataset { .. })
    ));

    let malformed_update = retained_update(
        &candidate,
        vec![crate::api::DatasetUpdateRecord {
            dataset_id: crate::test_support::docs_dataset_id(),
            operations: vec![flotsync_messages::datamodel::SchemaOperation::default()],
        }],
    );
    assert!(matches!(
        collect_affected_row_ids(&candidate, &mut affected_rows, malformed_update),
        Err(IncrementalPreparationError::DecodeOperation { .. })
    ));
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

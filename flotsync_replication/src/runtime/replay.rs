use super::{
    errors::{PublishChangesError, ReplayError, publish, replay},
    in_memory::{
        AppliedLocalOperation,
        LocalDataset,
        apply_local_delete,
        apply_local_upsert,
        apply_rebased_local_delete,
        apply_rebased_local_upsert,
    },
};
use crate::api::{
    DatasetId,
    DatasetRowStateSlice,
    ReplicationStoreTransaction,
    ReplicationUpdateFilter,
    ReplicationUpdateRecord,
    RowId,
    RowKey,
    RowValuesPatch,
    SchemaSource,
};
use flotsync_core::{
    GroupId,
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::codecs::datamodel::decode_schema_operation;
use flotsync_utils::option_when;
use snafu::prelude::*;
use std::{
    cmp,
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
};

/// Row-state materialisations needed to publish from a possibly stale read token.
pub(super) struct PublishDatasetState {
    /// Latest local dataset state used for direct writes and final persistence.
    latest_datasets: HashMap<DatasetId, LocalDataset>,
    /// Historical read-token bases for datasets whose latest rows changed after the token.
    replayed_read_base_datasets: HashMap<DatasetId, LocalDataset>,
}

impl PublishDatasetState {
    /// Apply one upsert, rebasing through a replayed read base only when needed.
    pub(super) fn apply_upsert(
        &mut self,
        row_id: &RowId,
        row: RowValuesPatch,
        update_id: UpdateId,
    ) -> Result<Option<AppliedLocalOperation>, PublishChangesError> {
        let latest_dataset = self
            .latest_datasets
            .get_mut(&row_id.dataset_id)
            .expect("publish row scope must preload every touched latest dataset");
        if let Some(read_base_dataset) =
            self.replayed_read_base_datasets.get_mut(&row_id.dataset_id)
        {
            apply_rebased_local_upsert(read_base_dataset, latest_dataset, row_id, row, update_id)
        } else {
            apply_local_upsert(latest_dataset, row_id, row, update_id)
        }
    }

    /// Apply one delete, rebasing through a replayed read base only when needed.
    pub(super) fn apply_delete(
        &mut self,
        row_id: &RowId,
        update_id: UpdateId,
    ) -> Result<AppliedLocalOperation, PublishChangesError> {
        let latest_dataset = self
            .latest_datasets
            .get_mut(&row_id.dataset_id)
            .expect("publish row scope must preload every touched latest dataset");
        if let Some(read_base_dataset) =
            self.replayed_read_base_datasets.get_mut(&row_id.dataset_id)
        {
            apply_rebased_local_delete(read_base_dataset, latest_dataset, row_id, update_id)
        } else {
            apply_local_delete(latest_dataset, row_id, update_id)
        }
    }
}

/// Load current row slices for the rows touched by one publish operation.
pub(super) async fn load_touched_dataset_slices(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_id: GroupId,
    dataset_rows: &HashMap<DatasetId, HashSet<RowKey>>,
) -> Result<HashMap<DatasetId, DatasetRowStateSlice>, crate::api::StoreError> {
    let mut slices = HashMap::with_capacity(dataset_rows.len());
    for (dataset_id, row_keys) in dataset_rows {
        let mut row_keys = row_keys.iter();
        let row_slice = transaction
            .load_dataset_rows(&group_id, dataset_id, &mut row_keys)
            .await?;
        slices.insert(dataset_id.clone(), row_slice);
    }
    Ok(slices)
}

/// Materialise loaded row slices into in-memory datasets for CRDT application.
pub(super) fn materialise_dataset_slices(
    schemas: &HashMap<DatasetId, SchemaSource>,
    slices: HashMap<DatasetId, DatasetRowStateSlice>,
) -> HashMap<DatasetId, LocalDataset> {
    let mut datasets = HashMap::with_capacity(slices.len());
    for (dataset_id, row_slice) in slices {
        let schema = schemas
            .get(&dataset_id)
            .expect("touched dataset schemas must be pre-loaded");
        datasets.insert(
            dataset_id,
            LocalDataset::from_row_slice(schema.clone(), row_slice),
        );
    }
    datasets
}

/// Load both latest and read-token row states needed to publish local changes.
///
/// Rows that have not changed since `read_versions` reuse the latest store
/// slice as their read base. Datasets containing newer rows are reconstructed
/// by replaying applied updates up to `read_versions` for only the rows touched
/// by this publish request.
pub(super) async fn load_publish_dataset_state(
    transaction: &mut dyn ReplicationStoreTransaction,
    schemas: &HashMap<DatasetId, SchemaSource>,
    group_id: GroupId,
    member_count: NonZeroUsize,
    dataset_rows: HashMap<DatasetId, HashSet<RowKey>>,
    read_versions: &VersionVector,
) -> Result<PublishDatasetState, PublishChangesError> {
    let latest_slices = load_touched_dataset_slices(transaction, group_id, &dataset_rows)
        .await
        .context(publish::StoreAccessSnafu)?;
    let dataset_ids_that_require_replay = latest_slices
        .iter()
        .filter_map(|(dataset_id, slice)| {
            option_when!(
                row_slice_needs_replay(slice, read_versions),
                dataset_id.clone()
            )
        })
        .collect::<HashSet<_>>();

    let latest_datasets = materialise_dataset_slices(schemas, latest_slices);
    let mut replayed_read_base_datasets = HashMap::new();
    if !dataset_ids_that_require_replay.is_empty() {
        let applied_updates = transaction
            .load_replication_updates(&group_id, ReplicationUpdateFilter::Applied, None)
            .await
            .context(publish::StoreAccessSnafu)?;
        let replayed_datasets = replay_datasets_at_versions(
            group_id,
            member_count,
            schemas,
            applied_updates,
            read_versions,
            &dataset_rows,
        )
        .context(publish::ReplaySnafu)?;

        for dataset_id in dataset_ids_that_require_replay {
            let schema = schemas
                .get(&dataset_id)
                .expect("replayed dataset schema must be pre-loaded");
            let replayed_dataset = replayed_datasets
                .get(&dataset_id)
                .cloned()
                .unwrap_or_else(|| LocalDataset::new(schema.clone()));
            replayed_read_base_datasets.insert(dataset_id, replayed_dataset);
        }
    }
    Ok(PublishDatasetState {
        latest_datasets,
        replayed_read_base_datasets,
    })
}

/// Reconstruct selected dataset rows at one historical read token.
///
/// The first implementation replays applied updates from the zero vector up to
/// `target_versions`. It is intentionally simple; future slices can add durable
/// checkpoints without changing the publish path.
pub(super) fn replay_datasets_at_versions(
    group_id: GroupId,
    member_count: NonZeroUsize,
    schemas: &HashMap<DatasetId, SchemaSource>,
    updates: Vec<ReplicationUpdateRecord>,
    target_versions: &VersionVector,
    row_scope: &HashMap<DatasetId, HashSet<RowKey>>,
) -> Result<HashMap<DatasetId, LocalDataset>, ReplayError> {
    let mut datasets = HashMap::new();
    let mut simulated_versions = VersionVector::initial(member_count);
    let mut pending_updates = included_updates(updates, target_versions);

    // Repeatedly select the next causally ready update because the store does
    // not guarantee topological ordering for applied-update replay.
    while let Some(update_index) = find_ready_update_index(&pending_updates, &simulated_versions) {
        let update = pending_updates.remove(update_index);
        replay_one_update(group_id, schemas, row_scope, &mut datasets, &update)?;
        simulated_versions.increment_at(update.update_id.node_index as usize);
    }

    ensure!(
        pending_updates.is_empty(),
        replay::IncompleteSnafu { group_id }
    );
    Ok(datasets)
}

fn row_slice_needs_replay(slice: &DatasetRowStateSlice, read_versions: &VersionVector) -> bool {
    slice.rows.values().any(|row| match row {
        Some(row) if row.last_changed_versions <= *read_versions => false,
        Some(_) => true,
        None => false,
    })
}

fn included_updates(
    updates: Vec<ReplicationUpdateRecord>,
    target_versions: &VersionVector,
) -> Vec<ReplicationUpdateRecord> {
    let mut updates = updates
        .into_iter()
        .filter(|update| {
            let producer_index = update.update_id.node_index as usize;
            target_versions.version_at(producer_index) >= update.update_id.version
        })
        .collect::<Vec<_>>();
    updates.sort_by(compare_replay_candidates);
    updates
}

fn compare_replay_candidates(
    left: &ReplicationUpdateRecord,
    right: &ReplicationUpdateRecord,
) -> cmp::Ordering {
    left.read_versions
        .partial_cmp(&right.read_versions)
        .unwrap_or(cmp::Ordering::Equal)
        .then_with(|| left.update_id.cmp(&right.update_id))
}

fn find_ready_update_index(
    pending_updates: &[ReplicationUpdateRecord],
    simulated_versions: &VersionVector,
) -> Option<usize> {
    pending_updates
        .iter()
        .position(|update| replay_ready(simulated_versions, update))
}

fn replay_ready(simulated_versions: &VersionVector, update: &ReplicationUpdateRecord) -> bool {
    if update.read_versions <= *simulated_versions {
        let producer_position = update.update_id.node_index as usize;
        let expected_next_version = simulated_versions
            .version_at(producer_position)
            .checked_add(1)
            .expect("member version counter must not overflow");
        expected_next_version == update.update_id.version
    } else {
        false
    }
}

fn replay_one_update(
    group_id: GroupId,
    schemas: &HashMap<DatasetId, SchemaSource>,
    row_scope: &HashMap<DatasetId, HashSet<RowKey>>,
    datasets: &mut HashMap<DatasetId, LocalDataset>,
    update: &ReplicationUpdateRecord,
) -> Result<(), ReplayError> {
    'dataset_updates: for dataset_update in &update.dataset_updates {
        let Some(schema) = schemas.get(&dataset_update.dataset_id) else {
            continue 'dataset_updates;
        };
        let Some(scoped_rows) = row_scope.get(&dataset_update.dataset_id) else {
            continue 'dataset_updates;
        };

        'operations: for operation in &dataset_update.operations {
            let operation = decode_schema_operation(operation.clone(), schema.as_schema())
                .context(replay::DecodeOperationSnafu {
                    dataset_id: dataset_update.dataset_id.clone(),
                })?;
            let row_key = RowKey(*operation.operation.row_id());
            if !scoped_rows.contains(&row_key) {
                continue 'operations;
            }

            let dataset = datasets
                .entry(dataset_update.dataset_id.clone())
                .or_insert_with(|| LocalDataset::new(schema.clone()));
            let row_id = RowId {
                group_id,
                dataset_id: dataset_update.dataset_id.clone(),
                row_key,
            };
            dataset.data = dataset
                .data
                .clone()
                .apply_schema_operation(operation)
                .context(replay::ApplyOperationSnafu { row_id })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::{DatasetRowStateWrite, DatasetUpdateRecord, ReplicationRowStateRecord},
        row_values,
    };
    use flotsync_core::{MemberIdentity, member::Identifier, versions::PureVersionVector};
    use flotsync_data_types::{Field, RowOperations, Schema, TableOperations};
    use std::sync::Arc;
    use uuid::Uuid;

    fn docs_dataset_id() -> DatasetId {
        DatasetId::try_new("docs").expect("dataset id should build")
    }

    fn title_schema() -> SchemaSource {
        SchemaSource::Shared(Arc::new(Schema::from_fields([Field::linear_string(
            "title",
        )])))
    }

    fn local_member() -> MemberIdentity {
        Identifier::from_array(["app", "alice"])
    }

    fn update_id(version: u64) -> UpdateId {
        UpdateId {
            version,
            node_index: 0,
        }
    }

    fn row_key(id: u128) -> RowKey {
        RowKey(Uuid::from_u128(id))
    }

    fn row_id(group_id: GroupId, dataset_id: &DatasetId, row_key: RowKey) -> RowId {
        RowId {
            group_id,
            dataset_id: dataset_id.clone(),
            row_key,
        }
    }

    fn replay_update(
        group_id: GroupId,
        dataset_id: &DatasetId,
        update_id: UpdateId,
        read_versions: VersionVector,
        operations: Vec<flotsync_messages::datamodel::SchemaOperation>,
    ) -> ReplicationUpdateRecord {
        ReplicationUpdateRecord {
            group_id,
            update_id,
            sender: local_member(),
            read_versions,
            dataset_updates: vec![DatasetUpdateRecord {
                dataset_id: dataset_id.clone(),
                operations,
            }],
            applied_locally: true,
        }
    }

    fn upsert_operation(
        dataset: &mut LocalDataset,
        row_id: &RowId,
        title: &str,
        update_id: UpdateId,
    ) -> flotsync_messages::datamodel::SchemaOperation {
        apply_local_upsert(dataset, row_id, row_values! { "title" => title }, update_id)
            .expect("local upsert should apply")
            .expect("local upsert should produce an operation")
            .encoded_operation
    }

    fn row_title(dataset: &LocalDataset, row_key: RowKey) -> String {
        let row = dataset.data.get_row(&row_key.0).expect("row should exist");
        row.get_field_value::<str>("title")
            .expect("title should decode")
            .to_string()
    }

    fn row_record_with_last_changed(
        group_id: GroupId,
        dataset_id: &DatasetId,
        row_key: RowKey,
        last_changed_versions: VersionVector,
    ) -> ReplicationRowStateRecord {
        let mut dataset = LocalDataset::new(title_schema());
        let applied = apply_local_upsert(
            &mut dataset,
            &row_id(group_id, dataset_id, row_key),
            row_values! { "title" => "stored" },
            update_id(1),
        )
        .expect("local upsert should apply")
        .expect("local upsert should produce an operation");
        let DatasetRowStateWrite::UpsertActive { snapshot, .. } = applied.row_write else {
            panic!("test upsert should produce an active row write");
        };
        ReplicationRowStateRecord {
            row_id: row_key,
            snapshot,
            tombstoned: false,
            last_changed_versions,
        }
    }

    #[test]
    fn replay_ignores_updates_for_rows_outside_scope() {
        let group_id = GroupId(Uuid::from_u128(40_001));
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let schemas = HashMap::from([(dataset_id.clone(), schema.clone())]);
        let member_count = NonZeroUsize::new(1).expect("member count should be non-zero");
        let read_versions = VersionVector::initial(member_count);
        let update_id = update_id(1);
        let scoped_row_key = row_key(50_001);
        let ignored_row_key = row_key(50_002);
        let mut source_dataset = LocalDataset::new(schema);
        let scoped_operation = upsert_operation(
            &mut source_dataset,
            &row_id(group_id, &dataset_id, scoped_row_key),
            "scoped",
            update_id,
        );
        let ignored_operation = upsert_operation(
            &mut source_dataset,
            &row_id(group_id, &dataset_id, ignored_row_key),
            "ignored",
            update_id,
        );
        let update = replay_update(
            group_id,
            &dataset_id,
            update_id,
            read_versions.clone(),
            vec![scoped_operation, ignored_operation],
        );
        let row_scope = HashMap::from([(dataset_id.clone(), HashSet::from([scoped_row_key]))]);

        let replayed = replay_datasets_at_versions(
            group_id,
            member_count,
            &schemas,
            vec![update],
            &read_versions.with_update_applied(update_id),
            &row_scope,
        )
        .expect("scoped replay should succeed");

        let dataset = replayed
            .get(&dataset_id)
            .expect("scoped dataset should be materialised");
        assert_eq!(row_title(dataset, scoped_row_key), "scoped");
        assert!(dataset.data.get_row(&ignored_row_key.0).is_none());
    }

    #[test]
    fn replay_selects_causally_ready_updates_from_unsorted_input() {
        let group_id = GroupId(Uuid::from_u128(40_002));
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let schemas = HashMap::from([(dataset_id.clone(), schema.clone())]);
        let member_count = NonZeroUsize::new(1).expect("member count should be non-zero");
        let initial_versions = VersionVector::initial(member_count);
        let first_update_id = update_id(1);
        let second_update_id = update_id(2);
        let replayed_row_key = row_key(50_003);
        let replayed_row_id = row_id(group_id, &dataset_id, replayed_row_key);
        let mut source_dataset = LocalDataset::new(schema);
        let first_operation = upsert_operation(
            &mut source_dataset,
            &replayed_row_id,
            "first",
            first_update_id,
        );
        let after_first = initial_versions.with_update_applied(first_update_id);
        let second_operation = upsert_operation(
            &mut source_dataset,
            &replayed_row_id,
            "second",
            second_update_id,
        );
        let first_update = replay_update(
            group_id,
            &dataset_id,
            first_update_id,
            initial_versions,
            vec![first_operation],
        );
        let second_update = replay_update(
            group_id,
            &dataset_id,
            second_update_id,
            after_first.clone(),
            vec![second_operation],
        );
        let row_scope = HashMap::from([(dataset_id.clone(), HashSet::from([replayed_row_key]))]);

        let replayed = replay_datasets_at_versions(
            group_id,
            member_count,
            &schemas,
            vec![second_update, first_update],
            &after_first.with_update_applied(second_update_id),
            &row_scope,
        )
        .expect("out-of-order replay should succeed");

        let dataset = replayed
            .get(&dataset_id)
            .expect("replayed dataset should be materialised");
        assert_eq!(row_title(dataset, replayed_row_key), "second");
    }

    #[test]
    fn row_slice_replay_detection_checks_last_changed_causality() {
        let group_id = GroupId(Uuid::from_u128(40_003));
        let dataset_id = docs_dataset_id();
        let member_count = NonZeroUsize::new(2).expect("member count should be non-zero");
        let read_versions = VersionVector::Synced {
            num_members: member_count,
            version: 2,
        };

        for (last_changed_versions, expected_needs_replay) in [
            (VersionVector::Full(PureVersionVector::from([1, 2])), false),
            (
                VersionVector::Synced {
                    num_members: member_count,
                    version: 2,
                },
                false,
            ),
            (VersionVector::Full(PureVersionVector::from([3, 2])), true),
            (VersionVector::Full(PureVersionVector::from([1, 3])), true),
        ] {
            let row_key = row_key(if expected_needs_replay {
                60_001
            } else {
                60_000
            });
            let slice = DatasetRowStateSlice {
                group_id,
                dataset_id: dataset_id.clone(),
                dataset_exists: true,
                rows: HashMap::from([(
                    row_key,
                    Some(row_record_with_last_changed(
                        group_id,
                        &dataset_id,
                        row_key,
                        last_changed_versions,
                    )),
                )]),
            };

            assert_eq!(
                row_slice_needs_replay(&slice, &read_versions),
                expected_needs_replay
            );
        }
    }
}

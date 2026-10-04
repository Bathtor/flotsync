use super::{
    errors::{ExactUpdateMismatch, PublishChangesError, ReplayError, publish, replay},
    in_memory::{
        AppliedLocalOperation,
        LocalDataset,
        UpdateDependency,
        apply_local_delete,
        apply_local_upsert,
        apply_rebased_local_delete,
        apply_rebased_local_upsert,
        load_update_dependencies,
    },
};
use crate::api::{
    DatasetId,
    DatasetRowStateSlice,
    GroupDatasetSchemaRef,
    GroupSchema,
    ReplicationStoreTransaction,
    ReplicationUpdateFilter,
    ReplicationUpdateRecord,
    RowId,
    RowKey,
    RowValuesPatch,
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
    group_schema: &GroupSchema,
    group_id: GroupId,
    dataset_rows: &HashMap<DatasetId, HashSet<RowKey>>,
) -> Result<HashMap<DatasetId, DatasetRowStateSlice>, crate::api::StoreError> {
    let mut slices = HashMap::with_capacity(dataset_rows.len());
    for (dataset_id, row_keys) in dataset_rows {
        let schema = group_schema
            .schema(dataset_id)
            .expect("touched dataset schemas must be present in the hosted group");
        let dataset = GroupDatasetSchemaRef {
            group_id: &group_id,
            dataset_id,
            schema: schema.as_schema(),
        };
        let mut row_keys = row_keys.iter();
        let row_slice = transaction
            .load_dataset_rows(dataset, &mut row_keys)
            .await?;
        slices.insert(dataset_id.clone(), row_slice);
    }
    Ok(slices)
}

/// Materialise loaded row slices into in-memory datasets for CRDT application.
pub(super) fn materialise_dataset_slices(
    group_schema: &GroupSchema,
    slices: HashMap<DatasetId, DatasetRowStateSlice>,
) -> HashMap<DatasetId, LocalDataset> {
    let mut datasets = HashMap::with_capacity(slices.len());
    for (dataset_id, row_slice) in slices {
        let schema = group_schema
            .schema(&dataset_id)
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
    group_schema: &GroupSchema,
    group_id: GroupId,
    member_count: NonZeroUsize,
    dataset_rows: HashMap<DatasetId, HashSet<RowKey>>,
    read_versions: &VersionVector,
) -> Result<PublishDatasetState, PublishChangesError> {
    let latest_slices =
        load_touched_dataset_slices(transaction, group_schema, group_id, &dataset_rows)
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

    let latest_datasets = materialise_dataset_slices(group_schema, latest_slices);
    let mut replayed_read_base_datasets = HashMap::new();
    if !dataset_ids_that_require_replay.is_empty() {
        let replayed_datasets = replay_datasets_at_versions(
            transaction,
            group_id,
            member_count,
            group_schema,
            read_versions,
            &dataset_rows,
        )
        .await?;

        for dataset_id in dataset_ids_that_require_replay {
            let schema = group_schema
                .schema(&dataset_id)
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
/// Read compact causal positions, then load and apply one selected payload at a time.
async fn replay_datasets_at_versions(
    transaction: &mut dyn ReplicationStoreTransaction,
    group_id: GroupId,
    member_count: NonZeroUsize,
    group_schema: &GroupSchema,
    target_versions: &VersionVector,
    row_scope: &HashMap<DatasetId, HashSet<RowKey>>,
) -> Result<HashMap<DatasetId, LocalDataset>, PublishChangesError> {
    let dependencies =
        load_update_dependencies(transaction, group_id, ReplicationUpdateFilter::Applied)
            .await
            .context(publish::StoreAccessSnafu)?;
    let ordered_updates = plan_replay_order(group_id, member_count, dependencies, target_versions)
        .context(publish::ReplaySnafu)?;
    let mut datasets = HashMap::new();
    for scheduled in ordered_updates {
        let update_id = scheduled.update_id;
        let update = transaction
            .load_replication_update(&group_id, update_id)
            .await
            .context(publish::StoreAccessSnafu)?;
        let update = update
            .context(replay::MissingUpdateSnafu {
                group_id,
                update_id,
            })
            .context(publish::ReplaySnafu)?;
        if update.group_id != group_id
            || update.update_id != update_id
            || update.read_versions != scheduled.read_versions
            || !update.applied_locally
        {
            replay::MismatchedUpdateSnafu {
                mismatch: Box::new(ExactUpdateMismatch::new(
                    group_id,
                    update_id,
                    &scheduled.read_versions,
                    true,
                    &update,
                )),
            }
            .fail::<()>()
            .context(publish::ReplaySnafu)?;
        }
        replay_one_update(group_id, group_schema, row_scope, &mut datasets, &update)
            .context(publish::ReplaySnafu)?;
    }
    Ok(datasets)
}

fn row_slice_needs_replay(slice: &DatasetRowStateSlice, read_versions: &VersionVector) -> bool {
    slice.state_rows.rows().any(|row| {
        matches!(
            row.metadata()
                .last_changed_versions
                .partial_cmp(read_versions),
            Some(cmp::Ordering::Greater) | None
        )
    })
}

/// Select a deterministic causal order using only identities and read frontiers.
fn plan_replay_order(
    group_id: GroupId,
    member_count: NonZeroUsize,
    updates: Vec<UpdateDependency>,
    target_versions: &VersionVector,
) -> Result<Vec<UpdateDependency>, ReplayError> {
    ensure!(
        target_versions.num_members() == member_count,
        replay::InvalidTargetVersionsSnafu {
            group_id,
            expected: member_count.get(),
            actual: target_versions.num_members().get(),
        }
    );
    for update in &updates {
        ensure!(
            (update.update_id.node_index as usize) < member_count.get()
                && update.read_versions.num_members() == member_count,
            replay::InvalidUpdateDependenciesSnafu {
                group_id,
                update_id: update.update_id,
            }
        );
    }
    let mut updates = updates
        .into_iter()
        .filter(|update| {
            let producer_index = update.update_id.node_index as usize;
            target_versions.version_at(producer_index) >= update.update_id.version
        })
        .collect::<Vec<_>>();
    updates.sort_by(UpdateDependency::compare_for_replay);
    let mut simulated_versions = VersionVector::initial(member_count);
    let mut ordered_updates = Vec::with_capacity(updates.len());
    while let Some(update_index) = find_ready_update_index(&updates, &simulated_versions) {
        let update = updates.remove(update_index);
        simulated_versions.increment_at(update.update_id.node_index as usize);
        ordered_updates.push(update);
    }
    ensure!(updates.is_empty(), replay::IncompleteSnafu { group_id });
    Ok(ordered_updates)
}

fn find_ready_update_index(
    pending_updates: &[UpdateDependency],
    simulated_versions: &VersionVector,
) -> Option<usize> {
    pending_updates
        .iter()
        .position(|update| update.is_ready_at(simulated_versions))
}

fn replay_one_update(
    group_id: GroupId,
    group_schema: &GroupSchema,
    row_scope: &HashMap<DatasetId, HashSet<RowKey>>,
    datasets: &mut HashMap<DatasetId, LocalDataset>,
    update: &ReplicationUpdateRecord,
) -> Result<(), ReplayError> {
    'dataset_updates: for dataset_update in &update.dataset_updates {
        let Some(schema) = group_schema.schema(&dataset_update.dataset_id) else {
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
        api::{
            DatasetRowStateWrite,
            DatasetUpdateRecord,
            ReplicationRowMetadata,
            ReplicationStateRowBatch,
            SchemaSource,
        },
        row_values,
    };
    use flotsync_core::{MemberIdentity, versions::PureVersionVector};
    use flotsync_data_types::{Field, RowOperations, Schema, TableOperations};
    use std::sync::Arc;
    use uuid::Uuid;

    fn docs_dataset_id() -> DatasetId {
        DatasetId::try_from_static("docs").expect("dataset id should build")
    }

    fn title_schema() -> SchemaSource {
        SchemaSource::Shared(Arc::new(Schema::from_fields([Field::linear_string(
            "title",
        )])))
    }

    fn local_member() -> MemberIdentity {
        MemberIdentity::from_array(["app", "alice"])
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

    fn row_slice_with_last_changed(
        group_id: GroupId,
        dataset_id: &DatasetId,
        row_key: RowKey,
        last_changed_versions: VersionVector,
    ) -> DatasetRowStateSlice {
        let schema = title_schema();
        let mut dataset = LocalDataset::new(schema.clone());
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
        let encoded = flotsync_messages::codecs::datamodel::encode_row_snapshot(
            &snapshot,
            schema.as_schema(),
        )
        .expect("test row snapshot should encode");
        let mut decoder =
            flotsync_messages::snapshots::datamodel::ProtoSchemaSnapshotDecoder::new(encoded)
                .expect("test row snapshot should create a decoder");
        let mut state_rows = ReplicationStateRowBatch::new(schema.as_schema());
        state_rows
            .push_decoded_row(
                ReplicationRowMetadata {
                    row_key,
                    tombstoned: false,
                    created_by: Some(update_id(1)),
                    last_changed_versions,
                },
                &mut decoder,
            )
            .expect("test row snapshot should decode into the batch");
        DatasetRowStateSlice {
            group_id,
            dataset_id: dataset_id.clone(),
            dataset_exists: true,
            state_rows,
            missing_row_keys: HashSet::new(),
        }
    }

    #[test]
    fn replay_ignores_updates_for_rows_outside_scope() {
        let group_id = GroupId(Uuid::from_u128(40_001));
        let dataset_id = docs_dataset_id();
        let schema = title_schema();
        let schemas = GroupSchema::new(HashMap::from([(dataset_id.clone(), schema.clone())]));
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

        let mut replayed = HashMap::new();
        replay_one_update(group_id, &schemas, &row_scope, &mut replayed, &update)
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
        let schemas = GroupSchema::new(HashMap::from([(dataset_id.clone(), schema.clone())]));
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

        let target = after_first.with_update_applied(second_update_id);
        let ordered_updates = plan_replay_order(
            group_id,
            member_count,
            vec![
                UpdateDependency {
                    update_id: second_update.update_id,
                    read_versions: second_update.read_versions.clone(),
                },
                UpdateDependency {
                    update_id: first_update.update_id,
                    read_versions: first_update.read_versions.clone(),
                },
            ],
            &target,
        )
        .expect("out-of-order updates should have a causal order");
        let ready_ids = ordered_updates
            .into_iter()
            .map(|update| update.update_id)
            .collect::<Vec<_>>();
        assert_eq!(ready_ids, [first_update_id, second_update_id]);
        let historical_ids = plan_replay_order(
            group_id,
            member_count,
            vec![
                UpdateDependency {
                    update_id: second_update.update_id,
                    read_versions: second_update.read_versions.clone(),
                },
                UpdateDependency {
                    update_id: first_update.update_id,
                    read_versions: first_update.read_versions.clone(),
                },
            ],
            &after_first,
        )
        .expect("historical target should select only its included update");
        assert_eq!(historical_ids.len(), 1);
        assert_eq!(historical_ids[0].update_id, first_update_id);
        let mut replayed = HashMap::new();
        for update_id in ready_ids {
            let update = if update_id == first_update_id {
                &first_update
            } else {
                &second_update
            };
            replay_one_update(group_id, &schemas, &row_scope, &mut replayed, update)
                .expect("ordered replay should succeed");
        }

        let dataset = replayed
            .get(&dataset_id)
            .expect("replayed dataset should be materialised");
        assert_eq!(row_title(dataset, replayed_row_key), "second");
    }

    #[test]
    fn replay_reports_missing_dependencies_and_malformed_frontiers() {
        let group_id = GroupId(Uuid::from_u128(40_004));
        let one_member = NonZeroUsize::MIN;
        let two_members = NonZeroUsize::new(2).expect("two members are non-zero");
        let second_update = UpdateDependency {
            update_id: update_id(2),
            read_versions: VersionVector::initial(one_member).with_version_at(0, 1),
        };
        let target = VersionVector::initial(one_member).with_version_at(0, 2);
        assert!(matches!(
            plan_replay_order(group_id, one_member, vec![second_update.clone()], &target),
            Err(ReplayError::Incomplete { .. })
        ));
        assert!(matches!(
            plan_replay_order(
                group_id,
                one_member,
                vec![second_update.clone()],
                &VersionVector::initial(two_members),
            ),
            Err(ReplayError::InvalidTargetVersions { .. })
        ));
        let malformed_update = UpdateDependency {
            read_versions: VersionVector::initial(two_members),
            ..second_update
        };
        assert!(matches!(
            plan_replay_order(group_id, one_member, vec![malformed_update], &target),
            Err(ReplayError::InvalidUpdateDependencies { .. })
        ));
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
            let slice =
                row_slice_with_last_changed(group_id, &dataset_id, row_key, last_changed_versions);

            assert_eq!(
                row_slice_needs_replay(&slice, &read_versions),
                expected_needs_replay
            );
        }
    }
}

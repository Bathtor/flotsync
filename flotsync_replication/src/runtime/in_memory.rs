use super::errors::{
    GroupInstallError,
    InboundDeliveryError,
    InstallMissingLocalMemberSnafu,
    InvalidPersistedMembersSnafu,
    PersistedLocalMemberIndexMismatchSnafu,
    PersistedVersionVectorMemberCountMismatchSnafu,
    PublishChangesError,
    inbound,
    publish,
};
use crate::api::{
    DatasetId,
    DatasetRowStatePatch,
    DatasetRowStateSlice,
    DatasetRowStateWrite,
    DatasetUpdateRecord,
    ReplicationGroupRecord,
    ReplicationRowStateSnapshot,
    ReplicationUpdateRecord,
    RowChange,
    RowId,
    RowKey,
    RowMutation,
    RowValuesPatch,
    SchemaSource,
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::GroupMembers,
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::{
    InitialFieldValue,
    OperationOutcome,
    PendingFieldUpdate,
    RowStateRead,
    Schema,
    TableOperations,
    schema::datamodel::{RowOperation, RowRecord},
};
use flotsync_messages::codecs::datamodel::{decode_schema_operation, encode_schema_operation};
use flotsync_utils::option_when;
use snafu::prelude::*;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};

struct LocalStoredStateRow {
    snapshot: ReplicationRowStateSnapshot,
    tombstoned: bool,
}

/// One fixed-membership replication group loaded for one store transaction.
///
/// The runtime no longer keeps this state resident between messages. Instead,
/// each publish or inbound-apply flow loads the persisted group metadata, uses
/// it to drive one isolated transaction-scoped working set, and then writes the
/// updated durable state back through `ReplicationStore`.
#[derive(Clone)]
pub(super) struct LoadedGroupMeta {
    pub(super) members: GroupMembers,
    pub(super) local_member_index: MemberIndex,
    pub(super) version_vector: VersionVector,
}

impl LoadedGroupMeta {
    /// Rebuild one transaction-scoped group view from a persisted group record.
    pub(super) fn from_replication_group_record(
        local_member: &MemberIdentity,
        group: ReplicationGroupRecord,
    ) -> Result<Self, GroupInstallError> {
        let group_id = group.group_id;
        let members = group
            .member_keys
            .to_group_members()
            .context(InvalidPersistedMembersSnafu { group_id })?;
        let local_member_index =
            members
                .member_index(local_member)
                .context(InstallMissingLocalMemberSnafu {
                    local_member: local_member.clone(),
                })?;
        ensure!(
            local_member_index == group.local_member_index,
            PersistedLocalMemberIndexMismatchSnafu {
                group_id,
                local_member: local_member.clone(),
                persisted_local_member_index: group.local_member_index,
                actual_local_member_index: local_member_index,
            }
        );

        let member_count =
            NonZeroUsize::new(members.len()).expect("persisted group members must not be empty");
        ensure!(
            group.version_vector.num_members() == member_count,
            PersistedVersionVectorMemberCountMismatchSnafu {
                group_id,
                persisted_member_count: group.version_vector.num_members().get(),
                actual_member_count: member_count.get(),
            }
        );

        Ok(Self {
            members,
            local_member_index,
            version_vector: group.version_vector,
        })
    }

    /// Return the fixed member count for this transaction-scoped group view.
    pub(super) fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.members.len()).expect("loaded group must be non-empty")
    }

    /// Return the durably applied version for the given member index.
    pub(super) fn applied_version(&self, member_index: MemberIndex) -> u64 {
        self.version_vector
            .version_at(member_index.as_u32() as usize)
    }

    /// Return the next producer version expected from the given member.
    pub(super) fn expected_next_version(&self, member_index: MemberIndex) -> u64 {
        self.applied_version(member_index)
            .checked_add(1)
            .expect("member version counter must not overflow")
    }

    /// Return `true` when `update_id` is already reflected in the durable VV.
    pub(super) fn has_applied(&self, update_id: UpdateId) -> bool {
        self.applied_version(MemberIndex::new(update_id.node_index)) >= update_id.version
    }

    /// Return `true` when `update` is causally ready and is the next version
    /// expected from its producer.
    pub(super) fn can_apply(&self, update: &ReplicationUpdateRecord) -> bool {
        let producer_index = MemberIndex::new(update.update_id.node_index);
        (self.version_vector >= update.read_versions)
            && self.expected_next_version(producer_index) == update.update_id.version
    }

    /// Advance the durable VV to reflect one update that has now applied.
    pub(super) fn mark_applied(&mut self, update_id: UpdateId) {
        self.version_vector
            .increment_at(update_id.node_index as usize);
    }
}

pub(super) fn validate_inbound_update_read_versions(
    update: &ReplicationUpdateRecord,
) -> Result<(), InboundDeliveryError> {
    let producer_read_version = update
        .read_versions
        .version_at(update.update_id.node_index as usize);
    ensure!(
        producer_read_version < update.update_id.version,
        inbound::SelfDependentReadVersionsSnafu {
            group_id: update.group_id,
            update_id: update.update_id,
            producer_read_version,
        }
    );
    Ok(())
}

/// Persisted-but-not-yet-applied updates loaded for one transactional
/// causality check.
pub(super) struct PendingUpdateSet {
    updates: BTreeMap<UpdateId, ReplicationUpdateRecord>,
}

impl PendingUpdateSet {
    /// Materialise one deterministic pending-update index from store records.
    pub(super) fn from_updates(updates: Vec<ReplicationUpdateRecord>) -> Self {
        let updates = updates
            .into_iter()
            .map(|update| (update.update_id, update))
            .collect();
        Self { updates }
    }

    /// Determine which pending updates are already reflected in durable state
    /// and which additional updates can now apply in causal order.
    pub(super) fn plan_apply_chain(&mut self, group: &LoadedGroupMeta) -> PendingApplyPlan {
        let mut already_applied = Vec::new();
        let mut ready_chain = Vec::new();
        let mut simulated_group = group.clone();

        'drain: loop {
            let stale_ids: Vec<_> = self
                .updates
                .keys()
                .copied()
                .filter(|update_id| simulated_group.has_applied(*update_id))
                .collect();
            for update_id in stale_ids {
                self.updates.remove(&update_id);
                already_applied.push(update_id);
            }

            let ready_update_id = self.updates.iter().find_map(|(update_id, update)| {
                option_when!(simulated_group.can_apply(update), *update_id)
            });
            let Some(ready_update_id) = ready_update_id else {
                break 'drain;
            };
            let ready_update = self
                .updates
                .remove(&ready_update_id)
                .expect("pending update must still exist while draining");
            simulated_group.mark_applied(ready_update_id);
            ready_chain.push(ready_update);
        }

        PendingApplyPlan {
            already_applied,
            ready_chain,
            blocked_updates: std::mem::take(&mut self.updates).into_values().collect(),
        }
    }
}

/// One transaction-local decision about pending persisted updates.
pub(super) struct PendingApplyPlan {
    pub(super) already_applied: Vec<UpdateId>,
    pub(super) ready_chain: Vec<ReplicationUpdateRecord>,
    pub(super) blocked_updates: Vec<ReplicationUpdateRecord>,
}

/// One local dataset together with its current replicated in-memory contents.
#[derive(Clone)]
pub(super) struct LocalDataset {
    pub(super) data: flotsync_messages::InMemoryStateData,
}

impl LocalDataset {
    pub(super) fn new(schema: impl Into<SchemaSource>) -> Self {
        Self {
            data: flotsync_messages::InMemoryStateData::new(schema),
        }
    }

    /// Rebuild one ephemeral in-memory dataset slice from store-loaded rows.
    pub(super) fn from_row_slice(schema: SchemaSource, slice: DatasetRowStateSlice) -> Self {
        let data = flotsync_messages::InMemoryStateData::from_row_snapshots_with_tombstones(
            schema,
            slice.rows.into_values().filter_map(|row| {
                row.map(|row| RowRecord {
                    row_id: row.row_id.0,
                    snapshot: row.snapshot,
                    tombstoned: row.tombstoned,
                })
            }),
        )
        .expect("store-loaded dataset slice must not contain duplicate row keys");
        Self { data }
    }

    fn stored_row(&self, row_key: RowKey) -> Option<LocalStoredStateRow> {
        let row = self.data.get_row(&row_key.0)?;
        Some(LocalStoredStateRow {
            snapshot: row.snapshot(),
            tombstoned: row.is_tombstoned(),
        })
    }

    fn row_is_tombstoned(&self, row_key: RowKey) -> Option<bool> {
        self.data.row_is_tombstoned(&row_key.0)
    }

    fn clone_row(&self, row_key: RowKey) -> Option<flotsync_data_types::OwnedStateRow<UpdateId>> {
        let row = self.data.get_row(&row_key.0)?;
        let mut fields = HashMap::with_capacity(self.data.num_fields());
        for field_name in self.data.field_names() {
            let value = row
                .get_field(field_name)
                .expect("dataset field iteration must resolve against the same row");
            fields.insert(field_name.to_owned(), value.clone());
        }
        Some(flotsync_data_types::OwnedStateRow::new(fields))
    }

    /// Snapshot the current row image for explicit durable row writes.
    fn snapshot_row(&self, row_key: RowKey) -> Option<ReplicationRowStateSnapshot> {
        self.data.get_row(&row_key.0).map(|row| row.snapshot())
    }
}

impl RowValuesPatch {
    fn into_initial_values<'schema>(
        self,
        schema: &'schema Schema,
        row_id: &RowId,
    ) -> Result<Vec<InitialFieldValue<'schema>>, PublishChangesError> {
        let mut initial_values = Vec::with_capacity(self.fields.len());
        for (field_name, value) in self.fields {
            let field = schema.columns.get(field_name.as_str()).with_context(|| {
                publish::UnknownSchemaFieldSnafu {
                    row_id: row_id.clone(),
                    dataset_id: row_id.dataset_id.clone(),
                    field_name,
                }
            })?;
            let initial_value = field.initial(value).map_err(Box::new).with_context(|_| {
                publish::InvalidFieldValueSnafu {
                    row_id: row_id.clone(),
                    dataset_id: row_id.dataset_id.clone(),
                }
            })?;
            initial_values.push(initial_value);
        }
        Ok(initial_values)
    }

    fn into_pending_updates<'schema>(
        self,
        schema: &'schema Schema,
        row_id: &RowId,
    ) -> Result<Vec<PendingFieldUpdate<'schema>>, PublishChangesError> {
        let mut pending_updates = Vec::with_capacity(self.fields.len());
        for (field_name, value) in self.fields {
            let field = schema.columns.get(field_name.as_str()).with_context(|| {
                publish::UnknownSchemaFieldSnafu {
                    row_id: row_id.clone(),
                    dataset_id: row_id.dataset_id.clone(),
                    field_name,
                }
            })?;
            let pending_update = field.set(value).map_err(Box::new).with_context(|_| {
                publish::InvalidFieldValueSnafu {
                    row_id: row_id.clone(),
                    dataset_id: row_id.dataset_id.clone(),
                }
            })?;
            pending_updates.push(pending_update);
        }
        Ok(pending_updates)
    }
}

/// One publish batch scoped to a single group and the touched rows per dataset.
pub(super) struct TouchedGroupRows {
    pub(super) group_id: GroupId,
    pub(super) dataset_rows: HashMap<DatasetId, HashSet<RowKey>>,
}

/// Validate that one publish call targets exactly one group and collect the
/// touched row keys for each dataset without imposing semantic ordering on the
/// original mutation list.
pub(super) fn collect_group_row_scope(
    changes: &[RowMutation],
) -> Result<TouchedGroupRows, PublishChangesError> {
    let Some(first_change) = changes.first() else {
        return publish::EmptyChangesSnafu.fail();
    };
    let group_id = first_change.row_id().group_id;
    let mut dataset_rows: HashMap<DatasetId, HashSet<RowKey>> = HashMap::new();
    for change in changes {
        let row_id = change.row_id();
        ensure!(
            row_id.group_id == group_id,
            publish::MixedGroupsSnafu {
                first_group_id: group_id,
                other_group_id: row_id.group_id,
            }
        );
        dataset_rows
            .entry(row_id.dataset_id.clone())
            .or_default()
            .insert(row_id.row_key);
    }
    Ok(TouchedGroupRows {
        group_id,
        dataset_rows,
    })
}

/// Collect the row keys touched by the given persisted updates.
///
/// Schemas are required because stored update records carry encoded schema
/// operations rather than decoded row operations.
pub(super) fn collect_record_row_scope(
    updates: &[ReplicationUpdateRecord],
    schemas: &HashMap<DatasetId, SchemaSource>,
) -> Result<HashMap<DatasetId, HashSet<RowKey>>, InboundDeliveryError> {
    let mut dataset_rows: HashMap<DatasetId, HashSet<RowKey>> = HashMap::new();
    for update in updates {
        for dataset_update in &update.dataset_updates {
            let schema = schemas
                .get(&dataset_update.dataset_id)
                .expect("touched inbound dataset schemas must be pre-loaded");
            for operation in &dataset_update.operations {
                let operation = decode_update_schema_operation(
                    update,
                    dataset_update,
                    operation.clone(),
                    schema.as_schema(),
                )?;
                let row_key = match operation.operation {
                    RowOperation::Insert { row_id, .. }
                    | RowOperation::Update { row_id, .. }
                    | RowOperation::Delete { row_id } => RowKey(row_id),
                };
                dataset_rows
                    .entry(dataset_update.dataset_id.clone())
                    .or_default()
                    .insert(row_key);
            }
        }
    }
    Ok(dataset_rows)
}

/// Validate that every schema operation embedded in one replication update is
/// decodable for the local schema and bound to the update's `UpdateId`.
pub(super) fn validate_update_mapping(
    update: &ReplicationUpdateRecord,
    schemas: &HashMap<DatasetId, SchemaSource>,
) -> Result<(), InboundDeliveryError> {
    for dataset_update in &update.dataset_updates {
        let schema = schemas
            .get(&dataset_update.dataset_id)
            .expect("inbound update schemas must be pre-loaded before validation");
        for operation in &dataset_update.operations {
            decode_update_schema_operation(
                update,
                dataset_update,
                operation.clone(),
                schema.as_schema(),
            )?;
        }
    }
    Ok(())
}

/// Returns the mutable working dataset image used for one inbound apply batch.
///
/// Callers are expected to pre-load every dataset touched by the causal apply
/// chain before the first operation is decoded.
fn working_dataset_for_inbound<'dataset>(
    working_datasets: &'dataset mut HashMap<DatasetId, LocalDataset>,
    dataset_id: &DatasetId,
) -> &'dataset mut LocalDataset {
    working_datasets
        .get_mut(dataset_id)
        .expect("touched inbound dataset must be pre-loaded before apply")
}

/// One prepared local publish batch together with the corresponding durable
/// row patches.
pub(super) struct PreparedLocalChanges {
    pub(super) dataset_updates: Vec<DatasetUpdateRecord>,
    pub(super) row_patches: Vec<DatasetRowStatePatch>,
    pub(super) row_changes: Vec<RowChange>,
}

/// One applied inbound batch together with the corresponding durable row patches.
pub(super) struct AppliedInboundBatch {
    pub(super) row_changes: Vec<RowChange>,
    pub(super) row_patches: Vec<DatasetRowStatePatch>,
}

/// One staged local mutation together with its explicit durable row write.
pub(super) struct AppliedLocalOperation {
    pub(super) encoded_operation: flotsync_messages::datamodel::SchemaOperation,
    pub(super) row_change: Option<RowChange>,
    pub(super) row_write: DatasetRowStateWrite,
}

struct AppliedRemoteOperation {
    /// Listener-visible change for this operation.
    ///
    /// `None` means the durable tombstone image changed, but the row was
    /// already deleted from the application's visible set and should not emit a
    /// second delete or a resurrection upsert.
    row_change: Option<RowChange>,
    row_write: DatasetRowStateWrite,
}

/// Applies one causally-ready inbound batch against one local group state.
///
/// All touched datasets are first materialised into working copies so the batch
/// either commits atomically into local state or returns an error without
/// partially replacing dataset maps.
pub(super) fn apply_one_update(
    group: &mut LoadedGroupMeta,
    working_datasets: &mut HashMap<DatasetId, LocalDataset>,
    update: &ReplicationUpdateRecord,
) -> Result<AppliedInboundBatch, InboundDeliveryError> {
    let mut row_changes = Vec::new();
    let mut row_patches = Vec::new();
    let last_changed_versions = update.read_versions.with_update_applied(update.update_id);
    for dataset_update in &update.dataset_updates {
        let working_dataset =
            working_dataset_for_inbound(working_datasets, &dataset_update.dataset_id);
        let mut row_writes = Vec::new();
        for operation in &dataset_update.operations {
            let schema = working_dataset.data.schema().clone();
            let operation =
                decode_update_schema_operation(update, dataset_update, operation.clone(), &schema)?;
            let applied_operation = apply_remote_operation(
                working_dataset,
                update.group_id,
                &dataset_update.dataset_id,
                operation,
            )?;
            if let Some(row_change) = applied_operation.row_change {
                row_changes.push(row_change);
            }
            row_writes.push(applied_operation.row_write);
        }
        if !row_writes.is_empty() {
            row_patches.push(DatasetRowStatePatch {
                group_id: update.group_id,
                dataset_id: dataset_update.dataset_id.clone(),
                actions: row_writes,
                last_changed_versions: last_changed_versions.clone(),
            });
        }
    }
    group.mark_applied(update.update_id);
    Ok(AppliedInboundBatch {
        row_changes,
        row_patches,
    })
}

fn decode_update_schema_operation<'schema>(
    update: &ReplicationUpdateRecord,
    dataset_update: &DatasetUpdateRecord,
    operation: flotsync_messages::datamodel::SchemaOperation,
    schema: &'schema Schema,
) -> Result<flotsync_messages::SchemaOperation<'schema>, InboundDeliveryError> {
    let operation = decode_schema_operation(operation, schema).context(
        inbound::DecodeSchemaOperationSnafu {
            dataset_id: dataset_update.dataset_id.clone(),
        },
    )?;
    ensure!(
        operation.change_id == update.update_id,
        inbound::UpdateOperationIdMismatchSnafu {
            group: update.group_id,
            update: update.update_id,
            dataset: dataset_update.dataset_id.clone(),
            operation_change: operation.change_id,
        }
    );
    Ok(operation)
}

/// Applies one local upsert and returns the encoded schema operation, if any.
///
/// A local upsert may still produce no transport operation when the new row
/// image is identical to what is already stored locally.
pub(super) fn apply_local_upsert(
    dataset: &mut LocalDataset,
    row_id: &RowId,
    row: RowValuesPatch,
    update_id: UpdateId,
) -> Result<Option<AppliedLocalOperation>, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    let encoded_operation = {
        let Some(operation) = apply_local_upsert_operation(dataset, row_id, row, update_id)? else {
            return Ok(None);
        };
        encode_publish_operation(row_id, &schema, &operation)?
    };
    let row_snapshot = dataset
        .snapshot_row(row_id.row_key)
        .unwrap_or_else(|| panic!("applied local upsert must leave row {row_id} readable"));
    let row = dataset
        .clone_row(row_id.row_key)
        .unwrap_or_else(|| panic!("applied local upsert must leave row {row_id} readable"));
    Ok(Some(AppliedLocalOperation {
        encoded_operation,
        row_change: Some(RowChange::Upsert {
            row_id: row_id.clone(),
            row: Arc::new(row),
        }),
        row_write: DatasetRowStateWrite::UpsertActive {
            row_key: row_id.row_key,
            snapshot: row_snapshot,
        },
    }))
}

/// Applies one local delete and encodes the resulting schema operation for transport.
pub(super) fn apply_local_delete(
    dataset: &mut LocalDataset,
    row_id: &RowId,
    update_id: UpdateId,
) -> Result<AppliedLocalOperation, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    let encoded_operation = {
        let operation = apply_local_delete_operation(dataset, row_id, update_id)?;
        encode_publish_operation(row_id, &schema, &operation)?
    };
    let row = dataset
        .stored_row(row_id.row_key)
        .unwrap_or_else(|| panic!("applied local delete must leave row {row_id} snapshotable"));
    debug_assert!(row.tombstoned);
    Ok(AppliedLocalOperation {
        encoded_operation,
        row_change: Some(RowChange::Delete {
            row_id: row_id.clone(),
        }),
        row_write: DatasetRowStateWrite::UpsertTombstone {
            row_key: row_id.row_key,
            snapshot: row.snapshot,
        },
    })
}

/// Generate one local upsert operation from `base_dataset`, then apply that
/// encoded operation to `current_dataset`.
pub(super) fn apply_rebased_local_upsert(
    base_dataset: &mut LocalDataset,
    current_dataset: &mut LocalDataset,
    row_id: &RowId,
    row: RowValuesPatch,
    update_id: UpdateId,
) -> Result<Option<AppliedLocalOperation>, PublishChangesError> {
    let schema = base_dataset.data.schema().clone();
    let Some(operation) = apply_local_upsert_operation(base_dataset, row_id, row, update_id)?
    else {
        return Ok(None);
    };
    let encoded_operation = encode_publish_operation(row_id, &schema, &operation)?;
    apply_decoded_publish_operation(current_dataset, row_id, operation, encoded_operation).map(Some)
}

/// Generate one local delete operation from `base_dataset`, then apply that
/// encoded operation to `current_dataset`.
pub(super) fn apply_rebased_local_delete(
    base_dataset: &mut LocalDataset,
    current_dataset: &mut LocalDataset,
    row_id: &RowId,
    update_id: UpdateId,
) -> Result<AppliedLocalOperation, PublishChangesError> {
    let schema = base_dataset.data.schema().clone();
    let operation = apply_local_delete_operation(base_dataset, row_id, update_id)?;
    let encoded_operation = encode_publish_operation(row_id, &schema, &operation)?;
    apply_decoded_publish_operation(current_dataset, row_id, operation, encoded_operation)
}

fn apply_local_upsert_operation<'dataset>(
    dataset: &'dataset mut LocalDataset,
    row_id: &RowId,
    row: RowValuesPatch,
    update_id: UpdateId,
) -> Result<Option<flotsync_messages::SchemaOperation<'dataset>>, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    if dataset.data.get_row(&row_id.row_key.0).is_some() {
        let pending_updates = row.into_pending_updates(&schema, row_id)?;
        match dataset
            .data
            .modify_row(update_id, row_id.row_key.0, pending_updates)
            .context(publish::ApplyLocalMutationSnafu {
                row_id: row_id.clone(),
            })? {
            OperationOutcome::Applied(operation) => Ok(Some(operation)),
            OperationOutcome::NoChanges => Ok(None),
        }
    } else {
        let initial_values = row.into_initial_values(&schema, row_id)?;
        let operation = dataset
            .data
            .insert_row(update_id, row_id.row_key.0, initial_values)
            .context(publish::ApplyLocalMutationSnafu {
                row_id: row_id.clone(),
            })?;
        Ok(Some(operation))
    }
}

fn apply_local_delete_operation<'dataset>(
    dataset: &'dataset mut LocalDataset,
    row_id: &RowId,
    update_id: UpdateId,
) -> Result<flotsync_messages::SchemaOperation<'dataset>, PublishChangesError> {
    dataset
        .data
        .delete_row(update_id, row_id.row_key.0)
        .context(publish::ApplyLocalMutationSnafu {
            row_id: row_id.clone(),
        })
}

fn encode_publish_operation(
    row_id: &RowId,
    schema: &Schema,
    operation: &flotsync_messages::SchemaOperation<'_>,
) -> Result<flotsync_messages::datamodel::SchemaOperation, PublishChangesError> {
    encode_schema_operation(operation, schema).context(publish::EncodeOperationSnafu {
        dataset_id: row_id.dataset_id.clone(),
    })
}

fn apply_decoded_publish_operation(
    dataset: &mut LocalDataset,
    row_id: &RowId,
    operation: flotsync_messages::SchemaOperation<'_>,
    encoded_operation: flotsync_messages::datamodel::SchemaOperation,
) -> Result<AppliedLocalOperation, PublishChangesError> {
    let was_tombstoned = dataset.row_is_tombstoned(row_id.row_key).unwrap_or(false);

    dataset.data = dataset
        .data
        .clone()
        .apply_schema_operation(operation)
        .context(publish::ApplyLocalMutationSnafu {
            row_id: row_id.clone(),
        })?;

    let stored_row = dataset
        .stored_row(row_id.row_key)
        .unwrap_or_else(|| panic!("applied local operation must leave row {row_id} snapshotable"));
    let row_change = if stored_row.tombstoned {
        if was_tombstoned {
            None
        } else {
            Some(RowChange::Delete {
                row_id: row_id.clone(),
            })
        }
    } else {
        let row = dataset
            .clone_row(row_id.row_key)
            .unwrap_or_else(|| panic!("applied local upsert must leave row {row_id} readable"));
        Some(RowChange::Upsert {
            row_id: row_id.clone(),
            row: Arc::new(row),
        })
    };
    Ok(AppliedLocalOperation {
        encoded_operation,
        row_change,
        row_write: if stored_row.tombstoned {
            DatasetRowStateWrite::UpsertTombstone {
                row_key: row_id.row_key,
                snapshot: stored_row.snapshot,
            }
        } else {
            DatasetRowStateWrite::UpsertActive {
                row_key: row_id.row_key,
                snapshot: stored_row.snapshot,
            }
        },
    })
}

fn apply_remote_operation(
    dataset: &mut LocalDataset,
    group_id: GroupId,
    dataset_id: &DatasetId,
    operation: flotsync_messages::SchemaOperation<'_>,
) -> Result<AppliedRemoteOperation, InboundDeliveryError> {
    let api_row_id = match &operation.operation {
        RowOperation::Insert { row_id, .. }
        | RowOperation::Update { row_id, .. }
        | RowOperation::Delete { row_id } => RowId {
            group_id,
            dataset_id: dataset_id.clone(),
            row_key: RowKey(*row_id),
        },
    };
    let was_tombstoned = dataset
        .row_is_tombstoned(api_row_id.row_key)
        .unwrap_or(false);

    // flotsync_messages::InMemoryStateData currently consumes `self` when applying
    // one schema operation, so the runtime must clone the current dataset image
    // before replacing it with the updated result.
    dataset.data = dataset
        .data
        .clone()
        .apply_schema_operation(operation)
        .context(inbound::ApplyInboundMutationSnafu {
            row_id: api_row_id.clone(),
        })?;

    let stored_row = dataset.stored_row(api_row_id.row_key).unwrap_or_else(|| {
        panic!("applied inbound operation must leave row {api_row_id} snapshotable")
    });
    let row_change = if stored_row.tombstoned {
        if was_tombstoned {
            None
        } else {
            Some(RowChange::Delete {
                row_id: api_row_id.clone(),
            })
        }
    } else {
        Some({
            let row = dataset.clone_row(api_row_id.row_key).unwrap_or_else(|| {
                panic!("applied inbound upsert must leave row {api_row_id} readable")
            });
            RowChange::Upsert {
                row_id: api_row_id.clone(),
                row: Arc::new(row),
            }
        })
    };
    Ok(AppliedRemoteOperation {
        row_change,
        row_write: if stored_row.tombstoned {
            DatasetRowStateWrite::UpsertTombstone {
                row_key: api_row_id.row_key,
                snapshot: stored_row.snapshot,
            }
        } else {
            DatasetRowStateWrite::UpsertActive {
                row_key: api_row_id.row_key,
                snapshot: stored_row.snapshot,
            }
        },
    })
}

use super::errors::*;
use crate::{
    GroupMembers,
    api::{
        DatasetId,
        DatasetSnapshotRowAction,
        DatasetSnapshotWrite,
        GroupId,
        MemberIdentity,
        MemberIndex,
        MutableRow,
        ReplicationGroupRecord,
        ReplicationUpdateRecord,
        RowChange,
        RowId,
        RowKey,
        RowMutation,
    },
};
use flotsync_core::versions::{UpdateId, VersionVector};
use flotsync_data_types::{
    InitialFieldValue,
    OperationOutcome,
    PendingFieldUpdate,
    RowRead,
    Schema,
    TableOperations,
    schema::datamodel::RowSnapshot,
};
use flotsync_messages::codecs::datamodel::{decode_schema_operation, encode_schema_operation};
use snafu::prelude::*;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};

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
        let members = GroupMembers::from_ordered_members(group.members)
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
                simulated_group.can_apply(update).then_some(*update_id)
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
        }
    }
}

/// One transaction-local decision about pending persisted updates.
pub(super) struct PendingApplyPlan {
    pub(super) already_applied: Vec<UpdateId>,
    pub(super) ready_chain: Vec<ReplicationUpdateRecord>,
}

/// One local dataset together with its current replicated in-memory contents.
#[derive(Clone)]
pub(super) struct LocalDataset {
    pub(super) data: flotsync_messages::InMemoryData,
}

impl LocalDataset {
    pub(super) fn new(schema: Arc<Schema>) -> Self {
        Self {
            data: flotsync_messages::InMemoryData::with_owned_schema(schema.as_ref().clone()),
        }
    }

    fn clone_row(&self, row_key: RowKey) -> Option<flotsync_data_types::OwnedRow<UpdateId>> {
        let row = self.data.get_row(&row_key.0)?;
        let mut fields = HashMap::with_capacity(self.data.num_fields());
        for field_name in self.data.field_names() {
            let value = row
                .get_field(field_name)
                .expect("dataset field iteration must resolve against the same row");
            fields.insert(field_name.to_owned(), value.clone());
        }
        Some(flotsync_data_types::OwnedRow::new(fields))
    }

    /// Materialise the active row set as deterministic owned snapshots.
    fn active_row_snapshots(&self) -> BTreeMap<RowKey, RowSnapshot<'static, UpdateId>> {
        let mut rows = BTreeMap::new();
        for row_id in self.data.active_row_ids() {
            let row = self
                .data
                .get_row(row_id)
                .expect("active row ids must resolve against the same dataset");
            let mut fields = Vec::with_capacity(self.data.num_fields());
            for field_name in self.data.field_names() {
                let value = row
                    .get_field(field_name)
                    .expect("field iteration must resolve against the same row")
                    .clone();
                fields.push((field_name.to_owned(), value));
            }
            rows.insert(RowKey(*row_id), RowSnapshot::from_owned_fields(fields));
        }
        rows
    }
}

impl MutableRow {
    fn into_initial_values<'schema>(
        self,
        schema: &'schema Schema,
        row_id: &RowId,
    ) -> Result<Vec<InitialFieldValue<'schema>>, PublishChangesError> {
        let mut initial_values = Vec::with_capacity(self.fields.len());
        for (field_name, value) in self.fields {
            let field =
                schema
                    .columns
                    .get(field_name.as_str())
                    .context(UnknownSchemaFieldSnafu {
                        row_id: row_id.clone(),
                        dataset_id: row_id.dataset_id.clone(),
                        field_name,
                    })?;
            let initial_value =
                field
                    .initial(value)
                    .map_err(Box::new)
                    .context(InvalidFieldValueSnafu {
                        row_id: row_id.clone(),
                        dataset_id: row_id.dataset_id.clone(),
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
            let field =
                schema
                    .columns
                    .get(field_name.as_str())
                    .context(UnknownSchemaFieldSnafu {
                        row_id: row_id.clone(),
                        dataset_id: row_id.dataset_id.clone(),
                        field_name,
                    })?;
            let pending_update =
                field
                    .set(value)
                    .map_err(Box::new)
                    .context(InvalidFieldValueSnafu {
                        row_id: row_id.clone(),
                        dataset_id: row_id.dataset_id.clone(),
                    })?;
            pending_updates.push(pending_update);
        }
        Ok(pending_updates)
    }
}

/// Validate that one publish call targets exactly one group and collect the
/// touched datasets without imposing any semantic ordering on them.
pub(super) fn collect_group_dataset_scope(
    changes: &[RowMutation],
) -> Result<(GroupId, HashSet<DatasetId>), PublishChangesError> {
    let Some(first_change) = changes.first() else {
        return EmptyChangesSnafu.fail();
    };
    let group_id = first_change.row_id().group_id;
    let mut dataset_ids = HashSet::new();
    for change in changes {
        let row_id = change.row_id();
        ensure!(
            row_id.group_id == group_id,
            MixedGroupsSnafu {
                first_group_id: group_id,
                other_group_id: row_id.group_id,
            }
        );
        dataset_ids.insert(row_id.dataset_id.clone());
    }
    Ok((group_id, dataset_ids))
}

/// Returns the mutable working dataset image used for one outbound publish batch.
///
/// Callers are expected to pre-load every touched dataset before publish
/// staging starts, so reaching a missing dataset here is a runtime bug rather
/// than a schema-loading decision.
pub(super) fn working_dataset_for_publish<'a>(
    working_datasets: &'a mut HashMap<DatasetId, LocalDataset>,
    dataset_id: &DatasetId,
) -> Result<&'a mut LocalDataset, PublishChangesError> {
    Ok(working_datasets
        .get_mut(dataset_id)
        .expect("touched publish dataset must be pre-loaded before staging"))
}

/// Returns the mutable working dataset image used for one inbound apply batch.
///
/// Callers are expected to pre-load every dataset touched by the causal apply
/// chain before the first operation is decoded.
fn working_dataset_for_inbound<'a>(
    working_datasets: &'a mut HashMap<DatasetId, LocalDataset>,
    dataset_id: &DatasetId,
) -> Result<&'a mut LocalDataset, InboundDeliveryError> {
    Ok(working_datasets
        .get_mut(dataset_id)
        .expect("touched inbound dataset must be pre-loaded before apply"))
}

/// Applies one causally-ready inbound batch against one local group state.
///
/// All touched datasets are first materialised into working copies so the batch
/// either commits atomically into local state or returns an error without
/// partially replacing dataset maps.
pub(super) fn apply_one_update_batch(
    group: &mut LoadedGroupMeta,
    working_datasets: &mut HashMap<DatasetId, LocalDataset>,
    update: &ReplicationUpdateRecord,
) -> Result<Vec<RowChange>, InboundDeliveryError> {
    let mut row_changes = Vec::new();
    for dataset_update in &update.dataset_updates {
        let working_dataset =
            working_dataset_for_inbound(working_datasets, &dataset_update.dataset_id)?;
        for operation in &dataset_update.operations {
            let schema = working_dataset.data.schema().clone();
            let operation = decode_schema_operation(operation.clone(), &schema).context(
                DecodeSchemaOperationSnafu {
                    dataset_id: dataset_update.dataset_id.clone(),
                },
            )?;
            assert_eq!(
                operation.change_id, update.update_id,
                "decoded inbound operation for dataset '{}' carried change id {}, expected {}",
                dataset_update.dataset_id, operation.change_id, update.update_id,
            );
            let row_change = apply_remote_operation(
                working_dataset,
                update.group_id,
                &dataset_update.dataset_id,
                operation,
            )?;
            row_changes.push(row_change);
        }
    }
    group.mark_applied(update.update_id);
    Ok(row_changes)
}

/// Compute the minimal durable row-action set that turns `before` into `after`.
pub(super) fn build_dataset_snapshot_write(
    group_id: GroupId,
    dataset_id: DatasetId,
    before: Option<&LocalDataset>,
    after: &LocalDataset,
) -> Option<DatasetSnapshotWrite> {
    let before_rows = before
        .map(LocalDataset::active_row_snapshots)
        .unwrap_or_default();
    let after_rows = after.active_row_snapshots();
    let mut row_actions = Vec::new();

    for (row_key, row) in &after_rows {
        match before_rows.get(row_key) {
            None => row_actions.push(DatasetSnapshotRowAction::Insert {
                row_key: *row_key,
                row: row.clone(),
            }),
            Some(existing_row) if existing_row != row => {
                row_actions.push(DatasetSnapshotRowAction::Update {
                    row_key: *row_key,
                    row: row.clone(),
                });
            }
            Some(_) => {}
        }
    }

    for row_key in before_rows.keys() {
        if !after_rows.contains_key(row_key) {
            row_actions.push(DatasetSnapshotRowAction::Delete { row_key: *row_key });
        }
    }

    if row_actions.is_empty() {
        return None;
    }

    Some(DatasetSnapshotWrite {
        group_id,
        dataset_id,
        row_actions,
    })
}

/// Applies one local upsert and returns the encoded schema operation, if any.
///
/// A local upsert may still produce no transport operation when the new row
/// image is identical to what is already stored locally.
pub(super) fn apply_local_upsert(
    dataset: &mut LocalDataset,
    row_id: &crate::api::RowId,
    row: MutableRow,
    update_id: UpdateId,
) -> Result<Option<flotsync_messages::datamodel::SchemaOperation>, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    let operation = if dataset.data.get_row(&row_id.row_key.0).is_some() {
        let pending_updates = row.into_pending_updates(&schema, row_id)?;
        match dataset
            .data
            .modify_row(update_id, row_id.row_key.0, pending_updates)
            .context(ApplyLocalMutationSnafu {
                row_id: row_id.clone(),
            })? {
            OperationOutcome::Applied(operation) => Some(operation),
            OperationOutcome::NoChanges => None,
        }
    } else {
        let initial_values = row.into_initial_values(&schema, row_id)?;
        let operation = dataset
            .data
            .insert_row(update_id, row_id.row_key.0, initial_values)
            .context(ApplyLocalMutationSnafu {
                row_id: row_id.clone(),
            })?;
        Some(operation)
    };

    let Some(operation) = operation else {
        return Ok(None);
    };
    let encoded_operation =
        encode_schema_operation(&operation, &schema).context(EncodeOperationSnafu {
            dataset_id: row_id.dataset_id.clone(),
        })?;
    Ok(Some(encoded_operation))
}

/// Applies one local delete and encodes the resulting schema operation for transport.
pub(super) fn apply_local_delete(
    dataset: &mut LocalDataset,
    row_id: &crate::api::RowId,
    update_id: UpdateId,
) -> Result<flotsync_messages::datamodel::SchemaOperation, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    let operation = dataset
        .data
        .delete_row(update_id, row_id.row_key.0)
        .context(ApplyLocalMutationSnafu {
            row_id: row_id.clone(),
        })?;
    let encoded_operation =
        encode_schema_operation(&operation, &schema).context(EncodeOperationSnafu {
            dataset_id: row_id.dataset_id.clone(),
        })?;
    Ok(encoded_operation)
}

fn apply_remote_operation(
    dataset: &mut LocalDataset,
    group_id: GroupId,
    dataset_id: &DatasetId,
    operation: flotsync_messages::SchemaOperation<'_>,
) -> Result<RowChange, InboundDeliveryError> {
    use flotsync_data_types::schema::datamodel::RowOperation;

    let api_row_id = match &operation.operation {
        RowOperation::Insert { row_id, .. }
        | RowOperation::Update { row_id, .. }
        | RowOperation::Delete { row_id } => crate::api::RowId {
            group_id,
            dataset_id: dataset_id.clone(),
            row_key: crate::api::RowKey(*row_id),
        },
    };
    let change_kind = match &operation.operation {
        RowOperation::Delete { .. } => AppliedChangeKind::Delete,
        RowOperation::Insert { .. } | RowOperation::Update { .. } => AppliedChangeKind::Upsert,
    };

    // flotsync_messages::InMemoryData currently consumes `self` when applying
    // one schema operation, so the runtime must clone the current dataset image
    // before replacing it with the updated result.
    dataset.data = dataset
        .data
        .clone()
        .apply_schema_operation(operation)
        .context(ApplyInboundMutationSnafu {
            row_id: api_row_id.clone(),
        })?;

    match change_kind {
        AppliedChangeKind::Delete => Ok(RowChange::Delete { row_id: api_row_id }),
        AppliedChangeKind::Upsert => {
            let row = dataset.clone_row(api_row_id.row_key).unwrap_or_else(|| {
                panic!("applied inbound upsert must leave row {api_row_id} readable")
            });
            Ok(RowChange::Upsert {
                row_id: api_row_id,
                row: Arc::new(row),
            })
        }
    }
}

#[derive(Clone, Copy)]
enum AppliedChangeKind {
    Upsert,
    Delete,
}

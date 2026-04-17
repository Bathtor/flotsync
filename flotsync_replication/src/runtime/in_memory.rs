use super::{component::BufferedInboundUpdate, errors::*, messages::UpdateBatchMessage};
use crate::{
    GroupMembers,
    GroupMemberships,
    api::{
        DatasetId,
        GroupId,
        MemberIdentity,
        MemberIndex,
        MutableRow,
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
    RowOperations,
    Schema,
    TableOperations,
};
use flotsync_messages::codecs::datamodel::{decode_schema_operation, encode_schema_operation};
use snafu::prelude::*;
use std::{
    collections::{BTreeMap, HashMap, HashSet, btree_map::Entry},
    num::NonZeroUsize,
    sync::Arc,
};

/// In-memory local-group state for the current replication runtime.
///
/// This is the temporary runtime backend until a persistent store-backed
/// implementation takes over the same responsibilities.
#[derive(Default)]
pub(super) struct LocalRuntimeState {
    pub(super) groups: HashMap<GroupId, LocalGroupState>,
}

impl LocalRuntimeState {
    pub(super) fn membership_snapshot(&self) -> GroupMemberships {
        GroupMemberships::from_groups(
            self.groups
                .iter()
                .map(|(group_id, group)| (*group_id, group.members.clone())),
        )
    }
}

/// One fixed-membership replication group currently hosted locally.
pub(super) struct LocalGroupState {
    pub(super) members: GroupMembers,
    pub(super) local_member_index: MemberIndex,
    pub(super) version_vector: VersionVector,
    pub(super) datasets: HashMap<DatasetId, LocalDataset>,
    pub(super) pending_updates: BTreeMap<UpdateId, BufferedInboundUpdate>,
}

impl LocalGroupState {
    pub(super) fn new(
        local_member: &MemberIdentity,
        members: GroupMembers,
    ) -> Result<Self, GroupInstallError> {
        let local_member_index =
            members
                .member_index(local_member)
                .context(InstallMissingLocalMemberSnafu {
                    local_member: local_member.clone(),
                })?;
        let member_count = NonZeroUsize::new(members.len())
            .expect("group installation must only happen for non-empty member sets");
        Ok(Self {
            members,
            local_member_index,
            version_vector: VersionVector::initial(member_count),
            datasets: HashMap::new(),
            pending_updates: BTreeMap::new(),
        })
    }

    /// Return the subset of `dataset_ids` that is not hosted locally yet.
    pub(super) fn missing_dataset_ids(
        &self,
        dataset_ids: &HashSet<DatasetId>,
    ) -> HashSet<DatasetId> {
        dataset_ids
            .iter()
            .filter(|dataset_id| !self.datasets.contains_key(*dataset_id))
            .cloned()
            .collect()
    }

    /// Return the fixed member count for this hosted group.
    pub(super) fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.members.len()).expect("hosted group must be non-empty")
    }

    /// Return the locally applied version for the given member index.
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

    /// Return `true` when `update_id` is already reflected in the local VV.
    pub(super) fn has_applied(&self, update_id: UpdateId) -> bool {
        self.applied_version(MemberIndex::new(update_id.node_index)) >= update_id.version
    }

    /// Return `true` when `message` is causally ready and is the next version
    /// expected from its producer.
    pub(super) fn can_apply(&self, message: &UpdateBatchMessage) -> bool {
        let producer_index = MemberIndex::new(message.update_id.node_index);
        (self.version_vector >= message.read_versions)
            && self.expected_next_version(producer_index) == message.update_id.version
    }

    /// Buffer one inbound update until its causal dependencies become ready.
    pub(super) fn buffer_update(
        &mut self,
        pending_update: BufferedInboundUpdate,
    ) -> Result<(), InboundDeliveryError> {
        let update_id = pending_update.message.update_id;
        match self.pending_updates.entry(update_id) {
            Entry::Vacant(entry) => {
                entry.insert(pending_update);
                Ok(())
            }
            Entry::Occupied(entry) => {
                if entry.get().message == pending_update.message {
                    Ok(())
                } else {
                    ConflictingBufferedUpdateSnafu {
                        group_id: pending_update.message.group_id,
                        update_id,
                    }
                    .fail()
                }
            }
        }
    }

    /// Drop already-applied duplicates, then return one causally-ready pending
    /// update when any exist.
    pub(super) fn take_next_actionable_pending_update(&mut self) -> Option<BufferedInboundUpdate> {
        let duplicate_keys: Vec<_> = self
            .pending_updates
            .keys()
            .copied()
            .filter(|update_id| self.has_applied(*update_id))
            .collect();
        for update_id in duplicate_keys {
            self.pending_updates.remove(&update_id);
        }

        let ready_key = self
            .pending_updates
            .iter()
            .find_map(|(update_id, pending_update)| {
                self.can_apply(&pending_update.message)
                    .then_some(*update_id)
            });
        ready_key.and_then(|update_id| self.pending_updates.remove(&update_id))
    }
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
/// The batch may touch a mix of already-hosted datasets and newly-loaded schema
/// definitions, so this helper either clones the current local dataset image or
/// seeds one from a freshly loaded schema.
pub(super) fn working_dataset_for_publish<'a>(
    working_datasets: &'a mut HashMap<DatasetId, LocalDataset>,
    local_group: &LocalGroupState,
    loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
    dataset_id: &DatasetId,
) -> Result<&'a mut LocalDataset, PublishChangesError> {
    if !working_datasets.contains_key(dataset_id) {
        let working_dataset = local_group
            .datasets
            .get(dataset_id)
            .cloned()
            .or_else(|| {
                loaded_schemas
                    .get(dataset_id)
                    .cloned()
                    .map(LocalDataset::new)
            })
            .context(MissingDatasetSchemaSnafu {
                dataset_id: dataset_id.clone(),
            })?;
        working_datasets.insert(dataset_id.clone(), working_dataset);
    }
    Ok(working_datasets
        .get_mut(dataset_id)
        .expect("working publish dataset must exist after insertion"))
}

/// Returns the mutable working dataset image used for one inbound apply batch.
///
/// The caller passes already-loaded schemas for datasets that were not hosted
/// locally yet, while existing datasets are cloned so the whole batch can apply
/// against one isolated working set before committing back into local state.
fn working_dataset_for_inbound<'a>(
    working_datasets: &'a mut HashMap<DatasetId, LocalDataset>,
    local_group: &LocalGroupState,
    loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
    dataset_id: &DatasetId,
) -> Result<&'a mut LocalDataset, InboundDeliveryError> {
    if !working_datasets.contains_key(dataset_id) {
        let working_dataset = local_group
            .datasets
            .get(dataset_id)
            .cloned()
            .or_else(|| {
                loaded_schemas
                    .get(dataset_id)
                    .cloned()
                    .map(LocalDataset::new)
            })
            .context(InboundMissingDatasetSchemaSnafu {
                dataset_id: dataset_id.clone(),
            })?;
        working_datasets.insert(dataset_id.clone(), working_dataset);
    }
    Ok(working_datasets
        .get_mut(dataset_id)
        .expect("working inbound dataset must exist after insertion"))
}

/// Applies one causally-ready inbound batch against one local group state.
///
/// All touched datasets are first materialised into working copies so the batch
/// either commits atomically into local state or returns an error without
/// partially replacing dataset maps.
pub(super) fn apply_one_update_batch(
    local_group: &mut LocalGroupState,
    message: UpdateBatchMessage,
    loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
) -> Result<Vec<RowChange>, InboundDeliveryError> {
    let mut working_datasets = HashMap::<DatasetId, LocalDataset>::new();
    let mut row_changes = Vec::new();
    for dataset_update in message.dataset_updates {
        let working_dataset = working_dataset_for_inbound(
            &mut working_datasets,
            local_group,
            loaded_schemas,
            &dataset_update.dataset_id,
        )?;
        for operation in dataset_update.operations {
            let schema = working_dataset.data.schema().clone();
            let operation = decode_schema_operation(operation, &schema).context(
                DecodeSchemaOperationSnafu {
                    dataset_id: dataset_update.dataset_id.clone(),
                },
            )?;
            assert_eq!(
                operation.change_id, message.update_id,
                "decoded inbound operation for dataset '{}' carried change id {}, expected {}",
                dataset_update.dataset_id, operation.change_id, message.update_id,
            );
            let row_change = apply_remote_operation(
                working_dataset,
                message.group_id,
                &dataset_update.dataset_id,
                operation,
            )?;
            row_changes.push(row_change);
        }
    }

    for (dataset_id, working_dataset) in working_datasets {
        local_group.datasets.insert(dataset_id, working_dataset);
    }
    local_group
        .version_vector
        .increment_at(message.update_id.node_index as usize);
    Ok(row_changes)
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

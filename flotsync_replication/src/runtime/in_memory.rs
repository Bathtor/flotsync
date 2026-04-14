use super::{errors::*, *};

/// In-memory hosted-group state for the first replication slice.
///
/// This is the temporary runtime backend until a persistent store-backed
/// implementation takes over the same responsibilities.
#[derive(Default)]
pub(super) struct RuntimeState {
    pub(super) groups: HashMap<GroupId, HostedGroup>,
}

impl RuntimeState {
    pub(super) fn membership_snapshot(&self) -> GroupMemberships {
        GroupMemberships::from_groups(
            self.groups
                .iter()
                .map(|(group_id, group)| (*group_id, group.members.clone())),
        )
    }
}

/// One fixed-membership replication group currently hosted by this runtime.
pub(super) struct HostedGroup {
    pub(super) members: GroupMembers,
    pub(super) local_member_index: MemberIndex,
    pub(super) version_vector: VersionVector,
    pub(super) datasets: HashMap<DatasetId, HostedDataset>,
    pub(super) pending_updates: BTreeMap<UpdateId, PendingInboundUpdate>,
}

impl HostedGroup {
    pub(super) fn new(
        local_member: &MemberIdentity,
        members: GroupMembers,
    ) -> Result<Self, GroupInstallError> {
        let local_member_index =
            members
                .member_index(local_member)
                .context(LocalMemberIndexMissingSnafu {
                    local_member: local_member.clone(),
                })?;
        let member_count = NonZeroUsize::new(members.len())
            .expect("group installation must only happen for non-empty member sets");
        Ok(Self {
            members,
            local_member_index,
            version_vector: initial_version_vector(member_count),
            datasets: HashMap::new(),
            pending_updates: BTreeMap::new(),
        })
    }

    pub(super) fn applied_version(&self, member_index: MemberIndex) -> u64 {
        version_vector_entry(&self.version_vector, member_index)
    }

    pub(super) fn expected_next_version(&self, member_index: MemberIndex) -> u64 {
        self.applied_version(member_index)
            .checked_add(1)
            .expect("member version counter must not overflow")
    }

    pub(super) fn has_applied(&self, update_id: UpdateId) -> bool {
        self.applied_version(MemberIndex::new(update_id.node_index)) >= update_id.version
    }

    pub(super) fn buffer_update(
        &mut self,
        pending_update: PendingInboundUpdate,
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

    pub(super) fn take_next_actionable_pending_update(&mut self) -> Option<PendingUpdateAction> {
        let mut duplicate_key = None;
        let mut ready_key = None;
        for (update_id, pending_update) in &self.pending_updates {
            if self.has_applied(*update_id) {
                duplicate_key = Some(*update_id);
                break;
            }
            let producer_index = MemberIndex::new(update_id.node_index);
            if version_vector_covers(&self.version_vector, &pending_update.message.read_versions)
                && self.expected_next_version(producer_index) == update_id.version
            {
                ready_key = Some(*update_id);
                break;
            }
        }

        if let Some(update_id) = duplicate_key {
            self.pending_updates.remove(&update_id);
            return Some(PendingUpdateAction::DropDuplicate);
        }
        ready_key
            .and_then(|update_id| self.pending_updates.remove(&update_id))
            .map(PendingUpdateAction::Apply)
    }
}

/// One hosted dataset together with its current replicated in-memory contents.
#[derive(Clone)]
pub(super) struct HostedDataset {
    pub(super) data: flotsync_messages::InMemoryData,
}

impl HostedDataset {
    pub(super) fn new(schema: Arc<Schema>) -> Self {
        Self {
            data: flotsync_messages::InMemoryData::with_owned_schema(schema.as_ref().clone()),
        }
    }

    fn clone_row(&self, row_key: crate::api::RowKey) -> Option<OwnedRow> {
        let row = self.data.get_row(&row_key.0)?;
        let mut fields = HashMap::with_capacity(self.data.num_fields());
        for field_name in self.data.field_names() {
            let value = row
                .get_field(field_name)
                .expect("dataset field iteration must resolve against the same row");
            fields.insert(field_name.to_owned(), value.clone());
        }
        Some(OwnedRow { fields })
    }
}

/// Owned row snapshot used when surfacing delivered changes to listeners.
///
/// The listener interface is object-safe and outlives internal staging
/// buffers, so inbound apply clones the current row image into this adapter.
struct OwnedRow {
    fields: HashMap<String, flotsync_data_types::InMemoryFieldValue<UpdateId>>,
}

impl RowRead for OwnedRow {
    fn get_field(
        &self,
        field_name: &str,
    ) -> Option<&flotsync_data_types::InMemoryFieldValue<UpdateId>> {
        self.fields.get(field_name)
    }
}

/// Fully prepared outbound publish batch ready for group fan-out.
pub(super) struct PreparedPublish {
    pub(super) group_id: GroupId,
    pub(super) update_id: UpdateId,
    pub(super) payload: bytes::Bytes,
}

/// One inbound update batch buffered until its causal dependencies are met.
pub(super) struct PendingInboundUpdate {
    pub(super) message: UpdateBatchMessage,
    pub(super) loaded_schemas: HashMap<DatasetId, Arc<Schema>>,
}

pub(super) enum PendingUpdateAction {
    DropDuplicate,
    Apply(PendingInboundUpdate),
}

pub(super) async fn load_publish_schemas(
    store: Arc<dyn ReplicationStore>,
    missing_dataset_ids: Vec<DatasetId>,
) -> Result<HashMap<DatasetId, Arc<Schema>>, PublishChangesError> {
    let mut loaded_schemas = HashMap::with_capacity(missing_dataset_ids.len());
    for dataset_id in missing_dataset_ids {
        let schema =
            store
                .load_dataset_schema(&dataset_id)
                .await
                .context(LoadDatasetSchemaSnafu {
                    dataset_id: dataset_id.clone(),
                })?;
        let schema = schema.context(MissingDatasetSchemaSnafu {
            dataset_id: dataset_id.clone(),
        })?;
        loaded_schemas.insert(dataset_id, schema);
    }
    Ok(loaded_schemas)
}

pub(super) async fn load_inbound_schemas(
    store: Arc<dyn ReplicationStore>,
    missing_dataset_ids: Vec<DatasetId>,
) -> Result<HashMap<DatasetId, Arc<Schema>>, InboundDeliveryError> {
    let mut loaded_schemas = HashMap::with_capacity(missing_dataset_ids.len());
    for dataset_id in missing_dataset_ids {
        let schema = store.load_dataset_schema(&dataset_id).await.context(
            InboundLoadDatasetSchemaSnafu {
                dataset_id: dataset_id.clone(),
            },
        )?;
        let schema = schema.context(InboundMissingDatasetSchemaSnafu {
            dataset_id: dataset_id.clone(),
        })?;
        loaded_schemas.insert(dataset_id, schema);
    }
    Ok(loaded_schemas)
}

fn mutation_row_id(mutation: &RowMutation) -> &crate::api::RowId {
    match mutation {
        RowMutation::Upsert { row_id, .. } | RowMutation::Delete { row_id } => row_id,
    }
}

pub(super) fn publish_scope(
    changes: &[RowMutation],
) -> Result<(GroupId, Vec<DatasetId>), PublishChangesError> {
    let Some(first_change) = changes.first() else {
        return EmptyChangesSnafu.fail();
    };
    let group_id = mutation_row_id(first_change).group_id;
    let mut dataset_ids = Vec::new();
    for change in changes {
        let row_id = mutation_row_id(change);
        ensure!(
            row_id.group_id == group_id,
            MixedGroupsSnafu {
                first_group_id: group_id,
                other_group_id: row_id.group_id,
            }
        );
        if !dataset_ids.contains(&row_id.dataset_id) {
            dataset_ids.push(row_id.dataset_id.clone());
        }
    }
    Ok((group_id, dataset_ids))
}

pub(super) fn staged_dataset_for_publish<'a>(
    staged_datasets: &'a mut HashMap<DatasetId, HostedDataset>,
    hosted_group: &HostedGroup,
    loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
    dataset_id: &DatasetId,
) -> Result<&'a mut HostedDataset, PublishChangesError> {
    if !staged_datasets.contains_key(dataset_id) {
        let staged_dataset = hosted_group
            .datasets
            .get(dataset_id)
            .cloned()
            .or_else(|| {
                loaded_schemas
                    .get(dataset_id)
                    .cloned()
                    .map(HostedDataset::new)
            })
            .context(MissingDatasetSchemaSnafu {
                dataset_id: dataset_id.clone(),
            })?;
        staged_datasets.insert(dataset_id.clone(), staged_dataset);
    }
    Ok(staged_datasets
        .get_mut(dataset_id)
        .expect("staged publish dataset must exist after insertion"))
}

fn staged_dataset_for_inbound<'a>(
    staged_datasets: &'a mut HashMap<DatasetId, HostedDataset>,
    hosted_group: &HostedGroup,
    loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
    dataset_id: &DatasetId,
) -> Result<&'a mut HostedDataset, InboundDeliveryError> {
    if !staged_datasets.contains_key(dataset_id) {
        let staged_dataset = hosted_group
            .datasets
            .get(dataset_id)
            .cloned()
            .or_else(|| {
                loaded_schemas
                    .get(dataset_id)
                    .cloned()
                    .map(HostedDataset::new)
            })
            .context(InboundMissingDatasetSchemaSnafu {
                dataset_id: dataset_id.clone(),
            })?;
        staged_datasets.insert(dataset_id.clone(), staged_dataset);
    }
    Ok(staged_datasets
        .get_mut(dataset_id)
        .expect("staged inbound dataset must exist after insertion"))
}

pub(super) fn initial_version_vector(num_members: NonZeroUsize) -> VersionVector {
    VersionVector::Synced {
        num_members,
        version: 0,
    }
}

fn version_vector_num_members(version_vector: &VersionVector) -> usize {
    version_vector.num_members().get()
}

fn version_vector_entry(version_vector: &VersionVector, member_index: MemberIndex) -> u64 {
    version_vector
        .iter()
        .nth(member_index.as_u32() as usize)
        .expect("member index must be within the group's version vector")
}

pub(super) fn version_vector_covers(local: &VersionVector, required: &VersionVector) -> bool {
    if version_vector_num_members(local) != version_vector_num_members(required) {
        return false;
    }
    local
        .iter()
        .zip(required.iter())
        .all(|(local_version, required_version)| local_version >= required_version)
}

pub(super) fn apply_one_update_batch(
    hosted_group: &mut HostedGroup,
    message: UpdateBatchMessage,
    loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
) -> Result<Vec<RowChange>, InboundDeliveryError> {
    let mut staged_datasets = HashMap::<DatasetId, HostedDataset>::new();
    let mut row_changes = Vec::new();
    for dataset_update in message.dataset_updates {
        let staged_dataset = staged_dataset_for_inbound(
            &mut staged_datasets,
            hosted_group,
            loaded_schemas,
            &dataset_update.dataset_id,
        )?;
        for operation in dataset_update.operations {
            let schema = staged_dataset.data.schema().clone();
            let operation = decode_schema_operation(operation, &schema).context(
                DecodeSchemaOperationSnafu {
                    dataset_id: dataset_update.dataset_id.clone(),
                },
            )?;
            ensure!(
                operation.change_id == message.update_id,
                MismatchedOperationUpdateIdSnafu {
                    dataset_id: dataset_update.dataset_id.clone(),
                    expected: message.update_id,
                    actual: operation.change_id,
                }
            );
            let row_change = apply_remote_operation(
                staged_dataset,
                message.group_id,
                &dataset_update.dataset_id,
                operation,
            )?;
            row_changes.push(row_change);
        }
    }

    for (dataset_id, staged_dataset) in staged_datasets {
        hosted_group.datasets.insert(dataset_id, staged_dataset);
    }
    hosted_group
        .version_vector
        .increment_at(message.update_id.node_index as usize);
    Ok(row_changes)
}

fn build_initial_values<'schema>(
    schema: &'schema Schema,
    row_id: &crate::api::RowId,
    row: &MutableRow,
) -> Result<Vec<InitialFieldValue<'schema>>, PublishChangesError> {
    let mut initial_values = Vec::with_capacity(row.fields.len());
    for (field_name, value) in &row.fields {
        let field = schema
            .columns
            .get(field_name.as_str())
            .context(UnknownSchemaFieldSnafu {
                row_id: Box::new(row_id.clone()),
                dataset_id: row_id.dataset_id.clone(),
                field_name: field_name.clone(),
            })?;
        let initial_value = field
            .initial(value.clone())
            .context(InvalidFieldValueSnafu {
                row_id: Box::new(row_id.clone()),
                dataset_id: row_id.dataset_id.clone(),
            })?;
        initial_values.push(initial_value);
    }
    Ok(initial_values)
}

fn build_pending_updates<'schema>(
    schema: &'schema Schema,
    row_id: &crate::api::RowId,
    row: &MutableRow,
) -> Result<Vec<PendingFieldUpdate<'schema>>, PublishChangesError> {
    let mut pending_updates = Vec::with_capacity(row.fields.len());
    for (field_name, value) in &row.fields {
        let field = schema
            .columns
            .get(field_name.as_str())
            .context(UnknownSchemaFieldSnafu {
                row_id: Box::new(row_id.clone()),
                dataset_id: row_id.dataset_id.clone(),
                field_name: field_name.clone(),
            })?;
        let pending_update = field.set(value.clone()).context(InvalidFieldValueSnafu {
            row_id: Box::new(row_id.clone()),
            dataset_id: row_id.dataset_id.clone(),
        })?;
        pending_updates.push(pending_update);
    }
    Ok(pending_updates)
}

pub(super) fn apply_local_upsert(
    dataset: &mut HostedDataset,
    row_id: &crate::api::RowId,
    row: &MutableRow,
    update_id: UpdateId,
) -> Result<Option<flotsync_messages::datamodel::SchemaOperation>, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    let operation = if dataset.data.get_row(&row_id.row_key.0).is_some() {
        let pending_updates = build_pending_updates(&schema, row_id, row)?;
        match dataset
            .data
            .modify_row(update_id, row_id.row_key.0, pending_updates)
            .context(ApplyLocalMutationSnafu {
                row_id: Box::new(row_id.clone()),
            })? {
            OperationOutcome::Applied(operation) => Some(operation),
            OperationOutcome::NoChanges => None,
        }
    } else {
        let initial_values = build_initial_values(&schema, row_id, row)?;
        let operation = dataset
            .data
            .insert_row(update_id, row_id.row_key.0, initial_values)
            .context(ApplyLocalMutationSnafu {
                row_id: Box::new(row_id.clone()),
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

pub(super) fn apply_local_delete(
    dataset: &mut HostedDataset,
    row_id: &crate::api::RowId,
    update_id: UpdateId,
) -> Result<flotsync_messages::datamodel::SchemaOperation, PublishChangesError> {
    let schema = dataset.data.schema().clone();
    let operation = dataset
        .data
        .delete_row(update_id, row_id.row_key.0)
        .context(ApplyLocalMutationSnafu {
            row_id: Box::new(row_id.clone()),
        })?;
    let encoded_operation =
        encode_schema_operation(&operation, &schema).context(EncodeOperationSnafu {
            dataset_id: row_id.dataset_id.clone(),
        })?;
    Ok(encoded_operation)
}

fn apply_remote_operation(
    dataset: &mut HostedDataset,
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
            let row = dataset
                .clone_row(api_row_id.row_key)
                .context(MissingAppliedRowSnafu {
                    row_id: api_row_id.clone(),
                })?;
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

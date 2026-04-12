mod host;
mod messages;

use crate::{
    GroupMembers,
    GroupMemberships,
    api::{
        ApiError,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DatasetId,
        GroupId,
        GroupMigration,
        ListenerError,
        LoadError,
        MemberIdentity,
        MemberIndex,
        MutableRow,
        PublishReceipt,
        ReplicationApi,
        ReplicationConfig,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationStore,
        RowChange,
        RowMutation,
        RowRead,
        StoreError,
        providers::VecRowProvider,
    },
    delivery::{
        contracts::{GroupBroadcastPortIndication, ReliableDeliveryPortIndication},
        group_broadcast::{
            GroupBroadcastDeliver,
            GroupBroadcastSubmit,
            GroupMessageEnvelope,
            GroupMessageHeader,
        },
        reliable_delivery::{
            ReliableDeliveryDeliver,
            ReliableDeliverySubmit,
            ReliableMessageEnvelope,
            ReliableMessageHeader,
        },
        shared::{
            DeliveryClass,
            DetachedSignature,
            EncryptedPayload,
            MessageId,
            SignatureScheme,
            SignedEnvelopeFooter,
        },
    },
};
use flotsync_core::{member::Identifier, versions::UpdateId};
use flotsync_data_types::{
    InitialFieldValue,
    OperationError,
    OperationOutcome,
    PendingFieldUpdate,
    RowOperations,
    Schema,
    TableOperations,
    schema::FieldValueBuildError,
};
use flotsync_messages::codecs::datamodel::{
    OperationCodecError,
    decode_schema_operation,
    encode_schema_operation,
};
use flotsync_utils::BoxFuture;
use kompact::prelude::{Completable, PromiseErr, block_on};
use messages::{
    BootstrapGroupMessage,
    DatasetUpdateMessage,
    RuntimeMessage,
    RuntimeMessageError,
    UpdateBatchMessage,
};
use snafu::prelude::*;
use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};
use uuid::Uuid;

use host::DeliveryRuntimeHost;

/// Entry point for loading replication state for one application id.
///
/// The current runtime implementation returns the first concrete internal slice.
pub async fn load_replication_runtime(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
) -> Result<Arc<dyn ReplicationApi>, LoadError> {
    let runtime = load_replication_runtime_inner(application_id, store, listener, config).await?;
    Ok(runtime)
}

async fn load_replication_runtime_inner(
    application_id: Identifier,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    config: ReplicationConfig,
) -> Result<Arc<ReplicationRuntime>, LoadError> {
    let local_member = store
        .local_member_identity()
        .await
        .map_err(|source| LoadError::runtime(application_id.clone(), source))?;
    let host = Arc::new(
        DeliveryRuntimeHost::new(local_member.clone(), config.external_udp_bind_addr)
            .map_err(|source| LoadError::runtime(application_id.clone(), source))?,
    );
    let shared = Arc::new(RuntimeShared {
        local_member: local_member.clone(),
        store,
        listener,
        host,
        state: Mutex::new(RuntimeState::default()),
    });
    let delivery_loop = DeliveryLoop::spawn(shared.clone());
    Ok(Arc::new(ReplicationRuntime {
        _application_id: application_id,
        shared,
        _config: config,
        _delivery_loop: delivery_loop,
    }))
}

/// Concrete application-facing runtime returned by `load_replication_runtime`.
///
/// The current slice supports one real end-to-end path:
/// creating a fixed-membership group and installing that membership remotely
/// through reliable delivery bootstrap messages.
struct ReplicationRuntime {
    _application_id: Identifier,
    shared: Arc<RuntimeShared>,
    _config: ReplicationConfig,
    _delivery_loop: DeliveryLoop,
}

impl ReplicationRuntime {
    fn unavailable(operation: &'static str) -> ApiError {
        ApiError::external(io::Error::other(format!(
            "replication runtime host is ready, but {operation} is not implemented yet"
        )))
    }
}

struct RuntimeShared {
    local_member: MemberIdentity,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    host: Arc<DeliveryRuntimeHost>,
    state: Mutex<RuntimeState>,
}

/// Mutable runtime-owned state for the first end-to-end replication slice.
///
/// Delivery keeps the authoritative shared membership snapshot for transport
/// decisions, while this state owns the corresponding hosted-group metadata and
/// in-memory dataset contents needed by `publish_changes` and inbound apply.
#[derive(Default)]
struct RuntimeState {
    groups: HashMap<GroupId, HostedGroup>,
}

impl RuntimeState {
    fn membership_snapshot(&self) -> GroupMemberships {
        GroupMemberships::from_groups(
            self.groups
                .iter()
                .map(|(group_id, group)| (*group_id, group.members.clone())),
        )
    }
}

/// One fixed-membership replication group currently hosted by this runtime.
struct HostedGroup {
    members: GroupMembers,
    local_member_index: MemberIndex,
    next_local_version: u64,
    datasets: HashMap<DatasetId, HostedDataset>,
}

impl HostedGroup {
    fn new(
        local_member: &MemberIdentity,
        members: GroupMembers,
    ) -> Result<Self, GroupInstallError> {
        let local_member_index =
            members
                .member_index(local_member)
                .context(LocalMemberIndexMissingSnafu {
                    local_member: local_member.clone(),
                })?;
        Ok(Self {
            members,
            local_member_index,
            next_local_version: 0,
            datasets: HashMap::new(),
        })
    }

    fn candidate_update_id(&self) -> UpdateId {
        UpdateId {
            version: self.next_local_version,
            node_index: self.local_member_index.as_u32(),
        }
    }
}

/// One hosted dataset together with its current replicated in-memory contents.
#[derive(Clone)]
struct HostedDataset {
    data: flotsync_messages::InMemoryData,
}

impl HostedDataset {
    fn new(schema: Arc<Schema>) -> Self {
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
struct PreparedPublish {
    group_id: GroupId,
    update_id: UpdateId,
    payload: bytes::Bytes,
}

impl RuntimeShared {
    async fn load_missing_dataset_schemas_for_publish(
        &self,
        group_id: GroupId,
        dataset_ids: &[DatasetId],
    ) -> Result<HashMap<DatasetId, Arc<Schema>>, PublishChangesError> {
        let missing_dataset_ids = {
            let state = self
                .state
                .lock()
                .expect("runtime state mutex must not be poisoned");
            let hosted_group = state
                .groups
                .get(&group_id)
                .context(UnknownGroupSnafu { group_id })?;
            let mut missing = Vec::new();
            for dataset_id in dataset_ids {
                if hosted_group.datasets.contains_key(dataset_id) || missing.contains(dataset_id) {
                    continue;
                }
                missing.push(dataset_id.clone());
            }
            missing
        };

        let mut loaded_schemas = HashMap::with_capacity(missing_dataset_ids.len());
        for dataset_id in missing_dataset_ids {
            let schema = self.store.load_dataset_schema(&dataset_id).await.context(
                LoadDatasetSchemaSnafu {
                    dataset_id: dataset_id.clone(),
                },
            )?;
            let schema = schema.context(MissingDatasetSchemaSnafu {
                dataset_id: dataset_id.clone(),
            })?;
            loaded_schemas.insert(dataset_id, schema);
        }
        Ok(loaded_schemas)
    }

    fn load_missing_dataset_schemas_blocking(
        &self,
        group_id: GroupId,
        dataset_ids: &[DatasetId],
    ) -> Result<HashMap<DatasetId, Arc<Schema>>, InboundDeliveryError> {
        let missing_dataset_ids = {
            let state = self
                .state
                .lock()
                .expect("runtime state mutex must not be poisoned");
            let hosted_group = state
                .groups
                .get(&group_id)
                .context(UnknownHostedGroupSnafu { group_id })?;
            let mut missing = Vec::new();
            for dataset_id in dataset_ids {
                if hosted_group.datasets.contains_key(dataset_id) || missing.contains(dataset_id) {
                    continue;
                }
                missing.push(dataset_id.clone());
            }
            missing
        };

        let mut loaded_schemas = HashMap::with_capacity(missing_dataset_ids.len());
        for dataset_id in missing_dataset_ids {
            let schema = block_on(self.store.load_dataset_schema(&dataset_id)).context(
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

    fn create_group(&self, req: CreateGroupRequest) -> Result<GroupId, CreateGroupError> {
        if req.initial_state.is_some() {
            return InitialStateUnsupportedSnafu.fail();
        }

        let members =
            GroupMembers::from_ordered_members(req.members).context(InvalidMembersSnafu)?;
        if !members.contains(&self.local_member) {
            return LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
            .fail();
        }

        let group_id = GroupId(Uuid::new_v4());
        self.install_group(group_id, members.clone())
            .context(InstallGroupSnafu)?;

        let bootstrap = RuntimeMessage::BootstrapGroup(BootstrapGroupMessage {
            group_id,
            members: members.ordered_members(),
        });
        let payload = bootstrap.encode_to_bytes();
        for recipient in members
            .ordered_members()
            .into_iter()
            .filter(|member| member != &self.local_member)
        {
            self.host.submit_reliable_delivery(ReliableDeliverySubmit {
                envelope: ReliableMessageEnvelope {
                    header: ReliableMessageHeader {
                        sender: self.local_member.clone(),
                        recipient,
                        message_id: MessageId(Uuid::new_v4()),
                    },
                    payload: EncryptedPayload {
                        ciphertext: payload.clone(),
                    },
                    footer: placeholder_signed_footer(),
                },
            });
        }
        Ok(group_id)
    }

    fn prepare_local_publish(
        &self,
        changes: Vec<RowMutation>,
        loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
    ) -> Result<PreparedPublish, PublishChangesError> {
        let (group_id, _) = publish_scope(&changes)?;
        let mut state = self
            .state
            .lock()
            .expect("runtime state mutex must not be poisoned");
        let hosted_group = state
            .groups
            .get_mut(&group_id)
            .context(UnknownGroupSnafu { group_id })?;

        ensure!(
            hosted_group.next_local_version < u64::MAX,
            ExhaustedUpdateIdsSnafu { group_id }
        );
        let update_id = hosted_group.candidate_update_id();

        let mut dataset_order = Vec::new();
        let mut staged_datasets = HashMap::<DatasetId, HostedDataset>::new();
        let mut encoded_operations =
            HashMap::<DatasetId, Vec<flotsync_messages::datamodel::SchemaOperation>>::new();
        for mutation in changes {
            match mutation {
                RowMutation::Upsert { row_id, row } => {
                    if !dataset_order.contains(&row_id.dataset_id) {
                        dataset_order.push(row_id.dataset_id.clone());
                    }
                    let staged_dataset = staged_dataset_for_publish(
                        &mut staged_datasets,
                        hosted_group,
                        loaded_schemas,
                        &row_id.dataset_id,
                    )?;
                    if let Some(encoded_operation) =
                        apply_local_upsert(staged_dataset, &row_id, &row, update_id)?
                    {
                        encoded_operations
                            .entry(row_id.dataset_id.clone())
                            .or_default()
                            .push(encoded_operation);
                    }
                }
                RowMutation::Delete { row_id } => {
                    if !dataset_order.contains(&row_id.dataset_id) {
                        dataset_order.push(row_id.dataset_id.clone());
                    }
                    let staged_dataset = staged_dataset_for_publish(
                        &mut staged_datasets,
                        hosted_group,
                        loaded_schemas,
                        &row_id.dataset_id,
                    )?;
                    let encoded_operation = apply_local_delete(staged_dataset, &row_id, update_id)?;
                    encoded_operations
                        .entry(row_id.dataset_id.clone())
                        .or_default()
                        .push(encoded_operation);
                }
            }
        }

        ensure!(
            !encoded_operations.is_empty(),
            NoEffectiveChangesSnafu { group_id }
        );

        for (dataset_id, staged_dataset) in staged_datasets {
            hosted_group.datasets.insert(dataset_id, staged_dataset);
        }
        hosted_group.next_local_version += 1;

        let dataset_updates = dataset_order
            .into_iter()
            .filter_map(|dataset_id| {
                encoded_operations
                    .remove(&dataset_id)
                    .map(|operations| DatasetUpdateMessage {
                        dataset_id,
                        operations,
                    })
            })
            .collect();
        let payload = RuntimeMessage::UpdateBatch(UpdateBatchMessage {
            group_id,
            update_id,
            dataset_updates,
        })
        .encode_to_bytes();
        Ok(PreparedPublish {
            group_id,
            update_id,
            payload,
        })
    }

    fn handle_reliable_delivery(
        &self,
        deliver: ReliableDeliveryDeliver,
    ) -> Result<(), InboundDeliveryError> {
        let message = RuntimeMessage::decode_from_slice(&deliver.envelope.payload.ciphertext)
            .context(DecodeMessageSnafu)?;
        match message {
            RuntimeMessage::BootstrapGroup(message) => {
                let group_id = message.group_id;
                let members = GroupMembers::from_ordered_members(message.members)
                    .context(InvalidBootstrapMembersSnafu)?;
                if !members.contains(&self.local_member) {
                    return BootstrapMissingLocalMemberSnafu {
                        group_id,
                        local_member: self.local_member.clone(),
                    }
                    .fail();
                }
                self.install_group(group_id, members)
                    .context(InstallBootstrapGroupSnafu { group_id })?;
                deliver.processed.complete().map_err(|source| {
                    InboundDeliveryError::CompleteProcessedPromise { group_id, source }
                })?;
                Ok(())
            }
            RuntimeMessage::UpdateBatch(_) => UnexpectedReliableMessageSnafu.fail(),
        }
    }

    fn handle_group_delivery(
        &self,
        deliver: GroupBroadcastDeliver,
    ) -> Result<(), InboundDeliveryError> {
        let sender = deliver.envelope.header.sender.clone();
        let message = RuntimeMessage::decode_from_slice(&deliver.envelope.payload.ciphertext)
            .context(DecodeMessageSnafu)?;
        match message {
            RuntimeMessage::BootstrapGroup(_) => UnexpectedGroupMessageSnafu.fail(),
            RuntimeMessage::UpdateBatch(message) => self.apply_update_batch(sender, message),
        }
    }

    fn apply_update_batch(
        &self,
        sender: MemberIdentity,
        message: UpdateBatchMessage,
    ) -> Result<(), InboundDeliveryError> {
        let dataset_ids: Vec<_> = message
            .dataset_updates
            .iter()
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect();
        let loaded_schemas =
            self.load_missing_dataset_schemas_blocking(message.group_id, &dataset_ids)?;

        let row_changes = {
            let mut state = self
                .state
                .lock()
                .expect("runtime state mutex must not be poisoned");
            let hosted_group =
                state
                    .groups
                    .get_mut(&message.group_id)
                    .context(UnknownHostedGroupSnafu {
                        group_id: message.group_id,
                    })?;
            let expected_sender_index = hosted_group.members.member_index(&sender).context(
                UpdateSenderNotInGroupSnafu {
                    group_id: message.group_id,
                    sender: sender.clone(),
                },
            )?;
            ensure!(
                expected_sender_index.as_u32() == message.update_id.node_index,
                UpdateIdSenderMismatchSnafu {
                    group_id: message.group_id,
                    sender: sender.clone(),
                    expected_index: expected_sender_index,
                    actual_index: MemberIndex::new(message.update_id.node_index),
                }
            );

            let mut staged_datasets = HashMap::<DatasetId, HostedDataset>::new();
            let mut row_changes = Vec::new();
            for dataset_update in message.dataset_updates {
                let staged_dataset = staged_dataset_for_inbound(
                    &mut staged_datasets,
                    hosted_group,
                    &loaded_schemas,
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
            row_changes
        };

        if row_changes.is_empty() {
            return Ok(());
        }
        block_on(self.listener.on_event(ReplicationEvent::DataChanged {
            rows: Box::new(VecRowProvider::new(row_changes)),
        }))
        .context(NotifyListenerSnafu)?;
        Ok(())
    }

    fn install_group(
        &self,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Result<(), GroupInstallError> {
        let mut state = self
            .state
            .lock()
            .expect("runtime state mutex must not be poisoned");
        if let Some(existing_group) = state.groups.get(&group_id) {
            if existing_group.members == members {
                return Ok(());
            }
            return ConflictingExistingGroupSnafu { group_id }.fail();
        }

        let hosted_group = HostedGroup::new(&self.local_member, members)?;
        state.groups.insert(group_id, hosted_group);
        self.host
            .replace_group_memberships(state.membership_snapshot());
        Ok(())
    }
}

fn mutation_row_id(mutation: &RowMutation) -> &crate::api::RowId {
    match mutation {
        RowMutation::Upsert { row_id, .. } | RowMutation::Delete { row_id } => row_id,
    }
}

fn publish_scope(
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

fn staged_dataset_for_publish<'a>(
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

fn apply_local_upsert(
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

fn apply_local_delete(
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

struct DeliveryLoop {
    stop: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl DeliveryLoop {
    fn spawn(shared: Arc<RuntimeShared>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let handle = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                let mut made_progress = false;
                while let Some(indication) = shared.host.pop_reliable_indication() {
                    made_progress = true;
                    match indication {
                        ReliableDeliveryPortIndication::Deliver(deliver) => {
                            let _ = shared.handle_reliable_delivery(deliver);
                        }
                    }
                }
                while let Some(indication) = shared.host.pop_group_indication() {
                    made_progress = true;
                    match indication {
                        GroupBroadcastPortIndication::Deliver(deliver) => {
                            let _ = shared.handle_group_delivery(deliver);
                        }
                    }
                }
                if !made_progress {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        });
        Self {
            stop,
            handle: Mutex::new(Some(handle)),
        }
    }
}

impl Drop for DeliveryLoop {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let Some(handle) = self
            .handle
            .lock()
            .expect("delivery loop handle mutex must not be poisoned")
            .take()
        else {
            return;
        };
        let _ = handle.join();
    }
}

#[derive(Debug, Snafu)]
enum CreateGroupError {
    #[snafu(display("Initial group state is not supported in the first replication slice."))]
    InitialStateUnsupported,
    #[snafu(display("Group members must include the local member {local_member}."))]
    LocalMemberMissing { local_member: MemberIdentity },
    #[snafu(display("Group member list is invalid."))]
    InvalidMembers { source: crate::GroupMembersError },
    #[snafu(display("Failed to install the created group locally."))]
    InstallGroup { source: GroupInstallError },
}

#[derive(Debug, Snafu)]
enum GroupInstallError {
    #[snafu(display(
        "Group {:?} already exists with a different canonical member order.",
        group_id
    ))]
    ConflictingExistingGroup { group_id: GroupId },
    #[snafu(display(
        "Group members did not expose a canonical index for the local member {local_member}."
    ))]
    LocalMemberIndexMissing { local_member: MemberIdentity },
}

#[derive(Debug, Snafu)]
enum PublishChangesError {
    #[snafu(display("publish_changes requires at least one row mutation."))]
    EmptyChanges,
    #[snafu(display(
        "publish_changes only supports one group per call in the first replication slice; saw both {:?} and {:?}.",
        first_group_id,
        other_group_id
    ))]
    MixedGroups {
        first_group_id: GroupId,
        other_group_id: GroupId,
    },
    #[snafu(display("Group {:?} is not hosted by this runtime.", group_id))]
    UnknownGroup { group_id: GroupId },
    #[snafu(display(
        "Failed to load schema for dataset '{dataset_id}' from the replication store."
    ))]
    LoadDatasetSchema {
        dataset_id: DatasetId,
        source: StoreError,
    },
    #[snafu(display("No schema was available for dataset '{dataset_id}'."))]
    MissingDatasetSchema { dataset_id: DatasetId },
    #[snafu(display(
        "Row {:?} referenced unknown schema field '{field_name}' in dataset '{dataset_id}'.",
        row_id
    ))]
    UnknownSchemaField {
        row_id: Box<crate::api::RowId>,
        dataset_id: DatasetId,
        field_name: String,
    },
    #[snafu(display(
        "Row {:?} carried a value incompatible with dataset '{dataset_id}'.",
        row_id
    ))]
    InvalidFieldValue {
        row_id: Box<crate::api::RowId>,
        dataset_id: DatasetId,
        source: FieldValueBuildError,
    },
    #[snafu(display("Applying local mutation for row {:?} failed.", row_id))]
    ApplyLocalMutation {
        row_id: Box<crate::api::RowId>,
        source: OperationError,
    },
    #[snafu(display("Encoding dataset '{dataset_id}' update for transport failed."))]
    EncodeOperation {
        dataset_id: DatasetId,
        source: OperationCodecError,
    },
    #[snafu(display(
        "publish_changes produced no effective schema operations for group {:?}.",
        group_id
    ))]
    NoEffectiveChanges { group_id: GroupId },
    #[snafu(display("Group {:?} exhausted its local update id range.", group_id))]
    ExhaustedUpdateIds { group_id: GroupId },
}

#[derive(Debug, Snafu)]
enum InboundDeliveryError {
    #[snafu(display("Failed to decode inbound runtime message."))]
    DecodeMessage { source: RuntimeMessageError },
    #[snafu(display("Reliable delivery unexpectedly carried a group-broadcast update message."))]
    UnexpectedReliableMessage,
    #[snafu(display("Group broadcast unexpectedly carried a reliable bootstrap message."))]
    UnexpectedGroupMessage,
    #[snafu(display("Inbound bootstrap message carried an invalid group member set."))]
    InvalidBootstrapMembers { source: crate::GroupMembersError },
    #[snafu(display(
        "Inbound bootstrap for group {:?} did not include the local member {local_member}.",
        group_id
    ))]
    BootstrapMissingLocalMember {
        group_id: GroupId,
        local_member: MemberIdentity,
    },
    #[snafu(display("Failed to install inbound bootstrap group {:?} locally.", group_id))]
    InstallBootstrapGroup {
        group_id: GroupId,
        source: GroupInstallError,
    },
    #[snafu(display("Failed to complete the processed promise for group {:?}.", group_id))]
    CompleteProcessedPromise {
        group_id: GroupId,
        source: PromiseErr,
    },
    #[snafu(display("Inbound update targeted unknown hosted group {:?}.", group_id))]
    UnknownHostedGroup { group_id: GroupId },
    #[snafu(display(
        "Failed to load schema for inbound dataset '{dataset_id}' from the replication store."
    ))]
    InboundLoadDatasetSchema {
        dataset_id: DatasetId,
        source: StoreError,
    },
    #[snafu(display("No schema was available for inbound dataset '{dataset_id}'."))]
    InboundMissingDatasetSchema { dataset_id: DatasetId },
    #[snafu(display(
        "Inbound update for group {:?} came from sender {sender}, which is not a group member.",
        group_id
    ))]
    UpdateSenderNotInGroup {
        group_id: GroupId,
        sender: MemberIdentity,
    },
    #[snafu(display(
        "Inbound update for group {:?} claimed sender index {actual_index:?}, but sender {sender} is canonically at {expected_index:?}.",
        group_id
    ))]
    UpdateIdSenderMismatch {
        group_id: GroupId,
        sender: MemberIdentity,
        expected_index: MemberIndex,
        actual_index: MemberIndex,
    },
    #[snafu(display("Failed to decode inbound schema operation for dataset '{dataset_id}'."))]
    DecodeSchemaOperation {
        dataset_id: DatasetId,
        source: OperationCodecError,
    },
    #[snafu(display(
        "Inbound operation for dataset '{dataset_id}' had change id {actual}, expected {expected}.",
    ))]
    MismatchedOperationUpdateId {
        dataset_id: DatasetId,
        expected: UpdateId,
        actual: UpdateId,
    },
    #[snafu(display("Applying inbound mutation for row {:?} failed.", row_id))]
    ApplyInboundMutation {
        row_id: crate::api::RowId,
        source: OperationError,
    },
    #[snafu(display(
        "Inbound mutation for row {:?} applied, but the resulting row could not be read back.",
        row_id
    ))]
    MissingAppliedRow { row_id: crate::api::RowId },
    #[snafu(display("Listener rejected one inbound data-change event."))]
    NotifyListener { source: ListenerError },
}

fn placeholder_signed_footer() -> SignedEnvelopeFooter {
    SignedEnvelopeFooter {
        signature: DetachedSignature {
            scheme: SignatureScheme::Ed25519,
            bytes: bytes::Bytes::from_static(b"runtime-placeholder-signature"),
        },
    }
}

impl ReplicationApi for ReplicationRuntime {
    fn publish_changes(
        &self,
        changes: Vec<RowMutation>,
    ) -> BoxFuture<'_, Result<PublishReceipt, ApiError>> {
        let shared = self.shared.clone();
        Box::pin(async move {
            let (group_id, dataset_ids) = publish_scope(&changes).map_err(ApiError::external)?;
            let loaded_schemas = shared
                .load_missing_dataset_schemas_for_publish(group_id, &dataset_ids)
                .await
                .map_err(ApiError::external)?;
            let prepared_publish = shared
                .prepare_local_publish(changes, &loaded_schemas)
                .map_err(ApiError::external)?;

            shared.host.submit_group_broadcast(GroupBroadcastSubmit {
                delivery_class: DeliveryClass::BestEffort,
                envelope: GroupMessageEnvelope {
                    header: GroupMessageHeader {
                        group_id: prepared_publish.group_id,
                        sender: shared.local_member.clone(),
                        message_id: MessageId(Uuid::new_v4()),
                    },
                    payload: EncryptedPayload {
                        ciphertext: prepared_publish.payload,
                    },
                    footer: placeholder_signed_footer(),
                },
                suppress_self_delivery: true,
            });

            Ok(PublishReceipt {
                update_id: prepared_publish.update_id,
            })
        })
    }

    fn create_group(
        &self,
        req: CreateGroupRequest,
    ) -> BoxFuture<'_, Result<crate::api::GroupId, ApiError>> {
        let shared = self.shared.clone();
        Box::pin(async move { shared.create_group(req).map_err(ApiError::external) })
    }

    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<GroupMigration, ApiError>> {
        let _ = req;
        Box::pin(async move { Err(Self::unavailable("change_group_membership")) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GroupMembers,
        GroupMemberships,
        api::{DatasetId, ListenerError, MemberIndex},
        delivery::{
            group_broadcast::{
                GroupBroadcastDeliver,
                GroupBroadcastSubmit,
                GroupMessageEnvelope,
                GroupMessageHeader,
            },
            shared::{
                DeliveryClass,
                DetachedSignature,
                EncryptedPayload,
                MessageId,
                SignatureScheme,
                SignedEnvelopeFooter,
            },
        },
    };
    use bytes::Bytes;
    use flotsync_data_types::{Field, Schema};
    use std::{
        collections::HashMap,
        future::Future,
        pin::pin,
        sync::{LazyLock, Mutex},
        task::{Context, Poll, Waker},
        thread,
        time::{Duration, Instant},
    };
    use uuid::Uuid;

    struct StoreStub {
        local_member: crate::api::MemberIdentity,
        schemas: HashMap<DatasetId, Arc<Schema>>,
    }

    impl StoreStub {
        fn new(local_member: crate::api::MemberIdentity) -> Self {
            Self {
                local_member,
                schemas: HashMap::new(),
            }
        }

        fn with_schema(mut self, dataset_id: DatasetId, schema: Arc<Schema>) -> Self {
            self.schemas.insert(dataset_id, schema);
            self
        }
    }

    impl crate::api::ReplicationStore for StoreStub {
        fn local_member_identity(
            &self,
        ) -> BoxFuture<'_, Result<crate::api::MemberIdentity, crate::api::StoreError>> {
            let local_member = self.local_member.clone();
            Box::pin(async move { Ok(local_member) })
        }

        fn load_dataset_schema(
            &self,
            dataset_id: &DatasetId,
        ) -> BoxFuture<
            '_,
            Result<Option<Arc<flotsync_data_types::schema::Schema>>, crate::api::StoreError>,
        > {
            let schema = self.schemas.get(dataset_id).cloned();
            Box::pin(async move { Ok(schema) })
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct CapturedDataChange {
        rows: Vec<CapturedRowChange>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum CapturedRowChange {
        Upsert {
            row_id: crate::api::RowId,
            title: String,
        },
        Delete {
            row_id: crate::api::RowId,
        },
    }

    impl CapturedRowChange {
        fn capture(change: RowChange) -> Result<Self, ListenerError> {
            match change {
                RowChange::Upsert { row_id, row } => {
                    let title = row
                        .get_field_value::<str>("title")
                        .map_err(ListenerError::external)?
                        .into_owned();
                    Ok(Self::Upsert { row_id, title })
                }
                RowChange::Delete { row_id } => Ok(Self::Delete { row_id }),
            }
        }
    }

    #[derive(Default)]
    struct ListenerStub {
        data_changes: Mutex<Vec<CapturedDataChange>>,
    }

    impl ListenerStub {
        fn wait_for_next_data_change(&self) -> CapturedDataChange {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(change) = self
                    .data_changes
                    .lock()
                    .expect("listener capture mutex must not be poisoned")
                    .pop()
                {
                    return change;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for listener data-change event"
                );
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    impl crate::api::ReplicationEventListener for ListenerStub {
        fn on_event(
            &self,
            event: crate::api::ReplicationEvent,
        ) -> BoxFuture<'_, Result<(), ListenerError>> {
            Box::pin(async move {
                match event {
                    crate::api::ReplicationEvent::DataChanged { mut rows } => {
                        let mut captured_rows = Vec::new();
                        loop {
                            let batch = rows.next_batch().await.map_err(ListenerError::external)?;
                            if batch.is_empty() {
                                break;
                            }
                            for change in batch {
                                captured_rows.push(CapturedRowChange::capture(change)?);
                            }
                        }
                        self.data_changes
                            .lock()
                            .expect("listener capture mutex must not be poisoned")
                            .push(CapturedDataChange {
                                rows: captured_rows,
                            });
                    }
                    crate::api::ReplicationEvent::GroupInvitation { .. } => {}
                }
                Ok(())
            })
        }
    }

    static PLACEHOLDER_FOOTER: LazyLock<SignedEnvelopeFooter> =
        LazyLock::new(|| SignedEnvelopeFooter {
            signature: DetachedSignature {
                scheme: SignatureScheme::Ed25519,
                bytes: Bytes::from_static(b"runtime-test-signature"),
            },
        });

    fn member<const N: usize>(segments: [&str; N]) -> crate::api::MemberIdentity {
        Identifier::from_array(segments)
    }

    fn wait_for_group_delivery(host: &DeliveryRuntimeHost) -> GroupBroadcastDeliver {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(indication) = host.pop_group_indication() {
                return match indication {
                    crate::delivery::contracts::GroupBroadcastPortIndication::Deliver(deliver) => {
                        deliver
                    }
                };
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for runtime-host group indication"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn poll_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("test future unexpectedly returned Pending"),
        }
    }

    fn load_runtime(
        application_id: crate::api::MemberIdentity,
        local_member: crate::api::MemberIdentity,
    ) -> Arc<ReplicationRuntime> {
        let store = Arc::new(StoreStub::new(local_member));
        let listener = Arc::new(ListenerStub::default());
        load_runtime_with_parts(application_id, store, listener)
    }

    fn load_runtime_with_parts(
        application_id: crate::api::MemberIdentity,
        store: Arc<StoreStub>,
        listener: Arc<ListenerStub>,
    ) -> Arc<ReplicationRuntime> {
        poll_ready(load_replication_runtime_inner(
            application_id,
            store,
            listener,
            ReplicationConfig::default(),
        ))
        .expect("runtime should load")
    }

    fn wait_for_group_install(runtime: &Arc<ReplicationRuntime>, group_id: GroupId) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let state = runtime
                .shared
                .state
                .lock()
                .expect("runtime state mutex must not be poisoned");
            if state.groups.contains_key(&group_id) {
                break;
            }
            drop(state);
            assert!(
                Instant::now() < deadline,
                "timed out waiting for runtime to install group"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn delivery_runtime_host_updates_shared_group_memberships() {
        let local_member = member(["alice"]);
        let host = DeliveryRuntimeHost::new(
            local_member.clone(),
            ReplicationConfig::default().external_udp_bind_addr,
        )
        .expect("host should start");
        let group_id = crate::api::GroupId(Uuid::from_u128(1));
        let memberships = GroupMemberships::from_groups([(
            group_id,
            GroupMembers::from_ordered_members(vec![local_member]).expect("group should build"),
        )]);

        host.replace_group_memberships(memberships);

        assert!(host.membership_snapshot().contains_group(&group_id));
    }

    #[test]
    fn delivery_runtime_host_submits_group_broadcast_through_internal_client() {
        let local_member = member(["alice"]);
        let host = DeliveryRuntimeHost::new(
            local_member.clone(),
            ReplicationConfig::default().external_udp_bind_addr,
        )
        .expect("host should start");
        let group_id = crate::api::GroupId(Uuid::from_u128(2));
        host.replace_group_memberships(GroupMemberships::from_groups([(
            group_id,
            GroupMembers::from_ordered_members(vec![local_member.clone()])
                .expect("group should build"),
        )]));
        let submit = GroupBroadcastSubmit {
            delivery_class: DeliveryClass::BestEffort,
            envelope: GroupMessageEnvelope {
                header: GroupMessageHeader {
                    group_id,
                    sender: local_member,
                    message_id: MessageId(Uuid::from_u128(3)),
                },
                payload: EncryptedPayload {
                    ciphertext: Bytes::from_static(b"runtime payload"),
                },
                footer: PLACEHOLDER_FOOTER.clone(),
            },
            suppress_self_delivery: false,
        };

        host.submit_group_broadcast(submit.clone());

        assert_eq!(wait_for_group_delivery(&host).envelope, submit.envelope);
    }

    #[test]
    fn load_replication_runtime_returns_concrete_runtime() {
        let application_id = Identifier::from_array(["app"]);
        let store = Arc::new(StoreStub::new(member(["alice"])));
        let listener = Arc::new(ListenerStub::default());

        let runtime = poll_ready(load_replication_runtime(
            application_id,
            store,
            listener,
            ReplicationConfig::default(),
        ));

        assert!(runtime.is_ok());
    }

    #[test]
    fn replication_config_defaults_to_wildcard_external_udp_bind() {
        assert_eq!(
            ReplicationConfig::default().external_udp_bind_addr,
            std::net::SocketAddr::from((std::net::Ipv4Addr::UNSPECIFIED, 0))
        );
    }

    #[test]
    fn create_group_bootstrap_installs_remote_membership() {
        let alice_member = member(["alice"]);
        let bob_member = member(["bob"]);
        let alice = load_runtime(
            Identifier::from_array(["app", "alice"]),
            alice_member.clone(),
        );
        let bob = load_runtime(Identifier::from_array(["app", "bob"]), bob_member.clone());

        assert!(
            alice
                .shared
                .host
                .external_udp_bind_addr()
                .ip()
                .is_unspecified()
        );
        assert!(
            bob.shared
                .host
                .external_udp_bind_addr()
                .ip()
                .is_unspecified()
        );

        alice.shared.host.publish_direct_peer_route(
            bob_member.clone(),
            bob.shared.host.advertised_loopback_udp_addr(),
        );
        bob.shared.host.publish_direct_peer_route(
            alice_member.clone(),
            alice.shared.host.advertised_loopback_udp_addr(),
        );

        let group_id = poll_ready(alice.create_group(CreateGroupRequest {
            members: vec![alice_member.clone(), bob_member.clone()],
            initial_state: None,
        }))
        .expect("create_group should succeed");
        wait_for_group_install(&bob, group_id);

        let alice_snapshot = alice.shared.host.membership_snapshot();
        let bob_snapshot = bob.shared.host.membership_snapshot();
        let alice_members = alice_snapshot
            .members(&group_id)
            .expect("local runtime should host the created group");
        let bob_members = bob_snapshot
            .members(&group_id)
            .expect("remote runtime should install the bootstrap group");

        assert_eq!(
            alice_members.member_index(&alice_member),
            Some(MemberIndex::new(0))
        );
        assert_eq!(
            alice_members.member_index(&bob_member),
            Some(MemberIndex::new(1))
        );
        assert_eq!(
            bob_members.member_index(&alice_member),
            Some(MemberIndex::new(0))
        );
        assert_eq!(
            bob_members.member_index(&bob_member),
            Some(MemberIndex::new(1))
        );
    }

    #[test]
    fn publish_changes_delivers_remote_data_changed_event() {
        let alice_member = member(["alice"]);
        let bob_member = member(["bob"]);
        let dataset_id = DatasetId::try_new("docs").expect("dataset id should be valid");
        let schema = Arc::new(Schema::from_fields([Field::linear_string("title")]));
        let alice_listener = Arc::new(ListenerStub::default());
        let bob_listener = Arc::new(ListenerStub::default());
        let alice_store = Arc::new(
            StoreStub::new(alice_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
        );
        let bob_store = Arc::new(
            StoreStub::new(bob_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
        );
        let alice = load_runtime_with_parts(
            Identifier::from_array(["app", "alice"]),
            alice_store,
            alice_listener,
        );
        let bob = load_runtime_with_parts(
            Identifier::from_array(["app", "bob"]),
            bob_store,
            bob_listener.clone(),
        );

        alice.shared.host.publish_direct_peer_route(
            bob_member.clone(),
            bob.shared.host.advertised_loopback_udp_addr(),
        );
        bob.shared.host.publish_direct_peer_route(
            alice_member.clone(),
            alice.shared.host.advertised_loopback_udp_addr(),
        );

        let group_id = poll_ready(alice.create_group(CreateGroupRequest {
            members: vec![alice_member.clone(), bob_member.clone()],
            initial_state: None,
        }))
        .expect("create_group should succeed");
        wait_for_group_install(&bob, group_id);
        let row_id = crate::api::RowId {
            group_id,
            dataset_id: dataset_id.clone(),
            row_key: crate::api::RowKey(Uuid::from_u128(11)),
        };

        let receipt = poll_ready(alice.publish_changes(vec![RowMutation::Upsert {
            row_id: row_id.clone(),
            row: crate::row_values! {
                "title" => "hello from alice",
            },
        }]))
        .expect("publish_changes should succeed");

        assert_eq!(
            receipt.update_id,
            UpdateId {
                version: 0,
                node_index: 0,
            }
        );

        let delivered = bob_listener.wait_for_next_data_change();
        assert_eq!(
            delivered,
            CapturedDataChange {
                rows: vec![CapturedRowChange::Upsert {
                    row_id: row_id.clone(),
                    title: "hello from alice".to_owned(),
                }],
            }
        );

        let state = bob
            .shared
            .state
            .lock()
            .expect("runtime state mutex must not be poisoned");
        let hosted_group = state
            .groups
            .get(&group_id)
            .expect("remote runtime should host the published group");
        let hosted_dataset = hosted_group
            .datasets
            .get(&dataset_id)
            .expect("remote runtime should host the published dataset");
        let hosted_row = hosted_dataset
            .clone_row(row_id.row_key)
            .expect("remote runtime should store the replicated row");

        assert_eq!(
            (&hosted_row as &dyn RowRead)
                .get_field_value::<str>("title")
                .expect("title field should decode")
                .into_owned(),
            "hello from alice"
        );
    }
}

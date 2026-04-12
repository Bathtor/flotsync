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
use flotsync_core::{
    member::Identifier,
    versions::{UpdateId, VersionVector},
};
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
    collections::{BTreeMap, HashMap, btree_map::Entry},
    io,
    num::NonZeroUsize,
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
        terminal_fault: Mutex::new(None),
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
    terminal_fault: Mutex<Option<TerminalRuntimeFault>>,
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
    version_vector: VersionVector,
    datasets: HashMap<DatasetId, HostedDataset>,
    pending_updates: BTreeMap<UpdateId, PendingInboundUpdate>,
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

    fn applied_version(&self, member_index: MemberIndex) -> u64 {
        version_vector_entry(&self.version_vector, member_index)
    }

    fn expected_next_version(&self, member_index: MemberIndex) -> u64 {
        self.applied_version(member_index)
            .checked_add(1)
            .expect("member version counter must not overflow")
    }

    fn has_applied(&self, update_id: UpdateId) -> bool {
        self.applied_version(MemberIndex::new(update_id.node_index)) >= update_id.version
    }

    fn buffer_update(
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

    fn take_next_actionable_pending_update(&mut self) -> Option<PendingUpdateAction> {
        let mut duplicate_key = None;
        let mut ready_key = None;
        for (update_id, pending_update) in &self.pending_updates {
            if self.has_applied(*update_id) {
                duplicate_key = Some(*update_id);
                break;
            }
            let producer_index = MemberIndex::new(update_id.node_index);
            if version_vector_covers(&self.version_vector, &pending_update.message.read_vv)
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

/// One inbound update batch buffered until its causal dependencies are met.
struct PendingInboundUpdate {
    message: UpdateBatchMessage,
    loaded_schemas: HashMap<DatasetId, Arc<Schema>>,
}

enum PendingUpdateAction {
    DropDuplicate,
    Apply(PendingInboundUpdate),
}

#[derive(Clone, Debug)]
struct TerminalRuntimeFault {
    operation: &'static str,
    message: String,
}

impl std::fmt::Display for TerminalRuntimeFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.operation, self.message)
    }
}

impl RuntimeShared {
    fn ensure_running(&self) -> Result<(), ApiError> {
        let fault = self
            .terminal_fault
            .lock()
            .expect("runtime fault mutex must not be poisoned")
            .clone();
        if let Some(fault) = fault {
            return Err(ApiError::external(io::Error::other(format!(
                "replication runtime terminated after inbound delivery failure: {fault}"
            ))));
        }
        Ok(())
    }

    fn record_terminal_fault(&self, operation: &'static str, error: &InboundDeliveryError) {
        let fault = {
            let mut slot = self
                .terminal_fault
                .lock()
                .expect("runtime fault mutex must not be poisoned");
            if slot.is_some() {
                return;
            }
            let fault = TerminalRuntimeFault {
                operation,
                message: error.to_string(),
            };
            *slot = Some(fault.clone());
            fault
        };
        eprintln!("[flotsync_replication] terminal runtime failure: {fault}");
    }

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

        let read_vv = hosted_group.version_vector.clone();
        let next_local_version = hosted_group
            .applied_version(hosted_group.local_member_index)
            .checked_add(1)
            .context(ExhaustedUpdateIdsSnafu { group_id })?;
        let update_id = UpdateId {
            version: next_local_version,
            node_index: hosted_group.local_member_index.as_u32(),
        };

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
        hosted_group
            .version_vector
            .increment_at(hosted_group.local_member_index.as_u32() as usize);

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
            read_vv,
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

        let event_batches = {
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
            ensure!(
                message.update_id.version > 0,
                InvalidUpdateVersionSnafu {
                    group_id: message.group_id,
                    update_id: message.update_id,
                }
            );
            ensure!(
                version_vector_num_members(&message.read_vv) == hosted_group.members.len(),
                ReadVersionVectorSizeMismatchSnafu {
                    group_id: message.group_id,
                    expected_members: hosted_group.members.len(),
                    actual_members: version_vector_num_members(&message.read_vv),
                }
            );
            if hosted_group.has_applied(message.update_id) {
                return Ok(());
            }
            if !version_vector_covers(&hosted_group.version_vector, &message.read_vv)
                || hosted_group.expected_next_version(expected_sender_index)
                    < message.update_id.version
            {
                hosted_group.buffer_update(PendingInboundUpdate {
                    message,
                    loaded_schemas,
                })?;
                return Ok(());
            }

            let mut event_batches = Vec::new();
            let row_changes = apply_one_update_batch(hosted_group, message, &loaded_schemas)?;
            if !row_changes.is_empty() {
                event_batches.push(row_changes);
            }
            while let Some(action) = hosted_group.take_next_actionable_pending_update() {
                let pending_update = match action {
                    PendingUpdateAction::DropDuplicate => {
                        continue;
                    }
                    PendingUpdateAction::Apply(pending_update) => pending_update,
                };
                let row_changes = apply_one_update_batch(
                    hosted_group,
                    pending_update.message,
                    &pending_update.loaded_schemas,
                )?;
                if !row_changes.is_empty() {
                    event_batches.push(row_changes);
                }
            }
            event_batches
        };

        for row_changes in event_batches {
            block_on(self.listener.on_event(ReplicationEvent::DataChanged {
                rows: Box::new(VecRowProvider::new(row_changes)),
            }))
            .context(NotifyListenerSnafu)?;
        }
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

fn initial_version_vector(num_members: NonZeroUsize) -> VersionVector {
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

fn version_vector_covers(local: &VersionVector, required: &VersionVector) -> bool {
    if version_vector_num_members(local) != version_vector_num_members(required) {
        return false;
    }
    local
        .iter()
        .zip(required.iter())
        .all(|(local_version, required_version)| local_version >= required_version)
}

fn apply_one_update_batch(
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
                            if let Err(error) = shared.handle_reliable_delivery(deliver) {
                                shared.record_terminal_fault("reliable delivery", &error);
                                return;
                            }
                        }
                    }
                }
                while let Some(indication) = shared.host.pop_group_indication() {
                    made_progress = true;
                    match indication {
                        GroupBroadcastPortIndication::Deliver(deliver) => {
                            if let Err(error) = shared.handle_group_delivery(deliver) {
                                // Temporary conservative failure mode for the
                                // first replication slice. See flotsync-4x8
                                // for relaxing transient inbound ordering
                                // errors without terminating the runtime.
                                shared.record_terminal_fault("group delivery", &error);
                                return;
                            }
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
    #[snafu(display(
        "Inbound update for group {:?} carried invalid version 0 in update id {update_id}.",
        group_id
    ))]
    InvalidUpdateVersion {
        group_id: GroupId,
        update_id: UpdateId,
    },
    #[snafu(display(
        "Inbound update for group {:?} carried read VV of length {actual_members}, expected {expected_members}.",
        group_id
    ))]
    ReadVersionVectorSizeMismatch {
        group_id: GroupId,
        expected_members: usize,
        actual_members: usize,
    },
    #[snafu(display(
        "Inbound update for group {:?} buffered two different payloads for update id {update_id}.",
        group_id
    ))]
    ConflictingBufferedUpdate {
        group_id: GroupId,
        update_id: UpdateId,
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
            shared.ensure_running()?;
            let (group_id, dataset_ids) = publish_scope(&changes).map_err(ApiError::external)?;
            let loaded_schemas = shared
                .load_missing_dataset_schemas_for_publish(group_id, &dataset_ids)
                .await
                .map_err(ApiError::external)?;
            shared.ensure_running()?;
            let prepared_publish = shared
                .prepare_local_publish(changes, &loaded_schemas)
                .map_err(ApiError::external)?;

            // From here on the update has been accepted locally. A concurrent
            // inbound runtime fault must not turn this publish into an API error.
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
        Box::pin(async move {
            shared.ensure_running()?;
            shared.create_group(req).map_err(ApiError::external)
        })
    }

    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<GroupMigration, ApiError>> {
        let _ = req;
        let shared = self.shared.clone();
        Box::pin(async move {
            shared.ensure_running()?;
            Err(Self::unavailable("change_group_membership"))
        })
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
        collections::{HashMap, VecDeque},
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
        data_changes: Mutex<VecDeque<CapturedDataChange>>,
    }

    impl ListenerStub {
        fn wait_for_next_data_change(&self) -> CapturedDataChange {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(change) = self
                    .data_changes
                    .lock()
                    .expect("listener capture mutex must not be poisoned")
                    .pop_front()
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

        fn wait_for_data_change_count(&self, count: usize) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if self
                    .data_changes
                    .lock()
                    .expect("listener capture mutex must not be poisoned")
                    .len()
                    >= count
                {
                    return;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for {count} listener data-change events"
                );
                thread::sleep(Duration::from_millis(10));
            }
        }

        fn captured_data_changes(&self) -> Vec<CapturedDataChange> {
            self.data_changes
                .lock()
                .expect("listener capture mutex must not be poisoned")
                .iter()
                .cloned()
                .collect()
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
                            .push_back(CapturedDataChange {
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
                version: 1,
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

    #[test]
    fn inbound_updates_buffer_until_causal_dependencies_are_met_and_ignore_duplicates() {
        let alice_member = member(["alice"]);
        let bob_member = member(["bob"]);
        let dataset_id = DatasetId::try_new("docs").expect("dataset id should be valid");
        let schema = Arc::new(Schema::from_fields([Field::linear_string("title")]));
        let bob_listener = Arc::new(ListenerStub::default());
        let bob_store = Arc::new(
            StoreStub::new(bob_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
        );
        let bob = load_runtime_with_parts(
            Identifier::from_array(["app", "bob"]),
            bob_store,
            bob_listener.clone(),
        );
        let group_id = GroupId(Uuid::from_u128(22));
        bob.shared
            .install_group(
                group_id,
                GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                    .expect("group should build"),
            )
            .expect("group should install");

        let row_id = crate::api::RowId {
            group_id,
            dataset_id: dataset_id.clone(),
            row_key: crate::api::RowKey(Uuid::from_u128(23)),
        };
        let mut source_dataset = HostedDataset::new(schema);
        let first_operation = apply_local_upsert(
            &mut source_dataset,
            &row_id,
            &crate::row_values! { "title" => "first" },
            UpdateId {
                version: 1,
                node_index: 0,
            },
        )
        .expect("first operation should build")
        .expect("first operation should apply");
        let second_operation = apply_local_upsert(
            &mut source_dataset,
            &row_id,
            &crate::row_values! { "title" => "second" },
            UpdateId {
                version: 2,
                node_index: 0,
            },
        )
        .expect("second operation should build")
        .expect("second operation should apply");

        let member_count = NonZeroUsize::new(2).expect("group has two members");
        let first_message = UpdateBatchMessage {
            group_id,
            update_id: UpdateId {
                version: 1,
                node_index: 0,
            },
            read_vv: initial_version_vector(member_count),
            dataset_updates: vec![DatasetUpdateMessage {
                dataset_id: dataset_id.clone(),
                operations: vec![first_operation],
            }],
        };
        let mut second_read_vv = initial_version_vector(member_count);
        second_read_vv.increment_at(0);
        let second_message = UpdateBatchMessage {
            group_id,
            update_id: UpdateId {
                version: 2,
                node_index: 0,
            },
            read_vv: second_read_vv,
            dataset_updates: vec![DatasetUpdateMessage {
                dataset_id: dataset_id.clone(),
                operations: vec![second_operation],
            }],
        };

        bob.shared
            .apply_update_batch(alice_member.clone(), second_message)
            .expect("out-of-order update should buffer");
        assert!(bob_listener.captured_data_changes().is_empty());
        {
            let state = bob
                .shared
                .state
                .lock()
                .expect("runtime state mutex must not be poisoned");
            let hosted_group = state
                .groups
                .get(&group_id)
                .expect("group should still be hosted");
            assert_eq!(hosted_group.pending_updates.len(), 1);
            assert_eq!(hosted_group.applied_version(MemberIndex::new(0)), 0);
        }

        bob.shared
            .apply_update_batch(alice_member.clone(), first_message.clone())
            .expect("first update should apply and drain the pending second update");
        bob_listener.wait_for_data_change_count(2);
        assert_eq!(
            bob_listener.captured_data_changes(),
            vec![
                CapturedDataChange {
                    rows: vec![CapturedRowChange::Upsert {
                        row_id: row_id.clone(),
                        title: "first".to_owned(),
                    }],
                },
                CapturedDataChange {
                    rows: vec![CapturedRowChange::Upsert {
                        row_id: row_id.clone(),
                        title: "second".to_owned(),
                    }],
                },
            ]
        );
        {
            let state = bob
                .shared
                .state
                .lock()
                .expect("runtime state mutex must not be poisoned");
            let hosted_group = state
                .groups
                .get(&group_id)
                .expect("group should still be hosted");
            assert!(hosted_group.pending_updates.is_empty());
            assert_eq!(hosted_group.applied_version(MemberIndex::new(0)), 2);
            let hosted_dataset = hosted_group
                .datasets
                .get(&dataset_id)
                .expect("dataset should be materialised after apply");
            let hosted_row = hosted_dataset
                .clone_row(row_id.row_key)
                .expect("row should exist after buffered apply");
            assert_eq!(
                (&hosted_row as &dyn RowRead)
                    .get_field_value::<str>("title")
                    .expect("title field should decode")
                    .into_owned(),
                "second"
            );
        }

        bob.shared
            .apply_update_batch(alice_member, first_message)
            .expect("duplicate update should be ignored");
        assert_eq!(bob_listener.captured_data_changes().len(), 2);
    }

    #[test]
    fn buffered_updates_reject_conflicting_duplicate_payloads() {
        let alice_member = member(["alice"]);
        let bob_member = member(["bob"]);
        let dataset_id = DatasetId::try_new("docs").expect("dataset id should be valid");
        let schema = Arc::new(Schema::from_fields([Field::linear_string("title")]));
        let bob_store = Arc::new(
            StoreStub::new(bob_member.clone()).with_schema(dataset_id.clone(), schema.clone()),
        );
        let bob = load_runtime_with_parts(
            Identifier::from_array(["app", "bob"]),
            bob_store,
            Arc::new(ListenerStub::default()),
        );
        let group_id = GroupId(Uuid::from_u128(24));
        bob.shared
            .install_group(
                group_id,
                GroupMembers::from_ordered_members(vec![alice_member.clone(), bob_member.clone()])
                    .expect("group should build"),
            )
            .expect("group should install");

        let row_id = crate::api::RowId {
            group_id,
            dataset_id: dataset_id.clone(),
            row_key: crate::api::RowKey(Uuid::from_u128(25)),
        };
        let member_count = NonZeroUsize::new(2).expect("group has two members");

        let mut first_source_dataset = HostedDataset::new(schema.clone());
        let first_operation = apply_local_upsert(
            &mut first_source_dataset,
            &row_id,
            &crate::row_values! { "title" => "first" },
            UpdateId {
                version: 1,
                node_index: 0,
            },
        )
        .expect("first operation should build")
        .expect("first operation should apply");

        let mut conflicting_source_dataset = HostedDataset::new(schema);
        let conflicting_operation = apply_local_upsert(
            &mut conflicting_source_dataset,
            &row_id,
            &crate::row_values! { "title" => "conflict" },
            UpdateId {
                version: 1,
                node_index: 0,
            },
        )
        .expect("conflicting operation should build")
        .expect("conflicting operation should apply");

        let buffered_message = UpdateBatchMessage {
            group_id,
            update_id: UpdateId {
                version: 2,
                node_index: 0,
            },
            read_vv: {
                let mut read_vv = initial_version_vector(member_count);
                read_vv.increment_at(0);
                read_vv
            },
            dataset_updates: vec![DatasetUpdateMessage {
                dataset_id: dataset_id.clone(),
                operations: vec![first_operation],
            }],
        };
        let conflicting_message = UpdateBatchMessage {
            group_id,
            update_id: UpdateId {
                version: 2,
                node_index: 0,
            },
            read_vv: {
                let mut read_vv = initial_version_vector(member_count);
                read_vv.increment_at(0);
                read_vv
            },
            dataset_updates: vec![DatasetUpdateMessage {
                dataset_id,
                operations: vec![conflicting_operation],
            }],
        };

        bob.shared
            .apply_update_batch(alice_member.clone(), buffered_message)
            .expect("first out-of-order update should buffer");
        let error = bob
            .shared
            .apply_update_batch(alice_member, conflicting_message)
            .expect_err("conflicting duplicate payload should fail");
        match error {
            InboundDeliveryError::ConflictingBufferedUpdate {
                group_id: actual_group_id,
                update_id,
            } => {
                assert_eq!(actual_group_id, group_id);
                assert_eq!(
                    update_id,
                    UpdateId {
                        version: 2,
                        node_index: 0,
                    }
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

mod host;
mod messages;

use crate::{
    GroupMembers,
    GroupMemberships,
    SharedGroupMemberships,
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
use flotsync_messages::{
    buffa::Message as _,
    codecs::datamodel::{OperationCodecError, decode_schema_operation, encode_schema_operation},
};
use flotsync_utils::{BoxFuture, LocalActor, impl_local_actor};
use kompact::prelude::*;
use messages::{
    BootstrapGroupMessage,
    DatasetUpdateMessage,
    RuntimeMessage,
    RuntimeMessageError,
    UpdateBatchMessage,
    WireRuntimeMessage,
    WireUpdateBatchMessage,
};
use snafu::prelude::*;
use std::{
    collections::{BTreeMap, HashMap, btree_map::Entry},
    io,
    num::NonZeroUsize,
    sync::Arc,
};
#[cfg(test)]
use std::{
    pin::pin,
    task::{Context, Poll, Waker},
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
    let local_member =
        store
            .local_member_identity()
            .await
            .boxed()
            .context(crate::RuntimeSnafu {
                application_id: application_id.clone(),
            })?;
    let mut host = DeliveryRuntimeHost::new(local_member.clone())
        .boxed()
        .context(crate::RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    let runtime_component = host
        .start_runtime_component(local_member, store, listener)
        .boxed()
        .context(crate::RuntimeSnafu {
            application_id: application_id.clone(),
        })?;
    let runtime_ref = runtime_component
        .actor_ref()
        .hold()
        .expect("replication runtime component must expose a strong actor ref");
    let host = Arc::new(host);
    Ok(Arc::new(ReplicationRuntime {
        _application_id: application_id,
        runtime_ref: Some(runtime_ref),
        host,
        _config: config,
    }))
}

/// Concrete application-facing runtime returned by `load_replication_runtime`.
///
/// The current slice supports one real end-to-end path:
/// creating a fixed-membership group and installing that membership remotely
/// through reliable delivery bootstrap messages.
struct ReplicationRuntime {
    _application_id: Identifier,
    runtime_ref: Option<ActorRefStrong<ReplicationRuntimeMessage>>,
    #[cfg_attr(not(test), allow(dead_code))]
    host: Arc<DeliveryRuntimeHost>,
    _config: ReplicationConfig,
}

impl ReplicationRuntime {
    fn runtime_ref(&self) -> &ActorRefStrong<ReplicationRuntimeMessage> {
        self.runtime_ref
            .as_ref()
            .expect("replication runtime actor ref must exist while the handle is live")
    }

    fn unavailable(operation: &'static str) -> ApiError {
        ApiError::ApiExternal {
            source: Box::new(io::Error::other(format!(
                "replication runtime host is ready, but {operation} is not implemented yet"
            ))),
        }
    }
}

impl Drop for ReplicationRuntime {
    fn drop(&mut self) {
        self.runtime_ref.take();
        if let Some(host) = Arc::get_mut(&mut self.host) {
            host.shutdown();
        }
    }
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

#[derive(Debug)]
enum ReplicationRuntimeMessage {
    PublishChanges(Ask<Vec<RowMutation>, Result<PublishReceipt, ApiError>>),
    CreateGroup(Ask<CreateGroupRequest, Result<GroupId, ApiError>>),
    ChangeGroupMembership(Ask<ChangeGroupMembershipRequest, Result<GroupMigration, ApiError>>),
    #[cfg(test)]
    InstallGroupForTest(Ask<TestInstallGroup, Result<(), GroupInstallError>>),
    #[cfg(test)]
    ApplyUpdateBatchForTest(Ask<TestApplyUpdateBatch, Result<(), InboundDeliveryError>>),
}

#[cfg(test)]
#[derive(Debug)]
struct TestInstallGroup {
    group_id: GroupId,
    members: GroupMembers,
}

#[cfg(test)]
#[derive(Debug)]
struct TestApplyUpdateBatch {
    sender: MemberIdentity,
    message: UpdateBatchMessage,
}

#[derive(ComponentDefinition)]
struct ReplicationRuntimeComponent {
    ctx: ComponentContext<Self>,
    group_broadcast: RequiredPort<crate::delivery::contracts::GroupBroadcastPort>,
    reliable_delivery: RequiredPort<crate::delivery::contracts::ReliableDeliveryPort>,
    local_member: MemberIdentity,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    group_memberships: SharedGroupMemberships,
    state: RuntimeState,
    terminal_fault: Option<TerminalRuntimeFault>,
}

impl ReplicationRuntimeComponent {
    fn new(
        local_member: MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        group_memberships: SharedGroupMemberships,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            group_broadcast: RequiredPort::uninitialised(),
            reliable_delivery: RequiredPort::uninitialised(),
            local_member,
            store,
            listener,
            group_memberships,
            state: RuntimeState::default(),
            terminal_fault: None,
        }
    }

    fn ensure_running(&self) -> Result<(), ApiError> {
        if let Some(fault) = self.terminal_fault.clone() {
            return Err(ApiError::ApiExternal {
                source: Box::new(io::Error::other(format!(
                    "replication runtime terminated after inbound delivery failure: {fault}"
                ))),
            });
        }
        Ok(())
    }

    fn record_terminal_fault(&mut self, operation: &'static str, error: &InboundDeliveryError) {
        if self.terminal_fault.is_some() {
            return;
        }
        let fault = TerminalRuntimeFault {
            operation,
            message: error.to_string(),
        };
        error!(self.log(), "terminal runtime failure: {}", fault);
        self.terminal_fault = Some(fault);
    }

    fn missing_dataset_ids_for_publish(
        &self,
        group_id: GroupId,
        dataset_ids: &[DatasetId],
    ) -> Result<Vec<DatasetId>, PublishChangesError> {
        let hosted_group = self
            .state
            .groups
            .get(&group_id)
            .context(UnknownGroupSnafu { group_id })?;
        let mut missing_dataset_ids = Vec::new();
        for dataset_id in dataset_ids {
            if hosted_group.datasets.contains_key(dataset_id)
                || missing_dataset_ids.contains(dataset_id)
            {
                continue;
            }
            missing_dataset_ids.push(dataset_id.clone());
        }
        Ok(missing_dataset_ids)
    }

    fn missing_dataset_ids_for_inbound(
        &self,
        group_id: GroupId,
        dataset_ids: &[DatasetId],
    ) -> Result<Vec<DatasetId>, InboundDeliveryError> {
        let hosted_group = self
            .state
            .groups
            .get(&group_id)
            .context(UnknownHostedGroupSnafu { group_id })?;
        let mut missing_dataset_ids = Vec::new();
        for dataset_id in dataset_ids {
            if hosted_group.datasets.contains_key(dataset_id)
                || missing_dataset_ids.contains(dataset_id)
            {
                continue;
            }
            missing_dataset_ids.push(dataset_id.clone());
        }
        Ok(missing_dataset_ids)
    }

    fn create_group(&mut self, req: CreateGroupRequest) -> Result<GroupId, CreateGroupError> {
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
        // Temporary byte serialisation at the delivery-envelope boundary.
        // See flotsync-ylo for the payload/encryption redesign.
        let payload = bootstrap.encode_to_proto().encode_to_bytes();
        for recipient in members
            .ordered_members()
            .into_iter()
            .filter(|member| member != &self.local_member)
        {
            self.reliable_delivery.trigger(
                crate::delivery::contracts::ReliableDeliveryPortRequest::Submit(
                    ReliableDeliverySubmit {
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
                    },
                ),
            );
        }
        Ok(group_id)
    }

    fn prepare_local_publish(
        &mut self,
        changes: Vec<RowMutation>,
        loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
    ) -> Result<PreparedPublish, PublishChangesError> {
        let (group_id, _) = publish_scope(&changes)?;
        let hosted_group = self
            .state
            .groups
            .get_mut(&group_id)
            .context(UnknownGroupSnafu { group_id })?;

        let read_versions = hosted_group.version_vector.clone();
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
        // Temporary byte serialisation at the delivery-envelope boundary.
        // See flotsync-ylo for the payload/encryption redesign.
        let payload = RuntimeMessage::UpdateBatch(UpdateBatchMessage {
            group_id,
            update_id,
            read_versions,
            dataset_updates,
        })
        .encode_to_proto()
        .encode_to_bytes();
        Ok(PreparedPublish {
            group_id,
            update_id,
            payload,
        })
    }

    fn handle_reliable_delivery(
        &mut self,
        deliver: ReliableDeliveryDeliver,
    ) -> Result<(), InboundDeliveryError> {
        let message = WireRuntimeMessage::decode_from_slice(&deliver.envelope.payload.ciphertext)
            .context(DecodeMessageSnafu)?;
        match message {
            WireRuntimeMessage::BootstrapGroup(message) => {
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
            WireRuntimeMessage::UpdateBatch(_) => UnexpectedReliableMessageSnafu.fail(),
        }
    }

    fn handle_group_delivery(
        &mut self,
        deliver: GroupBroadcastDeliver,
    ) -> Result<Handled, InboundDeliveryError> {
        let sender = deliver.envelope.header.sender.clone();
        let message = WireRuntimeMessage::decode_from_slice(&deliver.envelope.payload.ciphertext)
            .context(DecodeMessageSnafu)?;
        match message {
            WireRuntimeMessage::BootstrapGroup(_) => UnexpectedGroupMessageSnafu.fail(),
            WireRuntimeMessage::UpdateBatch(message) => {
                Ok(self.handle_update_batch(sender, message))
            }
        }
    }

    fn handle_update_batch(
        &mut self,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
    ) -> Handled {
        let dataset_ids: Vec<_> = message
            .dataset_updates
            .iter()
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect();
        let missing_dataset_ids =
            match self.missing_dataset_ids_for_inbound(message.group_id, &dataset_ids) {
                Ok(missing_dataset_ids) => missing_dataset_ids,
                Err(error) => {
                    self.record_terminal_fault("group delivery", &error);
                    return Handled::Ok;
                }
            };
        let store = self.store.clone();
        let listener = self.listener.clone();
        Handled::block_on(self, move |mut async_self| async move {
            let loaded_schemas = load_inbound_schemas(store, missing_dataset_ids).await;
            let reply = loaded_schemas.and_then(|loaded_schemas| {
                async_self.apply_wire_update_batch_loaded(sender, message, loaded_schemas)
            });
            match reply {
                Ok(event_batches) => {
                    for row_changes in event_batches {
                        let notify_result = listener
                            .on_event(ReplicationEvent::DataChanged {
                                rows: Box::new(VecRowProvider::new(row_changes)),
                            })
                            .await;
                        if let Err(error) = notify_result {
                            async_self.record_terminal_fault(
                                "group delivery",
                                &InboundDeliveryError::NotifyListener { source: error },
                            );
                            return;
                        }
                    }
                }
                Err(error) => {
                    // Temporary conservative failure mode for the
                    // first replication slice. See flotsync-4x8
                    // for relaxing transient inbound ordering
                    // errors without terminating the runtime.
                    async_self.record_terminal_fault("group delivery", &error);
                }
            }
        })
    }

    fn apply_wire_update_batch_loaded(
        &mut self,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
        loaded_schemas: HashMap<DatasetId, Arc<Schema>>,
    ) -> Result<Vec<Vec<RowChange>>, InboundDeliveryError> {
        let group_id = message.group_id;
        let hosted_group = self
            .state
            .groups
            .get_mut(&group_id)
            .context(UnknownHostedGroupSnafu { group_id })?;
        let member_count =
            NonZeroUsize::new(hosted_group.members.len()).expect("hosted group must be non-empty");
        let message = message
            .into_runtime(member_count)
            .context(DecodeReadVersionsSnafu { group_id })?;
        self.apply_update_batch_loaded(sender, message, loaded_schemas)
    }

    fn apply_update_batch_loaded(
        &mut self,
        sender: MemberIdentity,
        message: UpdateBatchMessage,
        loaded_schemas: HashMap<DatasetId, Arc<Schema>>,
    ) -> Result<Vec<Vec<RowChange>>, InboundDeliveryError> {
        let hosted_group =
            self.state
                .groups
                .get_mut(&message.group_id)
                .context(UnknownHostedGroupSnafu {
                    group_id: message.group_id,
                })?;
        let expected_sender_index =
            hosted_group
                .members
                .member_index(&sender)
                .context(UpdateSenderNotInGroupSnafu {
                    group_id: message.group_id,
                    sender: sender.clone(),
                })?;
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
        if hosted_group.has_applied(message.update_id) {
            return Ok(Vec::new());
        }
        if !version_vector_covers(&hosted_group.version_vector, &message.read_versions)
            || hosted_group.expected_next_version(expected_sender_index) < message.update_id.version
        {
            hosted_group.buffer_update(PendingInboundUpdate {
                message,
                loaded_schemas,
            })?;
            return Ok(Vec::new());
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

        Ok(event_batches)
    }

    fn install_group(
        &mut self,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Result<(), GroupInstallError> {
        if let Some(existing_group) = self.state.groups.get(&group_id) {
            if existing_group.members == members {
                return Ok(());
            }
            return ConflictingExistingGroupSnafu { group_id }.fail();
        }

        let hosted_group = HostedGroup::new(&self.local_member, members)?;
        self.state.groups.insert(group_id, hosted_group);
        self.group_memberships
            .replace(self.state.membership_snapshot());
        Ok(())
    }
}

async fn load_publish_schemas(
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

async fn load_inbound_schemas(
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
        "Inbound update for group {:?} carried invalid read versions.",
        group_id
    ))]
    DecodeReadVersions {
        group_id: GroupId,
        source: RuntimeMessageError,
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

ignore_lifecycle!(ReplicationRuntimeComponent);

impl ReplicationRuntimeComponent {
    fn handle_publish_changes(
        &mut self,
        ask: Ask<Vec<RowMutation>, Result<PublishReceipt, ApiError>>,
    ) -> Handled {
        let (promise, changes) = ask.take();
        let preflight = self.ensure_running().and_then(|()| {
            let (group_id, dataset_ids) = publish_scope(&changes)
                .boxed()
                .context(crate::ApiExternalSnafu)?;
            let missing_dataset_ids = self
                .missing_dataset_ids_for_publish(group_id, &dataset_ids)
                .boxed()
                .context(crate::ApiExternalSnafu)?;
            Ok(missing_dataset_ids)
        });
        let missing_dataset_ids = match preflight {
            Ok(missing_dataset_ids) => missing_dataset_ids,
            Err(error) => {
                if promise.fulfil(Err(error)).is_err() {
                    debug!(self.log(), "dropping publish_changes reply");
                }
                return Handled::Ok;
            }
        };
        let store = self.store.clone();
        Handled::block_on(self, move |mut async_self| async move {
            let loaded_schemas = load_publish_schemas(store, missing_dataset_ids).await;
            let reply = (|| -> Result<PublishReceipt, ApiError> {
                let loaded_schemas = loaded_schemas.boxed().context(crate::ApiExternalSnafu)?;
                let prepared_publish = async_self
                    .prepare_local_publish(changes, &loaded_schemas)
                    .boxed()
                    .context(crate::ApiExternalSnafu)?;
                let local_member = async_self.local_member.clone();
                async_self.group_broadcast.trigger(
                    crate::delivery::contracts::GroupBroadcastPortRequest::Submit(
                        GroupBroadcastSubmit {
                            delivery_class: DeliveryClass::BestEffort,
                            envelope: GroupMessageEnvelope {
                                header: GroupMessageHeader {
                                    group_id: prepared_publish.group_id,
                                    sender: local_member,
                                    message_id: MessageId(Uuid::new_v4()),
                                },
                                payload: EncryptedPayload {
                                    ciphertext: prepared_publish.payload,
                                },
                                footer: placeholder_signed_footer(),
                            },
                            suppress_self_delivery: true,
                        },
                    ),
                );
                Ok(PublishReceipt {
                    update_id: prepared_publish.update_id,
                })
            })();
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping publish_changes reply");
            }
        })
    }

    fn handle_create_group(
        &mut self,
        ask: Ask<CreateGroupRequest, Result<GroupId, ApiError>>,
    ) -> Handled {
        let (promise, req) = ask.take();
        let reply = self.ensure_running().and_then(|()| {
            self.create_group(req)
                .boxed()
                .context(crate::ApiExternalSnafu)
        });
        if promise.fulfil(reply).is_err() {
            debug!(self.log(), "dropping create_group reply");
        }
        Handled::Ok
    }

    fn handle_change_group_membership(
        &mut self,
        ask: Ask<ChangeGroupMembershipRequest, Result<GroupMigration, ApiError>>,
    ) -> Handled {
        let (promise, req) = ask.take();
        let _ = req;
        let reply = self
            .ensure_running()
            .and_then(|()| Err(ReplicationRuntime::unavailable("change_group_membership")));
        if promise.fulfil(reply).is_err() {
            debug!(self.log(), "dropping change_group_membership reply");
        }
        Handled::Ok
    }

    #[cfg(test)]
    fn handle_test_install_group(
        &mut self,
        ask: Ask<TestInstallGroup, Result<(), GroupInstallError>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        let reply = self.install_group(request.group_id, request.members);
        let _ = promise.fulfil(reply);
        Handled::Ok
    }

    #[cfg(test)]
    fn handle_test_apply_update_batch(
        &mut self,
        ask: Ask<TestApplyUpdateBatch, Result<(), InboundDeliveryError>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        let dataset_ids: Vec<_> = request
            .message
            .dataset_updates
            .iter()
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect();
        let missing_dataset_ids =
            match self.missing_dataset_ids_for_inbound(request.message.group_id, &dataset_ids) {
                Ok(missing_dataset_ids) => missing_dataset_ids,
                Err(error) => {
                    let _ = promise.fulfil(Err(error));
                    return Handled::Ok;
                }
            };
        let store = self.store.clone();
        let listener = self.listener.clone();
        Handled::block_on(self, move |mut async_self| async move {
            let loaded_schemas = load_inbound_schemas(store, missing_dataset_ids).await;
            let reply = match loaded_schemas {
                Ok(loaded_schemas) => {
                    match async_self.apply_update_batch_loaded(
                        request.sender,
                        request.message,
                        loaded_schemas,
                    ) {
                        Ok(event_batches) => {
                            let mut notify_error = None;
                            for row_changes in event_batches {
                                let notify_result = listener
                                    .on_event(ReplicationEvent::DataChanged {
                                        rows: Box::new(VecRowProvider::new(row_changes)),
                                    })
                                    .await;
                                if let Err(error) = notify_result {
                                    notify_error = Some(InboundDeliveryError::NotifyListener {
                                        source: error,
                                    });
                                    break;
                                }
                            }
                            match notify_error {
                                Some(error) => Err(error),
                                None => Ok(()),
                            }
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            };
            let _ = promise.fulfil(reply);
        })
    }
}

impl Require<crate::delivery::contracts::ReliableDeliveryPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> Handled {
        if self.terminal_fault.is_some() {
            return Handled::Ok;
        }
        match indication {
            ReliableDeliveryPortIndication::Deliver(deliver) => {
                if let Err(error) = self.handle_reliable_delivery(deliver) {
                    self.record_terminal_fault("reliable delivery", &error);
                }
                Handled::Ok
            }
        }
    }
}

impl Require<crate::delivery::contracts::GroupBroadcastPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: GroupBroadcastPortIndication) -> Handled {
        if self.terminal_fault.is_some() {
            return Handled::Ok;
        }
        match indication {
            GroupBroadcastPortIndication::Deliver(deliver) => {
                match self.handle_group_delivery(deliver) {
                    Ok(handled) => handled,
                    Err(error) => {
                        self.record_terminal_fault("group delivery", &error);
                        Handled::Ok
                    }
                }
            }
        }
    }
}

impl LocalActor for ReplicationRuntimeComponent {
    type Message = ReplicationRuntimeMessage;

    fn receive(&mut self, msg: Self::Message) -> Handled {
        match msg {
            ReplicationRuntimeMessage::PublishChanges(ask) => self.handle_publish_changes(ask),
            ReplicationRuntimeMessage::CreateGroup(ask) => self.handle_create_group(ask),
            ReplicationRuntimeMessage::ChangeGroupMembership(ask) => {
                self.handle_change_group_membership(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeMessage::InstallGroupForTest(ask) => {
                self.handle_test_install_group(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeMessage::ApplyUpdateBatchForTest(ask) => {
                self.handle_test_apply_update_batch(ask)
            }
        }
    }
}

impl_local_actor!(ReplicationRuntimeComponent);

async fn resolve_runtime_future<T: Send>(
    future: KFuture<Result<T, ApiError>>,
) -> Result<T, ApiError> {
    match future.await {
        Ok(reply) => reply,
        Err(_) => Err(ApiError::ApiExternal {
            source: Box::new(io::Error::other(
                "replication runtime component became unavailable",
            )),
        }),
    }
}

#[cfg(test)]
fn wait_for_test_future<F>(future: F) -> F::Output
where
    F: Future,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for test future to resolve"
                );
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

#[cfg(test)]
impl ReplicationRuntime {
    fn install_group_for_test(
        &self,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Result<(), GroupInstallError> {
        let (promise, future) = promise::<Result<(), GroupInstallError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::InstallGroupForTest(Ask::new(
                promise,
                TestInstallGroup { group_id, members },
            )));
        match wait_for_test_future(future) {
            Ok(reply) => reply,
            Err(_) => {
                panic!("replication runtime component became unavailable during test install")
            }
        }
    }

    fn apply_update_batch_for_test(
        &self,
        sender: MemberIdentity,
        message: UpdateBatchMessage,
    ) -> Result<(), InboundDeliveryError> {
        let (promise, future) = promise::<Result<(), InboundDeliveryError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::ApplyUpdateBatchForTest(
                Ask::new(promise, TestApplyUpdateBatch { sender, message }),
            ));
        match wait_for_test_future(future) {
            Ok(reply) => reply,
            Err(_) => {
                panic!(
                    "replication runtime component became unavailable during test apply_update_batch"
                )
            }
        }
    }
}

impl ReplicationApi for ReplicationRuntime {
    fn publish_changes(
        &self,
        changes: Vec<RowMutation>,
    ) -> BoxFuture<'_, Result<PublishReceipt, ApiError>> {
        let (promise, future) = promise::<Result<PublishReceipt, ApiError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::PublishChanges(Ask::new(
                promise, changes,
            )));
        Box::pin(async move { resolve_runtime_future(future).await })
    }

    fn create_group(
        &self,
        req: CreateGroupRequest,
    ) -> BoxFuture<'_, Result<crate::api::GroupId, ApiError>> {
        let (promise, future) = promise::<Result<GroupId, ApiError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::CreateGroup(Ask::new(
                promise, req,
            )));
        Box::pin(async move { resolve_runtime_future(future).await })
    }

    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<GroupMigration, ApiError>> {
        let (promise, future) = promise::<Result<GroupMigration, ApiError>>();
        self.runtime_ref()
            .tell(ReplicationRuntimeMessage::ChangeGroupMembership(Ask::new(
                promise, req,
            )));
        Box::pin(async move { resolve_runtime_future(future).await })
    }
}

#[cfg(test)]
mod tests;

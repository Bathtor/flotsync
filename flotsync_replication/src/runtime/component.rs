use super::{
    errors::*,
    in_memory::{
        LocalDataset,
        LocalGroupState,
        LocalRuntimeState,
        apply_local_delete,
        apply_local_upsert,
        apply_one_update_batch,
        collect_group_dataset_scope,
        working_dataset_for_publish,
    },
    messages::{
        BootstrapGroupMessage,
        DatasetUpdateMessage,
        RuntimeMessage,
        UpdateBatchMessage,
        WireRuntimeMessage,
        WireUpdateBatchMessage,
    },
};
use crate::{
    GroupMembers,
    SharedGroupMemberships,
    api::{
        ApiError,
        ApiExternalSnafu,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DatasetId,
        GroupId,
        GroupMigration,
        MemberIdentity,
        MemberIndex,
        PublishReceipt,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationStore,
        RowChange,
        RowMutation,
        StoreError,
        providers::VecRowProvider,
    },
    delivery::{
        contracts::{
            GroupBroadcastPort,
            GroupBroadcastPortIndication,
            GroupBroadcastPortRequest,
            ReliableDeliveryPort,
            ReliableDeliveryPortIndication,
            ReliableDeliveryPortRequest,
        },
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
use flotsync_core::versions::{UpdateId, VersionVector};
use flotsync_data_types::Schema;
use flotsync_messages::buffa::Message as BuffaMessage;
use kompact::prelude::*;
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(super) struct TerminalRuntimeFault {
    operation: &'static str,
    message: String,
}

impl std::fmt::Display for TerminalRuntimeFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.operation, self.message)
    }
}

/// One local publish batch after local apply, encoding, and delivery-envelope preparation.
struct PreparedLocalPublish {
    group_id: GroupId,
    update_id: UpdateId,
    payload: bytes::Bytes,
}

/// One inbound update batch buffered until its causal dependencies are met.
pub(super) struct BufferedInboundUpdate {
    pub(super) message: UpdateBatchMessage,
    pub(super) loaded_schemas: HashMap<DatasetId, Arc<Schema>>,
}

/// Prepared local dataset changes for one outbound publish.
struct PreparedDatasetUpdates {
    dataset_updates: Vec<DatasetUpdateMessage>,
    working_datasets: HashMap<DatasetId, LocalDataset>,
}

enum DatasetSchemaLoadError {
    Load {
        dataset_id: DatasetId,
        source: StoreError,
    },
    Missing {
        dataset_id: DatasetId,
    },
}

/// Local-actor messages understood by [`ReplicationRuntimeComponent`].
///
/// This is the imperative bridge surface between the public async runtime
/// handle and the Kompact-hosted replication component.
#[derive(Debug)]
pub enum ReplicationRuntimeMessage {
    /// Submit one local publish request through the component interface.
    PublishChanges(Ask<Vec<RowMutation>, Result<PublishReceipt, ApiError>>),
    /// Create one new fixed-membership group through the component interface.
    CreateGroup(Ask<CreateGroupRequest, Result<GroupId, ApiError>>),
    /// Request one group-membership change through the component interface.
    ChangeGroupMembership(Ask<ChangeGroupMembershipRequest, Result<GroupMigration, ApiError>>),
    #[cfg(test)]
    Test(ReplicationRuntimeTestMessage),
}

#[cfg(test)]
#[derive(Debug)]
#[allow(
    private_interfaces,
    reason = "test-only ask plumbing reuses internal error types"
)]
pub enum ReplicationRuntimeTestMessage {
    InstallGroup(Ask<(GroupId, GroupMembers), Result<(), GroupInstallError>>),
    ApplyUpdateBatch(Ask<(MemberIdentity, UpdateBatchMessage), Result<(), InboundDeliveryError>>),
}

#[cfg(test)]
impl ReplicationRuntimeMessage {
    pub(super) fn test_install_group(
        promise: KPromise<Result<(), GroupInstallError>>,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::InstallGroup(Ask::new(
            promise,
            (group_id, members),
        )))
    }

    pub(super) fn test_apply_update_batch(
        promise: KPromise<Result<(), InboundDeliveryError>>,
        sender: MemberIdentity,
        message: UpdateBatchMessage,
    ) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::ApplyUpdateBatch(Ask::new(
            promise,
            (sender, message),
        )))
    }
}

/// Stateful Kompact component that hosts one in-memory replication runtime.
///
/// It owns the local replicated group state, speaks to the delivery-layer
/// components through required ports, and translates the public replication
/// API into deterministic local state transitions plus outbound delivery work.
#[derive(ComponentDefinition)]
pub struct ReplicationRuntimeComponent {
    ctx: ComponentContext<Self>,
    group_broadcast: RequiredPort<GroupBroadcastPort>,
    reliable_delivery: RequiredPort<ReliableDeliveryPort>,
    local_member: MemberIdentity,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    group_memberships: SharedGroupMemberships,
    state: LocalRuntimeState,
    terminal_fault: Option<TerminalRuntimeFault>,
}

impl ReplicationRuntimeComponent {
    /// Construct one replication runtime component for one local member.
    pub fn new(
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
            state: LocalRuntimeState::default(),
            terminal_fault: None,
        }
    }

    fn ensure_running(&self) -> Result<(), ApiError> {
        if let Some(fault) = self.terminal_fault.clone() {
            return Err(ApiError::RuntimeTerminated {
                fault: fault.to_string(),
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
        self.ctx.suicide();
    }

    fn reply_api<T>(
        &self,
        promise: KPromise<Result<T, ApiError>>,
        operation: &'static str,
        reply: Result<T, ApiError>,
    ) where
        T: Send + 'static,
    {
        if promise.fulfil(reply).is_err() {
            warn!(self.log(), "dropping {operation} reply");
        }
    }

    /// Validate one publish request locally and return the missing dataset
    /// schemas that must be loaded before local staging can proceed.
    fn preflight_publish(&self, changes: &[RowMutation]) -> Result<HashSet<DatasetId>, ApiError> {
        let (group_id, dataset_ids) = collect_group_dataset_scope(changes)
            .boxed()
            .context(ApiExternalSnafu)?;
        let local_group = self
            .state
            .groups
            .get(&group_id)
            .context(UnknownGroupSnafu { group_id })
            .boxed()
            .context(ApiExternalSnafu)?;
        Ok(local_group.missing_dataset_ids(&dataset_ids))
    }

    async fn load_dataset_schemas(
        store: Arc<dyn ReplicationStore>,
        missing_dataset_ids: HashSet<DatasetId>,
    ) -> Result<HashMap<DatasetId, Arc<Schema>>, DatasetSchemaLoadError> {
        let mut loaded_schemas = HashMap::with_capacity(missing_dataset_ids.len());
        for dataset_id in missing_dataset_ids {
            let schema = store
                .load_dataset_schema(&dataset_id)
                .await
                .map_err(|source| DatasetSchemaLoadError::Load {
                    dataset_id: dataset_id.clone(),
                    source,
                })?;
            let schema = schema.ok_or_else(|| DatasetSchemaLoadError::Missing {
                dataset_id: dataset_id.clone(),
            })?;
            loaded_schemas.insert(dataset_id, schema.into_shared());
        }
        Ok(loaded_schemas)
    }

    /// Submit one encoded live update to the group-broadcast layer.
    fn submit_group_update(&mut self, prepared_publish: PreparedLocalPublish) {
        let local_member = self.local_member.clone();
        self.group_broadcast
            .trigger(GroupBroadcastPortRequest::Submit(GroupBroadcastSubmit {
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
            }));
    }

    /// Encode one runtime message into the temporary byte payload expected by
    /// the current delivery-envelope boundary.
    fn encode_runtime_payload(message: RuntimeMessage) -> bytes::Bytes {
        // Temporary byte serialisation at the delivery-envelope boundary.
        // See flotsync-ylo for the payload/encryption redesign.
        message.encode_to_proto().encode_to_bytes()
    }

    /// Submit the reliable bootstrap fan-out for one newly created group.
    fn submit_group_bootstrap(&mut self, group_id: GroupId, members: &GroupMembers) {
        let payload =
            Self::encode_runtime_payload(RuntimeMessage::BootstrapGroup(BootstrapGroupMessage {
                group_id,
                members: members.ordered_members(),
            }));
        for recipient in members
            .ordered_members()
            .into_iter()
            .filter(|member| member != &self.local_member)
        {
            self.reliable_delivery
                .trigger(ReliableDeliveryPortRequest::Submit(
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
                ));
        }
    }

    fn collect_dataset_ids(dataset_updates: &[DatasetUpdateMessage]) -> HashSet<DatasetId> {
        dataset_updates
            .iter()
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect()
    }

    fn create_group(&mut self, req: CreateGroupRequest) -> Result<GroupId, CreateGroupError> {
        if req.initial_state.is_some() {
            return InitialStateUnsupportedSnafu.fail();
        }

        let members =
            GroupMembers::from_ordered_members(req.members).context(InvalidMembersSnafu)?;
        ensure!(
            members.contains(&self.local_member),
            LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );

        let group_id = GroupId(Uuid::new_v4());
        self.install_group(group_id, members.clone())
            .context(InstallGroupSnafu)?;
        self.submit_group_bootstrap(group_id, &members);
        Ok(group_id)
    }

    fn prepare_local_publish(
        &mut self,
        changes: Vec<RowMutation>,
        loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
    ) -> Result<PreparedLocalPublish, PublishChangesError> {
        let (group_id, _) = collect_group_dataset_scope(&changes)?;
        let local_group = self
            .state
            .groups
            .get_mut(&group_id)
            .context(UnknownGroupSnafu { group_id })?;

        let read_versions = local_group.version_vector.clone();
        let update_id = Self::next_local_update_id(local_group, group_id)?;
        let prepared_datasets = Self::build_local_dataset_updates(
            group_id,
            local_group,
            loaded_schemas,
            changes,
            update_id,
        )?;
        let payload = Self::finalise_local_publish(
            local_group,
            group_id,
            update_id,
            read_versions,
            prepared_datasets,
        );
        Ok(PreparedLocalPublish {
            group_id,
            update_id,
            payload,
        })
    }

    fn finalise_local_publish(
        local_group: &mut LocalGroupState,
        group_id: GroupId,
        update_id: UpdateId,
        read_versions: VersionVector,
        prepared_datasets: PreparedDatasetUpdates,
    ) -> bytes::Bytes {
        for (dataset_id, working_dataset) in prepared_datasets.working_datasets {
            local_group.datasets.insert(dataset_id, working_dataset);
        }
        local_group
            .version_vector
            .increment_at(local_group.local_member_index.as_u32() as usize);

        Self::encode_runtime_payload(RuntimeMessage::UpdateBatch(UpdateBatchMessage {
            group_id,
            update_id,
            read_versions,
            dataset_updates: prepared_datasets.dataset_updates,
        }))
    }

    fn next_local_update_id(
        local_group: &LocalGroupState,
        group_id: GroupId,
    ) -> Result<UpdateId, PublishChangesError> {
        let next_local_version = local_group
            .applied_version(local_group.local_member_index)
            .checked_add(1)
            .context(ExhaustedUpdateIdsSnafu { group_id })?;
        Ok(UpdateId {
            version: next_local_version,
            node_index: local_group.local_member_index.as_u32(),
        })
    }

    fn build_local_dataset_updates(
        group_id: GroupId,
        local_group: &LocalGroupState,
        loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
        changes: Vec<RowMutation>,
        update_id: UpdateId,
    ) -> Result<PreparedDatasetUpdates, PublishChangesError> {
        let mut working_datasets = HashMap::<DatasetId, LocalDataset>::new();
        let mut encoded_operations: HashMap<
            DatasetId,
            Vec<flotsync_messages::datamodel::SchemaOperation>,
        > = HashMap::new();

        'changes: for mutation in changes {
            let row_id = mutation.row_id().clone();
            let working_dataset = working_dataset_for_publish(
                &mut working_datasets,
                local_group,
                loaded_schemas,
                &row_id.dataset_id,
            )?;

            let encoded_operation = match mutation {
                RowMutation::Upsert { row, .. } => {
                    apply_local_upsert(working_dataset, &row_id, row, update_id)?
                }
                RowMutation::Delete { .. } => {
                    Some(apply_local_delete(working_dataset, &row_id, update_id)?)
                }
            };
            let Some(encoded_operation) = encoded_operation else {
                continue 'changes;
            };
            encoded_operations
                .entry(row_id.dataset_id.clone())
                .or_default()
                .push(encoded_operation);
        }

        ensure!(
            !encoded_operations.is_empty(),
            NoEffectiveChangesSnafu { group_id }
        );
        let dataset_updates = encoded_operations
            .into_iter()
            .map(|(dataset_id, operations)| DatasetUpdateMessage {
                dataset_id,
                operations,
            })
            .collect();
        Ok(PreparedDatasetUpdates {
            dataset_updates,
            working_datasets,
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
                ensure!(
                    members.contains(&self.local_member),
                    BootstrapMissingLocalMemberSnafu {
                        group_id,
                        local_member: self.local_member.clone(),
                    }
                );
                self.install_group(group_id, members)
                    .context(InstallBootstrapGroupSnafu { group_id })?;
                // Dropping `deliver` without completing `processed` intentionally
                // withholds the recipient acknowledgement from reliable delivery.
                // Sender-side timeout/retry semantics are tracked in flotsync-46r.
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

    async fn load_and_apply_update_batch(
        &mut self,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
        missing_dataset_ids: HashSet<DatasetId>,
    ) -> Result<Vec<Vec<RowChange>>, InboundDeliveryError> {
        let loaded_schemas = Self::load_dataset_schemas(self.store.clone(), missing_dataset_ids)
            .await
            .map_err(|error| match error {
                DatasetSchemaLoadError::Load { dataset_id, source } => {
                    InboundDeliveryError::InboundLoadDatasetSchema { dataset_id, source }
                }
                DatasetSchemaLoadError::Missing { dataset_id } => {
                    InboundDeliveryError::InboundMissingDatasetSchema { dataset_id }
                }
            })?;
        self.apply_wire_update_batch_loaded(sender, message, loaded_schemas)
    }

    fn handle_update_batch(
        &mut self,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
    ) -> Handled {
        let dataset_ids = Self::collect_dataset_ids(&message.dataset_updates);
        let missing_dataset_ids =
            if let Some(local_group) = self.state.groups.get(&message.group_id) {
                local_group.missing_dataset_ids(&dataset_ids)
            } else {
                let error = UnknownHostedGroupSnafu {
                    group_id: message.group_id,
                }
                .build();
                self.record_terminal_fault("group delivery", &error);
                return Handled::Ok;
            };
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .load_and_apply_update_batch(sender, message, missing_dataset_ids)
                .await;
            if let Ok(event_batches) = reply {
                if let Err(error) =
                    notify_listener_batches(async_self.listener.clone(), event_batches).await
                {
                    async_self.record_terminal_fault("group delivery", &error);
                }
            } else if let Err(error) = reply {
                // Temporary conservative failure mode for the
                // first replication slice. See flotsync-4x8
                // for relaxing transient inbound ordering
                // errors without terminating the runtime.
                async_self.record_terminal_fault("group delivery", &error);
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
        let member_count = self
            .state
            .groups
            .get(&group_id)
            .context(UnknownHostedGroupSnafu { group_id })?
            .member_count();
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
        let local_group =
            self.state
                .groups
                .get_mut(&message.group_id)
                .context(UnknownHostedGroupSnafu {
                    group_id: message.group_id,
                })?;
        let expected_sender_index =
            local_group
                .members
                .member_index(&sender)
                .context(UpdateSenderNotInGroupSnafu {
                    group_id: message.group_id,
                    sender: sender.clone(),
                })?;
        ensure!(
            expected_sender_index.as_u32() == message.update_id.node_index,
            UpdateSenderIndexMismatchSnafu {
                group_id: message.group_id,
                sender: sender.clone(),
                expected_index: expected_sender_index,
                actual_index: MemberIndex::new(message.update_id.node_index),
            }
        );
        if local_group.has_applied(message.update_id) {
            return Ok(Vec::new());
        }
        if !local_group.can_apply(&message) {
            local_group.buffer_update(BufferedInboundUpdate {
                message,
                loaded_schemas,
            })?;
            return Ok(Vec::new());
        }

        // Listener notifications are emitted only after the whole drain
        // succeeds. If one later buffered batch fails after an earlier batch
        // already committed locally, the listener can temporarily lag the
        // runtime state. Tracked in flotsync-46i.
        let mut event_batches = Vec::new();
        let row_changes = apply_one_update_batch(local_group, message, &loaded_schemas)?;
        if !row_changes.is_empty() {
            event_batches.push(row_changes);
        }
        while let Some(pending_update) = local_group.take_next_actionable_pending_update() {
            let row_changes = apply_one_update_batch(
                local_group,
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

        let local_group = LocalGroupState::new(&self.local_member, members)?;
        self.state.groups.insert(group_id, local_group);
        self.group_memberships
            .replace(self.state.membership_snapshot());
        Ok(())
    }

    fn handle_publish_changes(
        &mut self,
        ask: Ask<Vec<RowMutation>, Result<PublishReceipt, ApiError>>,
    ) -> Handled {
        let (promise, changes) = ask.take();
        let missing_dataset_ids = match self
            .ensure_running()
            .and_then(|()| self.preflight_publish(&changes))
        {
            Ok(missing_dataset_ids) => missing_dataset_ids,
            Err(error) => {
                self.reply_api(promise, "publish_changes", Err(error));
                return Handled::Ok;
            }
        };
        Handled::block_on(self, async move |mut async_self| {
            let reply =
                match Self::load_dataset_schemas(async_self.store.clone(), missing_dataset_ids)
                    .await
                    .map_err(|error| match error {
                        DatasetSchemaLoadError::Load { dataset_id, source } => {
                            PublishChangesError::LoadDatasetSchema { dataset_id, source }
                        }
                        DatasetSchemaLoadError::Missing { dataset_id } => {
                            PublishChangesError::MissingDatasetSchema { dataset_id }
                        }
                    }) {
                    Ok(loaded_schemas) => {
                        let prepared_publish = async_self
                            .prepare_local_publish(changes, &loaded_schemas)
                            .boxed()
                            .context(ApiExternalSnafu);
                        match prepared_publish {
                            Ok(prepared_publish) => {
                                let update_id = prepared_publish.update_id;
                                async_self.submit_group_update(prepared_publish);
                                Ok(PublishReceipt { update_id })
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err(ApiError::ApiExternal {
                        source: Box::new(error),
                    }),
                };
            async_self.reply_api(promise, "publish_changes", reply);
        })
    }

    fn handle_create_group(
        &mut self,
        ask: Ask<CreateGroupRequest, Result<GroupId, ApiError>>,
    ) -> Handled {
        let (promise, req) = ask.take();
        let reply = self
            .ensure_running()
            .and_then(|()| self.create_group(req).boxed().context(ApiExternalSnafu));
        self.reply_api(promise, "create_group", reply);
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
            .and_then(|()| Err(unavailable_api("change_group_membership")));
        self.reply_api(promise, "change_group_membership", reply);
        Handled::Ok
    }

    #[cfg(test)]
    fn handle_test_install_group(
        &mut self,
        ask: Ask<(GroupId, GroupMembers), Result<(), GroupInstallError>>,
    ) -> Handled {
        let (promise, (group_id, members)) = ask.take();
        let reply = self.install_group(group_id, members);
        let _ = promise.fulfil(reply);
        Handled::Ok
    }

    #[cfg(test)]
    fn handle_test_apply_update_batch(
        &mut self,
        ask: Ask<(MemberIdentity, UpdateBatchMessage), Result<(), InboundDeliveryError>>,
    ) -> Handled {
        let (promise, (sender, message)) = ask.take();
        let dataset_ids = Self::collect_dataset_ids(&message.dataset_updates);
        let missing_dataset_ids =
            if let Some(local_group) = self.state.groups.get(&message.group_id) {
                local_group.missing_dataset_ids(&dataset_ids)
            } else {
                let error = UnknownHostedGroupSnafu {
                    group_id: message.group_id,
                }
                .build();
                let _ = promise.fulfil(Err(error));
                return Handled::Ok;
            };
        Handled::block_on(self, async move |mut async_self| {
            let reply = match async_self
                .load_and_apply_update_batch(
                    sender,
                    WireUpdateBatchMessage::from(message),
                    missing_dataset_ids,
                )
                .await
            {
                Ok(event_batches) => {
                    notify_listener_batches(async_self.listener.clone(), event_batches).await
                }
                Err(error) => Err(error),
            };
            let _ = promise.fulfil(reply);
        })
    }
}

ignore_lifecycle!(ReplicationRuntimeComponent);

impl Require<ReliableDeliveryPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> Handled {
        if self.terminal_fault.is_some() {
            return Handled::Ok;
        }
        let ReliableDeliveryPortIndication::Deliver(deliver) = indication;
        if let Err(error) = self.handle_reliable_delivery(deliver) {
            self.record_terminal_fault("reliable delivery", &error);
        }
        Handled::Ok
    }
}

impl Require<GroupBroadcastPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: GroupBroadcastPortIndication) -> Handled {
        if self.terminal_fault.is_some() {
            return Handled::Ok;
        }
        let GroupBroadcastPortIndication::Deliver(deliver) = indication;
        match self.handle_group_delivery(deliver) {
            Ok(handled) => handled,
            Err(error) => {
                self.record_terminal_fault("group delivery", &error);
                Handled::Ok
            }
        }
    }
}

impl Actor for ReplicationRuntimeComponent {
    type Message = ReplicationRuntimeMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            ReplicationRuntimeMessage::PublishChanges(ask) => self.handle_publish_changes(ask),
            ReplicationRuntimeMessage::CreateGroup(ask) => self.handle_create_group(ask),
            ReplicationRuntimeMessage::ChangeGroupMembership(ask) => {
                self.handle_change_group_membership(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeMessage::Test(ReplicationRuntimeTestMessage::InstallGroup(ask)) => {
                self.handle_test_install_group(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeMessage::Test(ReplicationRuntimeTestMessage::ApplyUpdateBatch(
                ask,
            )) => self.handle_test_apply_update_batch(ask),
        }
    }
}

fn unavailable_api(operation: &'static str) -> ApiError {
    ApiError::UnsupportedOperation { operation }
}

fn placeholder_signed_footer() -> SignedEnvelopeFooter {
    SignedEnvelopeFooter {
        signature: DetachedSignature {
            scheme: SignatureScheme::Ed25519,
            bytes: bytes::Bytes::from_static(b"runtime-placeholder-signature"),
        },
    }
}

async fn notify_listener_batches(
    listener: Arc<dyn ReplicationEventListener>,
    event_batches: Vec<Vec<RowChange>>,
) -> Result<(), InboundDeliveryError> {
    for row_changes in event_batches {
        listener
            .on_event(ReplicationEvent::DataChanged {
                rows: Box::new(VecRowProvider::new(row_changes)),
            })
            .await
            .map_err(|source| InboundDeliveryError::NotifyListener { source })?;
    }
    Ok(())
}

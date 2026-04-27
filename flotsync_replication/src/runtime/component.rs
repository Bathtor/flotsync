use super::{
    errors::{inbound, publish, *},
    in_memory::{
        LoadedGroupMeta,
        LocalDataset,
        PendingUpdateSet,
        PreparedLocalChanges,
        TouchedGroupRows,
        apply_local_delete,
        apply_local_upsert,
        apply_one_update_batch,
        collect_group_row_scope,
        collect_record_row_scope,
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
    GroupMemberships,
    SharedGroupMemberships,
    api::{
        ApiError,
        ApiExternalSnafu,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DatasetId,
        DatasetRowPatch,
        DatasetRowWrite,
        DatasetUpdateRecord,
        GroupId,
        GroupMigration,
        MemberIdentity,
        MemberIndex,
        PublishReceipt,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationGroupRecord,
        ReplicationStore,
        ReplicationStoreTransaction,
        ReplicationUpdateFilter,
        ReplicationUpdateRecord,
        RowChange,
        RowKey,
        RowMutation,
        SchemaSource,
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
use flotsync_messages::buffa::Message as BuffaMessage;
use kompact::prelude::*;
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};
use uuid::Uuid;

/// One local publish batch after local apply, encoding, and delivery-envelope preparation.
struct PreparedLocalPublish {
    group_id: GroupId,
    update_id: UpdateId,
    payload: bytes::Bytes,
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

/// Envelope metadata included in inbound delivery fault logs.
#[derive(Clone, Debug)]
enum InboundDeliveryContext {
    /// Recipient-addressed envelope handed over by reliable delivery.
    Reliable {
        sender: MemberIdentity,
        recipient: MemberIdentity,
        message_id: MessageId,
    },
    /// Group-scoped envelope handed over by group broadcast.
    Group {
        group_id: GroupId,
        sender: MemberIdentity,
        message_id: MessageId,
    },
}

impl InboundDeliveryContext {
    fn group(header: &GroupMessageHeader) -> Self {
        Self::Group {
            group_id: header.group_id,
            sender: header.sender.clone(),
            message_id: header.message_id,
        }
    }

    fn reliable(header: &ReliableMessageHeader) -> Self {
        Self::Reliable {
            sender: header.sender.clone(),
            recipient: header.recipient.clone(),
            message_id: header.message_id,
        }
    }
}

impl std::fmt::Display for InboundDeliveryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reliable {
                sender,
                recipient,
                message_id,
            } => write!(
                f,
                "reliable delivery message {message_id} from {sender} to {recipient}"
            ),
            Self::Group {
                group_id,
                sender,
                message_id,
            } => write!(
                f,
                "group broadcast message {message_id} for group {group_id} from {sender}"
            ),
        }
    }
}

/// Inbound delivery failure paired with the envelope that caused it.
struct InboundDeliveryFailure {
    context: InboundDeliveryContext,
    error: Box<InboundDeliveryError>,
}

impl InboundDeliveryFailure {
    fn new(context: InboundDeliveryContext, error: InboundDeliveryError) -> Self {
        Self {
            context,
            error: Box::new(error),
        }
    }
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
    /// Confirm that the runtime component is alive and able to process one
    /// mailbox turn after startup.
    Ping(Ask<(), ()>),
    InstallGroup(Ask<(GroupId, GroupMembers), Result<(), GroupInstallError>>),
    ApplyUpdateBatch(Ask<(MemberIdentity, UpdateBatchMessage), Result<(), InboundDeliveryError>>),
}

#[cfg(test)]
impl ReplicationRuntimeMessage {
    pub(super) fn test_ping(promise: KPromise<()>) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::Ping(Ask::new(promise, ())))
    }

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

/// Stateful Kompact component that hosts one replication runtime.
///
/// Durable replication state now lives entirely in `ReplicationStore`. This
/// component only owns membership-routing snapshots plus the imperative bridge
/// from public API calls and delivery events into transactional store work.
#[derive(ComponentDefinition)]
pub struct ReplicationRuntimeComponent {
    ctx: ComponentContext<Self>,
    group_broadcast: RequiredPort<GroupBroadcastPort>,
    reliable_delivery: RequiredPort<ReliableDeliveryPort>,
    local_member: MemberIdentity,
    store: Arc<dyn ReplicationStore>,
    listener: Arc<dyn ReplicationEventListener>,
    group_memberships: SharedGroupMemberships,
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
        }
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

    fn record_inbound_failure(&self, failure: &InboundDeliveryFailure) -> InboundFailureAction {
        let action = failure.error.failure_action();
        match action {
            InboundFailureAction::Drop => {
                warn!(
                    self.log(),
                    "dropping inbound {} after recoverable error: {}",
                    failure.context,
                    failure.error
                );
            }
            InboundFailureAction::Fatal => {
                error!(
                    self.log(),
                    "fatal inbound {} failure: {}", failure.context, failure.error
                );
            }
        }
        action
    }

    async fn load_dataset_schemas<I>(
        store: Arc<dyn ReplicationStore>,
        dataset_ids: I,
    ) -> Result<HashMap<DatasetId, SchemaSource>, DatasetSchemaLoadError>
    where
        I: IntoIterator<Item = DatasetId>,
    {
        let mut loaded_schemas = HashMap::new();
        for dataset_id in dataset_ids {
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
            loaded_schemas.insert(dataset_id, schema);
        }
        Ok(loaded_schemas)
    }

    /// Materialise one ephemeral in-memory dataset slice for each touched
    /// dataset using only the requested row keys.
    async fn materialise_touched_datasets(
        transaction: &mut dyn ReplicationStoreTransaction,
        schemas: &HashMap<DatasetId, SchemaSource>,
        group_id: GroupId,
        dataset_rows: &HashMap<DatasetId, HashSet<RowKey>>,
    ) -> Result<HashMap<DatasetId, LocalDataset>, StoreError> {
        let mut datasets = HashMap::with_capacity(dataset_rows.len());
        for (dataset_id, row_keys) in dataset_rows {
            let mut row_keys = row_keys.iter();
            let row_slice = transaction
                .load_dataset_rows(&group_id, dataset_id, &mut row_keys)
                .await?;
            let schema = schemas
                .get(dataset_id)
                .expect("touched dataset schemas must be pre-loaded");
            datasets.insert(
                dataset_id.clone(),
                LocalDataset::from_row_slice(schema.clone(), row_slice),
            );
        }
        Ok(datasets)
    }

    /// Persist one set of explicit row patches back into durable storage.
    async fn apply_dataset_row_patches(
        transaction: &mut dyn ReplicationStoreTransaction,
        patches: Vec<DatasetRowPatch>,
    ) -> Result<(), StoreError> {
        for patch in patches {
            transaction.apply_dataset_row_patch(patch).await?;
        }
        Ok(())
    }

    /// Build the canonical persisted record for one group definition that has
    /// already passed local membership validation.
    fn build_replication_group_record(
        &self,
        group_id: GroupId,
        members: &GroupMembers,
    ) -> ReplicationGroupRecord {
        let member_count = NonZeroUsize::new(members.len())
            .expect("group installation must keep members non-empty");
        let local_member_index = members
            .member_index(&self.local_member)
            .expect("group installation validates the local member before persistence");
        ReplicationGroupRecord {
            group_id,
            members: members.ordered_members(),
            local_member_index,
            version_vector: VersionVector::initial(member_count),
        }
    }

    /// Persist one newly observed group definition.
    ///
    /// If the same group already exists with the same canonical membership
    /// definition, this returns the stored record without mutating it.
    async fn store_new_replication_group(
        &mut self,
        record: ReplicationGroupRecord,
    ) -> Result<ReplicationGroupRecord, GroupInstallError> {
        let group_id = record.group_id;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(StoreGroupSnafu { group_id })?;
        let existing = transaction
            .load_replication_group(&group_id)
            .await
            .context(StoreGroupSnafu { group_id })?;
        if let Some(existing) = existing {
            transaction
                .commit()
                .await
                .context(StoreGroupSnafu { group_id })?;
            ensure!(
                existing.members == record.members
                    && existing.local_member_index == record.local_member_index,
                ConflictingExistingGroupSnafu { group_id }
            );
            return Ok(existing);
        }

        transaction
            .insert_replication_group(record.clone())
            .await
            .context(StoreGroupSnafu { group_id })?;
        transaction
            .commit()
            .await
            .context(StoreGroupSnafu { group_id })?;
        Ok(record)
    }

    /// Install one validated group view into the shared delivery-membership
    /// snapshot.
    fn install_group_membership_view(
        &mut self,
        group: ReplicationGroupRecord,
    ) -> Result<(), GroupInstallError> {
        let group_id = group.group_id;
        let local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, group)?;

        let mut memberships = self.group_memberships.snapshot().as_ref().clone();
        let existing_members = memberships.insert(group_id, local_group.members.clone());
        if let Some(existing_members) = existing_members {
            ensure!(
                existing_members == local_group.members,
                ConflictingExistingGroupSnafu { group_id }
            );
            return Ok(());
        }
        self.group_memberships.replace(memberships);
        Ok(())
    }

    /// Load the persisted group registry into the shared membership snapshot during
    /// component startup.
    async fn load_hydrated_runtime_memberships(
        &mut self,
    ) -> Result<GroupMemberships, RuntimeStartupError> {
        let initial_memberships = self.group_memberships.snapshot().as_ref().clone();
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(StoreStartupSnafu)?;
        let persisted_groups = transaction
            .load_replication_groups()
            .await
            .context(StoreStartupSnafu)?;
        transaction.commit().await.context(StoreStartupSnafu)?;

        let mut memberships = initial_memberships;
        for persisted_group in persisted_groups {
            let group_id = persisted_group.group_id;
            let local_group =
                LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                    .context(InvalidGroupSnafu { group_id })?;
            let existing_members = memberships.insert(group_id, local_group.members.clone());
            if existing_members.is_some() {
                return DuplicateGroupSnafu { group_id }.fail();
            }
        }

        Ok(memberships)
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

    /// Validate one create-group request and derive the canonical persisted
    /// group record that should be written if the request succeeds.
    fn prepare_created_group(
        &self,
        req: CreateGroupRequest,
    ) -> Result<(GroupId, GroupMembers, ReplicationGroupRecord), CreateGroupError> {
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
        let record = self.build_replication_group_record(group_id, &members);
        Ok((group_id, members, record))
    }

    fn next_local_update_id(
        local_group: &LoadedGroupMeta,
        group_id: GroupId,
    ) -> Result<UpdateId, PublishChangesError> {
        let next_local_version = local_group
            .applied_version(local_group.local_member_index)
            .checked_add(1)
            .context(publish::ExhaustedUpdateIdsSnafu { group_id })?;
        Ok(UpdateId {
            version: next_local_version,
            node_index: local_group.local_member_index.as_u32(),
        })
    }

    fn build_local_dataset_updates(
        group_id: GroupId,
        working_datasets: &mut HashMap<DatasetId, LocalDataset>,
        changes: Vec<RowMutation>,
        update_id: UpdateId,
    ) -> Result<PreparedLocalChanges, PublishChangesError> {
        let mut encoded_operations: HashMap<
            DatasetId,
            Vec<flotsync_messages::datamodel::SchemaOperation>,
        > = HashMap::new();
        let mut row_writes: HashMap<DatasetId, Vec<DatasetRowWrite>> = HashMap::new();

        'changes: for mutation in changes {
            let row_id = mutation.row_id().clone();
            let working_dataset =
                working_dataset_for_publish(working_datasets, &row_id.dataset_id)?;

            let applied_operation = match mutation {
                RowMutation::Upsert { row, .. } => {
                    apply_local_upsert(working_dataset, &row_id, row, update_id)?
                }
                RowMutation::Delete { .. } => {
                    Some(apply_local_delete(working_dataset, &row_id, update_id)?)
                }
            };
            let Some(applied_operation) = applied_operation else {
                continue 'changes;
            };
            encoded_operations
                .entry(row_id.dataset_id.clone())
                .or_default()
                .push(applied_operation.encoded_operation);
            row_writes
                .entry(row_id.dataset_id.clone())
                .or_default()
                .push(applied_operation.row_write);
        }

        ensure!(
            !encoded_operations.is_empty(),
            publish::NoEffectiveChangesSnafu { group_id }
        );
        let dataset_updates = encoded_operations
            .into_iter()
            .map(|(dataset_id, operations)| DatasetUpdateRecord {
                dataset_id,
                operations,
            })
            .collect();
        let row_patches = row_writes
            .into_iter()
            .map(|(dataset_id, actions)| DatasetRowPatch {
                group_id,
                dataset_id,
                actions,
            })
            .collect();
        Ok(PreparedLocalChanges {
            dataset_updates,
            row_patches,
        })
    }

    /// Convert one runtime update message into the durable update-record shape.
    fn build_replication_update_record(
        sender: MemberIdentity,
        message: UpdateBatchMessage,
        applied_locally: bool,
    ) -> ReplicationUpdateRecord {
        ReplicationUpdateRecord {
            group_id: message.group_id,
            update_id: message.update_id,
            sender,
            read_versions: message.read_versions,
            dataset_updates: message
                .dataset_updates
                .into_iter()
                .map(|dataset_update| DatasetUpdateRecord {
                    dataset_id: dataset_update.dataset_id,
                    operations: dataset_update.operations,
                })
                .collect(),
            applied_locally,
        }
    }

    /// Publish one local change batch through one durable store transaction.
    async fn publish_changes_transactionally(
        &mut self,
        changes: Vec<RowMutation>,
    ) -> Result<PreparedLocalPublish, PublishChangesError> {
        let TouchedGroupRows {
            group_id,
            dataset_rows,
        } = collect_group_row_scope(&changes)?;
        let loaded_schemas =
            Self::load_dataset_schemas(self.store.clone(), dataset_rows.keys().cloned())
                .await
                .map_err(|error| match error {
                    DatasetSchemaLoadError::Load { dataset_id, source } => {
                        PublishChangesError::LoadDatasetSchema { dataset_id, source }
                    }
                    DatasetSchemaLoadError::Missing { dataset_id } => {
                        PublishChangesError::MissingDatasetSchema { dataset_id }
                    }
                })?;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(publish::StoreAccessSnafu)?;
        let persisted_group = transaction
            .load_replication_group(&group_id)
            .await
            .context(publish::StoreAccessSnafu)?;
        let persisted_group = persisted_group.context(publish::UnknownGroupSnafu { group_id })?;
        let mut local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                .context(publish::InvalidPersistedGroupSnafu { group_id })?;
        let mut working_datasets = Self::materialise_touched_datasets(
            transaction.as_mut(),
            &loaded_schemas,
            group_id,
            &dataset_rows,
        )
        .await
        .context(publish::StoreAccessSnafu)?;
        let read_versions = local_group.version_vector.clone();
        let update_id = Self::next_local_update_id(&local_group, group_id)?;
        let prepared_local_changes =
            Self::build_local_dataset_updates(group_id, &mut working_datasets, changes, update_id)?;
        local_group.mark_applied(update_id);

        let persisted_update = ReplicationUpdateRecord {
            group_id,
            update_id,
            sender: self.local_member.clone(),
            read_versions: read_versions.clone(),
            dataset_updates: prepared_local_changes.dataset_updates.clone(),
            applied_locally: true,
        };
        Self::apply_dataset_row_patches(transaction.as_mut(), prepared_local_changes.row_patches)
            .await
            .context(publish::StoreAccessSnafu)?;
        transaction
            .append_replication_update(persisted_update)
            .await
            .context(publish::StoreAccessSnafu)?;
        transaction
            .update_replication_group_version_vector(&group_id, local_group.version_vector)
            .await
            .context(publish::StoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(publish::StoreAccessSnafu)?;

        let payload =
            Self::encode_runtime_payload(RuntimeMessage::UpdateBatch(UpdateBatchMessage {
                group_id,
                update_id,
                read_versions,
                dataset_updates: prepared_local_changes
                    .dataset_updates
                    .into_iter()
                    .map(|dataset_update| DatasetUpdateMessage {
                        dataset_id: dataset_update.dataset_id,
                        operations: dataset_update.operations,
                    })
                    .collect(),
            }));
        Ok(PreparedLocalPublish {
            group_id,
            update_id,
            payload,
        })
    }

    fn handle_reliable_delivery(
        &mut self,
        deliver: ReliableDeliveryDeliver,
    ) -> Result<Handled, InboundDeliveryFailure> {
        let context = InboundDeliveryContext::reliable(&deliver.envelope.header);
        let message =
            match WireRuntimeMessage::decode_from_slice(&deliver.envelope.payload.ciphertext)
                .context(inbound::DecodeMessageSnafu)
            {
                Ok(message) => message,
                Err(error) => return Err(InboundDeliveryFailure::new(context, error)),
            };
        match message {
            WireRuntimeMessage::BootstrapGroup(message) => {
                let group_id = message.group_id;
                let members = match GroupMembers::from_ordered_members(message.members)
                    .context(inbound::InvalidBootstrapMembersSnafu)
                {
                    Ok(members) => members,
                    Err(error) => return Err(InboundDeliveryFailure::new(context, error)),
                };
                if !members.contains(&self.local_member) {
                    return Err(InboundDeliveryFailure::new(
                        context,
                        InboundDeliveryError::BootstrapMissingLocalMember {
                            group_id,
                            local_member: self.local_member.clone(),
                        },
                    ));
                }
                let record = self.build_replication_group_record(group_id, &members);
                Ok(Handled::block_on(self, async move |mut async_self| {
                    let reply = async {
                        let persisted_group = async_self
                            .store_new_replication_group(record)
                            .await
                            .context(inbound::InstallBootstrapGroupSnafu { group_id })?;
                        async_self
                            .install_group_membership_view(persisted_group)
                            .context(inbound::InstallBootstrapGroupSnafu { group_id })?;
                        // Complete `processed` only after the bootstrap group
                        // is durably installed locally. Failure paths
                        // intentionally withhold this completion, so the
                        // sender-side recipient-ack timeout can redeliver the
                        // same reliable message later.
                        deliver
                            .processed
                            .complete()
                            .context(inbound::CompleteProcessedPromiseSnafu { group_id })?;
                        Ok::<(), InboundDeliveryError>(())
                    }
                    .await;
                    if let Err(error) = reply {
                        let failure = InboundDeliveryFailure::new(context, error);
                        let action = async_self.record_inbound_failure(&failure);
                        panic_if_fatal_inbound_failure(action, &failure);
                    }
                }))
            }
            WireRuntimeMessage::UpdateBatch(_) => Err(InboundDeliveryFailure::new(
                context,
                InboundDeliveryError::UnexpectedReliableMessage,
            )),
        }
    }

    fn handle_group_delivery(
        &mut self,
        deliver: GroupBroadcastDeliver,
    ) -> Result<Handled, InboundDeliveryFailure> {
        let context = InboundDeliveryContext::group(&deliver.envelope.header);
        let sender = deliver.envelope.header.sender.clone();
        let message =
            match WireRuntimeMessage::decode_from_slice(&deliver.envelope.payload.ciphertext)
                .context(inbound::DecodeMessageSnafu)
            {
                Ok(message) => message,
                Err(error) => return Err(InboundDeliveryFailure::new(context, error)),
            };
        match message {
            WireRuntimeMessage::BootstrapGroup(_) => Err(InboundDeliveryFailure::new(
                context,
                InboundDeliveryError::UnexpectedGroupMessage,
            )),
            WireRuntimeMessage::UpdateBatch(message) => {
                Ok(self.handle_update_batch(context, sender, message))
            }
        }
    }

    /// Persist one inbound update and apply every newly-ready successor inside
    /// the same store transaction.
    async fn persist_and_apply_update_batch(
        &mut self,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
    ) -> Result<Vec<Vec<RowChange>>, InboundDeliveryError> {
        let group_id = message.group_id;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let persisted_group = transaction
            .load_replication_group(&group_id)
            .await
            .context(inbound::StoreAccessSnafu)?;
        let persisted_group =
            persisted_group.context(inbound::UnknownHostedGroupSnafu { group_id })?;
        let local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                .context(inbound::InvalidPersistedGroupSnafu { group_id })?;
        let message = message
            .into_runtime(local_group.member_count())
            .context(inbound::DecodeReadVersionsSnafu { group_id })?;
        let expected_sender_index = local_group.members.member_index(&sender).context(
            inbound::UpdateSenderNotInGroupSnafu {
                group_id,
                sender: sender.clone(),
            },
        )?;
        ensure!(
            expected_sender_index.as_u32() == message.update_id.node_index,
            inbound::UpdateSenderIndexMismatchSnafu {
                group_id,
                sender: sender.clone(),
                expected_index: expected_sender_index,
                actual_index: MemberIndex::new(message.update_id.node_index),
            }
        );
        if local_group.has_applied(message.update_id) {
            transaction
                .commit()
                .await
                .context(inbound::StoreAccessSnafu)?;
            return Ok(Vec::new());
        }

        let inbound_update = Self::build_replication_update_record(sender, message, false);
        if let Some(existing_update) = transaction
            .load_replication_update(&group_id, inbound_update.update_id)
            .await
            .context(inbound::StoreAccessSnafu)?
        {
            ensure!(
                existing_update == inbound_update,
                inbound::ConflictingPersistedUpdateSnafu {
                    group_id,
                    update_id: inbound_update.update_id,
                }
            );
        } else {
            transaction
                .append_replication_update(inbound_update.clone())
                .await
                .context(inbound::StoreAccessSnafu)?;
        }

        let pending_updates = transaction
            .load_replication_updates(&group_id, ReplicationUpdateFilter::PendingApply)
            .await
            .context(inbound::StoreAccessSnafu)?;
        let mut pending_updates = PendingUpdateSet::from_updates(pending_updates);
        let mut local_group = local_group;
        let apply_plan = pending_updates.plan_apply_chain(&local_group);
        for applied_update_id in &apply_plan.already_applied {
            transaction
                .mark_replication_update_applied(&group_id, *applied_update_id)
                .await
                .context(inbound::StoreAccessSnafu)?;
        }
        if apply_plan.ready_chain.is_empty() {
            transaction
                .commit()
                .await
                .context(inbound::StoreAccessSnafu)?;
            return Ok(Vec::new());
        }

        let touched_dataset_ids = apply_plan
            .ready_chain
            .iter()
            .flat_map(|update| update.dataset_updates.iter())
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect::<HashSet<_>>();
        let loaded_schemas = Self::load_dataset_schemas(self.store.clone(), touched_dataset_ids)
            .await
            .map_err(|error| match error {
                DatasetSchemaLoadError::Load { dataset_id, source } => {
                    InboundDeliveryError::LoadDatasetSchema { dataset_id, source }
                }
                DatasetSchemaLoadError::Missing { dataset_id } => {
                    InboundDeliveryError::MissingDatasetSchema { dataset_id }
                }
            })?;
        let touched_dataset_rows =
            collect_record_row_scope(&apply_plan.ready_chain, &loaded_schemas)?;
        let mut working_datasets = Self::materialise_touched_datasets(
            transaction.as_mut(),
            &loaded_schemas,
            group_id,
            &touched_dataset_rows,
        )
        .await
        .context(inbound::StoreAccessSnafu)?;
        let mut event_batches = Vec::new();
        for ready_update in &apply_plan.ready_chain {
            let applied_batch =
                apply_one_update_batch(&mut local_group, &mut working_datasets, ready_update)?;
            if !applied_batch.row_changes.is_empty() {
                event_batches.push(applied_batch.row_changes);
            }
            Self::apply_dataset_row_patches(transaction.as_mut(), applied_batch.row_patches)
                .await
                .context(inbound::StoreAccessSnafu)?;
            transaction
                .mark_replication_update_applied(&group_id, ready_update.update_id)
                .await
                .context(inbound::StoreAccessSnafu)?;
        }
        transaction
            .update_replication_group_version_vector(&group_id, local_group.version_vector)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)?;
        Ok(event_batches)
    }

    fn handle_update_batch(
        &mut self,
        context: InboundDeliveryContext,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
    ) -> Handled {
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .persist_and_apply_update_batch(sender, message)
                .await;
            let error = match reply {
                Ok(event_batches) => {
                    notify_listener_batches(async_self.listener.clone(), event_batches)
                        .await
                        .err()
                }
                Err(error) => Some(error),
            };
            if let Some(error) = error {
                let failure = InboundDeliveryFailure::new(context, error);
                let action = async_self.record_inbound_failure(&failure);
                panic_if_fatal_inbound_failure(action, &failure);
            }
        })
    }

    fn handle_publish_changes(
        &mut self,
        ask: Ask<Vec<RowMutation>, Result<PublishReceipt, ApiError>>,
    ) -> Handled {
        let (promise, changes) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = match async_self.publish_changes_transactionally(changes).await {
                Ok(prepared_publish) => {
                    let update_id = prepared_publish.update_id;
                    async_self.submit_group_update(prepared_publish);
                    Ok(PublishReceipt { update_id })
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
        let created_group = self.prepare_created_group(req);
        let (group_id, members, record) = match created_group {
            Ok(created_group) => created_group,
            Err(error) => {
                let reply = Err(ApiError::ApiExternal {
                    source: Box::new(error),
                });
                self.reply_api(promise, "create_group", reply);
                return Handled::Ok;
            }
        };
        Handled::block_on(self, async move |mut async_self| {
            let persisted_group = async_self.store_new_replication_group(record).await;
            let reply = match persisted_group {
                Ok(persisted_group) => {
                    let install_result = async_self.install_group_membership_view(persisted_group);
                    let reply = install_result.map(|()| {
                        async_self.submit_group_bootstrap(group_id, &members);
                        group_id
                    });
                    reply.boxed().context(ApiExternalSnafu)
                }
                Err(error) => Err(ApiError::ApiExternal {
                    source: Box::new(error),
                }),
            };
            async_self.reply_api(promise, "create_group", reply);
        })
    }

    fn handle_change_group_membership(
        &mut self,
        ask: Ask<ChangeGroupMembershipRequest, Result<GroupMigration, ApiError>>,
    ) -> Handled {
        let (promise, req) = ask.take();
        let _ = req;
        let reply = Err(unavailable_api("change_group_membership"));
        self.reply_api(promise, "change_group_membership", reply);
        Handled::Ok
    }

    #[cfg(test)]
    fn handle_test_install_group(
        &mut self,
        ask: Ask<(GroupId, GroupMembers), Result<(), GroupInstallError>>,
    ) -> Handled {
        let (promise, (group_id, members)) = ask.take();
        let record = self.build_replication_group_record(group_id, &members);
        Handled::block_on(self, async move |mut async_self| {
            let persisted_group = async_self.store_new_replication_group(record).await;
            let reply = match persisted_group {
                Ok(persisted_group) => async_self.install_group_membership_view(persisted_group),
                Err(error) => Err(error),
            };
            let _ = promise.fulfil(reply);
        })
    }

    #[cfg(test)]
    fn handle_test_ping(&mut self, ask: Ask<(), ()>) -> Handled {
        let (promise, ()) = ask.take();
        let _ = promise.fulfil(());
        Handled::Ok
    }

    #[cfg(test)]
    fn handle_test_apply_update_batch(
        &mut self,
        ask: Ask<(MemberIdentity, UpdateBatchMessage), Result<(), InboundDeliveryError>>,
    ) -> Handled {
        let (promise, (sender, message)) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = match async_self
                .persist_and_apply_update_batch(sender, WireUpdateBatchMessage::from(message))
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

impl ComponentLifecycle for ReplicationRuntimeComponent {
    fn on_start(&mut self) -> Handled {
        Handled::block_on(self, async move |mut async_self| {
            let hydrated_memberships = async_self.load_hydrated_runtime_memberships().await;
            match hydrated_memberships {
                Ok(hydrated_memberships) => {
                    async_self.group_memberships.replace(hydrated_memberships);
                }
                Err(error) => {
                    error!(
                        async_self.log(),
                        "replication runtime startup failed: {error}"
                    );
                    panic!("replication runtime startup failed: {error}");
                }
            }
        })
    }

    fn on_stop(&mut self) -> Handled {
        Handled::Ok
    }

    fn on_kill(&mut self) -> Handled {
        Handled::Ok
    }
}

impl Require<ReliableDeliveryPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> Handled {
        let ReliableDeliveryPortIndication::Deliver(deliver) = indication;
        match self.handle_reliable_delivery(deliver) {
            Ok(handled) => handled,
            Err(failure) => {
                let action = self.record_inbound_failure(&failure);
                handled_after_inbound_failure(action, &failure)
            }
        }
    }
}

impl Require<GroupBroadcastPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: GroupBroadcastPortIndication) -> Handled {
        let GroupBroadcastPortIndication::Deliver(deliver) = indication;
        match self.handle_group_delivery(deliver) {
            Ok(handled) => handled,
            Err(failure) => {
                let action = self.record_inbound_failure(&failure);
                handled_after_inbound_failure(action, &failure)
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
            ReplicationRuntimeMessage::Test(ReplicationRuntimeTestMessage::Ping(ask)) => {
                self.handle_test_ping(ask)
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

fn panic_if_fatal_inbound_failure(action: InboundFailureAction, failure: &InboundDeliveryFailure) {
    if matches!(action, InboundFailureAction::Fatal) {
        panic!(
            "fatal inbound {} failure: {}",
            failure.context, failure.error
        );
    }
}

fn handled_after_inbound_failure(
    action: InboundFailureAction,
    failure: &InboundDeliveryFailure,
) -> Handled {
    panic_if_fatal_inbound_failure(action, failure);
    Handled::Ok
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ListenerError;

    #[test]
    fn inbound_error_classification_matches_manual_slice_policy() {
        let group_id = GroupId(Uuid::from_u128(91));
        let update_id = UpdateId {
            version: 1,
            node_index: 0,
        };

        assert_eq!(
            InboundDeliveryError::UnknownHostedGroup { group_id }.failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::UnexpectedGroupMessage.failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::ConflictingPersistedUpdate {
                group_id,
                update_id,
            }
            .failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::NotifyListener {
                source: ListenerError::Rejected {
                    message: "listener closed permanently".to_owned(),
                },
            }
            .failure_action(),
            InboundFailureAction::Fatal
        );
    }
}

use super::{
    errors::*,
    in_memory::{
        LoadedGroupMeta,
        LocalDataset,
        PendingUpdateSet,
        apply_local_delete,
        apply_local_upsert,
        apply_one_update_batch,
        build_dataset_snapshot_write,
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
    GroupMemberships,
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
        ReplicationGroupRecord,
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
    num::NonZeroUsize,
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

enum DatasetSchemaLoadError {
    Load {
        dataset_id: DatasetId,
        source: StoreError,
    },
    Missing {
        dataset_id: DatasetId,
    },
}

/// One transaction-scoped dataset load result for the currently touched ids.
///
/// `datasets` contains fully materialised working copies for dataset ids that
/// already had durable snapshot state. `missing_dataset_ids` tracks datasets
/// that do not yet exist in durable storage and therefore still need to be
/// seeded from the application schema catalogue.
struct MaterialisedDatasets {
    datasets: HashMap<DatasetId, LocalDataset>,
    missing_dataset_ids: HashSet<DatasetId>,
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

    fn install_terminal_fault(&mut self, operation: &'static str, message: String) {
        if self.terminal_fault.is_some() {
            return;
        }
        let fault = TerminalRuntimeFault { operation, message };
        error!(self.log(), "terminal runtime failure: {}", fault);
        self.terminal_fault = Some(fault);
    }

    fn record_terminal_fault(&mut self, operation: &'static str, error: &InboundDeliveryError) {
        self.install_terminal_fault(operation, error.to_string());
        self.ctx.suicide();
    }

    fn record_startup_fault(&mut self, operation: &'static str, error: &RuntimeStartupError) {
        self.install_terminal_fault(operation, error.to_string());
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

    /// Materialise the committed state for every touched dataset id.
    ///
    /// This currently loads full dataset images into memory because the store
    /// boundary only exposes whole-dataset snapshots. `flotsync-i5b` tracks the
    /// planned follow-up to reduce this to row-granular transactional loading.
    async fn materialise_touched_datasets(
        transaction: &mut dyn crate::api::ReplicationStoreTransaction,
        group_id: GroupId,
        dataset_ids: &HashSet<DatasetId>,
    ) -> Result<MaterialisedDatasets, StoreError> {
        let mut datasets = HashMap::with_capacity(dataset_ids.len());
        let mut missing_dataset_ids = HashSet::new();
        for dataset_id in dataset_ids {
            let snapshot = transaction
                .load_dataset_snapshot(&group_id, dataset_id)
                .await?;
            if let Some(snapshot) = snapshot {
                datasets.insert(
                    dataset_id.clone(),
                    LocalDataset {
                        data: snapshot.data,
                    },
                );
            } else {
                missing_dataset_ids.insert(dataset_id.clone());
            }
        }
        Ok(MaterialisedDatasets {
            datasets,
            missing_dataset_ids,
        })
    }

    /// Seed empty working datasets from application schemas for dataset ids
    /// that were not yet present in durable storage.
    ///
    /// This stays in the runtime for now because the current store boundary
    /// does not carry schema-backed empty datasets when no snapshot rows exist.
    async fn seed_missing_datasets(
        store: Arc<dyn ReplicationStore>,
        datasets: &mut HashMap<DatasetId, LocalDataset>,
        missing_dataset_ids: HashSet<DatasetId>,
    ) -> Result<(), DatasetSchemaLoadError> {
        let loaded_schemas = Self::load_dataset_schemas(store, missing_dataset_ids).await?;
        for (dataset_id, schema) in loaded_schemas {
            datasets.insert(dataset_id, LocalDataset::new(schema));
        }
        Ok(())
    }

    /// Persist the transactional dataset diff back into durable snapshot rows.
    async fn persist_dataset_changes(
        transaction: &mut dyn crate::api::ReplicationStoreTransaction,
        group_id: GroupId,
        original_datasets: &HashMap<DatasetId, LocalDataset>,
        working_datasets: &HashMap<DatasetId, LocalDataset>,
    ) -> Result<(), StoreError> {
        let dataset_ids: HashSet<_> = original_datasets
            .keys()
            .chain(working_datasets.keys())
            .cloned()
            .collect();
        for dataset_id in dataset_ids {
            let snapshot_write = working_datasets
                .get(&dataset_id)
                .and_then(|working_dataset| {
                    build_dataset_snapshot_write(
                        group_id,
                        dataset_id.clone(),
                        original_datasets.get(&dataset_id),
                        working_dataset,
                    )
                });
            let Some(snapshot_write) = snapshot_write else {
                continue;
            };
            transaction.apply_dataset_snapshot(snapshot_write).await?;
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
            .context(ExhaustedUpdateIdsSnafu { group_id })?;
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
    ) -> Result<Vec<crate::api::DatasetUpdateRecord>, PublishChangesError> {
        let mut encoded_operations: HashMap<
            DatasetId,
            Vec<flotsync_messages::datamodel::SchemaOperation>,
        > = HashMap::new();

        'changes: for mutation in changes {
            let row_id = mutation.row_id().clone();
            let working_dataset =
                working_dataset_for_publish(working_datasets, &row_id.dataset_id)?;

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
        Ok(encoded_operations
            .into_iter()
            .map(|(dataset_id, operations)| crate::api::DatasetUpdateRecord {
                dataset_id,
                operations,
            })
            .collect())
    }

    /// Convert one runtime update message into the durable update-record shape.
    fn build_replication_update_record(
        sender: MemberIdentity,
        message: UpdateBatchMessage,
        applied_locally: bool,
    ) -> crate::api::ReplicationUpdateRecord {
        crate::api::ReplicationUpdateRecord {
            group_id: message.group_id,
            update_id: message.update_id,
            sender,
            read_versions: message.read_versions,
            dataset_updates: message
                .dataset_updates
                .into_iter()
                .map(|dataset_update| crate::api::DatasetUpdateRecord {
                    dataset_id: dataset_update.dataset_id,
                    operations: dataset_update.operations,
                })
                .collect(),
            applied_locally,
        }
    }

    /// Collect the dataset ids touched by persisted update records.
    fn collect_record_dataset_ids(
        updates: &[crate::api::ReplicationUpdateRecord],
    ) -> HashSet<DatasetId> {
        updates
            .iter()
            .flat_map(|update| update.dataset_updates.iter())
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect()
    }

    /// Publish one local change batch through one durable store transaction.
    async fn publish_changes_transactionally(
        &mut self,
        changes: Vec<RowMutation>,
    ) -> Result<PreparedLocalPublish, PublishChangesError> {
        let (group_id, dataset_ids) = collect_group_dataset_scope(&changes)?;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(PublishStoreAccessSnafu)?;
        let persisted_group = transaction
            .load_replication_group(&group_id)
            .await
            .context(PublishStoreAccessSnafu)?;
        let persisted_group = persisted_group.context(UnknownGroupSnafu { group_id })?;
        let mut local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                .context(PublishInvalidPersistedGroupSnafu { group_id })?;
        let materialised_datasets =
            Self::materialise_touched_datasets(transaction.as_mut(), group_id, &dataset_ids)
                .await
                .context(PublishStoreAccessSnafu)?;
        let mut working_datasets = materialised_datasets.datasets;
        Self::seed_missing_datasets(
            self.store.clone(),
            &mut working_datasets,
            materialised_datasets.missing_dataset_ids,
        )
        .await
        .map_err(|error| match error {
            DatasetSchemaLoadError::Load { dataset_id, source } => {
                PublishChangesError::LoadDatasetSchema { dataset_id, source }
            }
            DatasetSchemaLoadError::Missing { dataset_id } => {
                PublishChangesError::MissingDatasetSchema { dataset_id }
            }
        })?;

        // Current snapshot diffing keeps both the committed and working dataset
        // images in memory. `flotsync-i5b` tracks the planned row-granular
        // redesign that should remove this full-copy comparison.
        let original_datasets = working_datasets.clone();
        let read_versions = local_group.version_vector.clone();
        let update_id = Self::next_local_update_id(&local_group, group_id)?;
        let dataset_updates =
            Self::build_local_dataset_updates(group_id, &mut working_datasets, changes, update_id)?;
        local_group.mark_applied(update_id);

        let persisted_update = crate::api::ReplicationUpdateRecord {
            group_id,
            update_id,
            sender: self.local_member.clone(),
            read_versions: read_versions.clone(),
            dataset_updates: dataset_updates.clone(),
            applied_locally: true,
        };
        transaction
            .append_replication_update(persisted_update)
            .await
            .context(PublishStoreAccessSnafu)?;
        Self::persist_dataset_changes(
            transaction.as_mut(),
            group_id,
            &original_datasets,
            &working_datasets,
        )
        .await
        .context(PublishStoreAccessSnafu)?;
        transaction
            .update_replication_group_version_vector(&group_id, local_group.version_vector)
            .await
            .context(PublishStoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(PublishStoreAccessSnafu)?;

        let payload =
            Self::encode_runtime_payload(RuntimeMessage::UpdateBatch(UpdateBatchMessage {
                group_id,
                update_id,
                read_versions,
                dataset_updates: dataset_updates
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
    ) -> Result<Handled, InboundDeliveryError> {
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
                let record = self.build_replication_group_record(group_id, &members);
                Ok(Handled::block_on(self, async move |mut async_self| {
                    let reply = async {
                        let persisted_group = async_self
                            .store_new_replication_group(record)
                            .await
                            .context(InstallBootstrapGroupSnafu { group_id })?;
                        async_self
                            .install_group_membership_view(persisted_group)
                            .context(InstallBootstrapGroupSnafu { group_id })?;
                        // Dropping `deliver` without completing `processed`
                        // intentionally withholds the recipient acknowledgement
                        // from reliable delivery. Sender-side timeout/retry
                        // semantics are tracked in flotsync-46r.
                        deliver
                            .processed
                            .complete()
                            .context(CompleteProcessedPromiseSnafu { group_id })?;
                        Ok::<(), InboundDeliveryError>(())
                    }
                    .await;
                    if let Err(error) = reply {
                        async_self.record_terminal_fault("reliable delivery", &error);
                    }
                }))
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
            .context(InboundStoreAccessSnafu)?;
        let persisted_group = transaction
            .load_replication_group(&group_id)
            .await
            .context(InboundStoreAccessSnafu)?;
        let persisted_group = persisted_group.context(UnknownHostedGroupSnafu { group_id })?;
        let local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                .context(InboundInvalidPersistedGroupSnafu { group_id })?;
        let message = message
            .into_runtime(local_group.member_count())
            .context(DecodeReadVersionsSnafu { group_id })?;
        let expected_sender_index =
            local_group
                .members
                .member_index(&sender)
                .context(UpdateSenderNotInGroupSnafu {
                    group_id,
                    sender: sender.clone(),
                })?;
        ensure!(
            expected_sender_index.as_u32() == message.update_id.node_index,
            UpdateSenderIndexMismatchSnafu {
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
                .context(InboundStoreAccessSnafu)?;
            return Ok(Vec::new());
        }

        let inbound_update = Self::build_replication_update_record(sender, message, false);
        if let Some(existing_update) = transaction
            .load_replication_update(&group_id, inbound_update.update_id)
            .await
            .context(InboundStoreAccessSnafu)?
        {
            ensure!(
                existing_update == inbound_update,
                ConflictingPersistedUpdateSnafu {
                    group_id,
                    update_id: inbound_update.update_id,
                }
            );
        } else {
            transaction
                .append_replication_update(inbound_update.clone())
                .await
                .context(InboundStoreAccessSnafu)?;
        }

        let pending_updates = transaction
            .load_replication_updates(&group_id, crate::api::ReplicationUpdateFilter::PendingApply)
            .await
            .context(InboundStoreAccessSnafu)?;
        let mut pending_updates = PendingUpdateSet::from_updates(pending_updates);
        let mut local_group = local_group;
        let apply_plan = pending_updates.plan_apply_chain(&local_group);
        for applied_update_id in &apply_plan.already_applied {
            transaction
                .mark_replication_update_applied(&group_id, *applied_update_id)
                .await
                .context(InboundStoreAccessSnafu)?;
        }
        if apply_plan.ready_chain.is_empty() {
            transaction
                .commit()
                .await
                .context(InboundStoreAccessSnafu)?;
            return Ok(Vec::new());
        }

        let touched_dataset_ids = Self::collect_record_dataset_ids(&apply_plan.ready_chain);
        let materialised_datasets = Self::materialise_touched_datasets(
            transaction.as_mut(),
            group_id,
            &touched_dataset_ids,
        )
        .await
        .context(InboundStoreAccessSnafu)?;
        let mut working_datasets = materialised_datasets.datasets;
        Self::seed_missing_datasets(
            self.store.clone(),
            &mut working_datasets,
            materialised_datasets.missing_dataset_ids,
        )
        .await
        .map_err(|error| match error {
            DatasetSchemaLoadError::Load { dataset_id, source } => {
                InboundDeliveryError::InboundLoadDatasetSchema { dataset_id, source }
            }
            DatasetSchemaLoadError::Missing { dataset_id } => {
                InboundDeliveryError::InboundMissingDatasetSchema { dataset_id }
            }
        })?;
        // Current snapshot diffing keeps both the committed and working dataset
        // images in memory. `flotsync-i5b` tracks the planned row-granular
        // redesign that should remove this full-copy comparison.
        let original_datasets = working_datasets.clone();
        let mut event_batches = Vec::new();
        for ready_update in &apply_plan.ready_chain {
            let row_changes =
                apply_one_update_batch(&mut local_group, &mut working_datasets, ready_update)?;
            if !row_changes.is_empty() {
                event_batches.push(row_changes);
            }
            transaction
                .mark_replication_update_applied(&group_id, ready_update.update_id)
                .await
                .context(InboundStoreAccessSnafu)?;
        }
        Self::persist_dataset_changes(
            transaction.as_mut(),
            group_id,
            &original_datasets,
            &working_datasets,
        )
        .await
        .context(InboundStoreAccessSnafu)?;
        transaction
            .update_replication_group_version_vector(&group_id, local_group.version_vector)
            .await
            .context(InboundStoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(InboundStoreAccessSnafu)?;
        Ok(event_batches)
    }

    fn handle_update_batch(
        &mut self,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
    ) -> Handled {
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .persist_and_apply_update_batch(sender, message)
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

    fn handle_publish_changes(
        &mut self,
        ask: Ask<Vec<RowMutation>, Result<PublishReceipt, ApiError>>,
    ) -> Handled {
        let (promise, changes) = ask.take();
        if let Err(error) = self.ensure_running() {
            self.reply_api(promise, "publish_changes", Err(error));
            return Handled::Ok;
        }
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
        if let Err(error) = self.ensure_running() {
            self.reply_api(promise, "create_group", Err(error));
            return Handled::Ok;
        }
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
                    async_self.record_startup_fault("runtime startup", &error);
                    async_self.ctx.suicide();
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
        if self.terminal_fault.is_some() {
            return Handled::Ok;
        }
        let ReliableDeliveryPortIndication::Deliver(deliver) = indication;
        match self.handle_reliable_delivery(deliver) {
            Ok(handled) => handled,
            Err(error) => {
                self.record_terminal_fault("reliable delivery", &error);
                Handled::DieNow
            }
        }
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

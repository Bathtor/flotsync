#[cfg(test)]
use super::messages::UpdateBatchMessage;
use super::{
    DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES,
    catch_up_manager::{
        CatchUpManagerMessage,
        NeedVersions,
        ObservedAvailable,
        subtract_available_ranges,
    },
    config_keys,
    errors::{
        ConflictingExistingGroupSnafu,
        CreateGroupError,
        DuplicateGroupSnafu,
        GroupInstallError,
        InboundDeliveryError,
        InboundFailureAction,
        InitialStateUnsupportedSnafu,
        InvalidGroupSnafu,
        InvalidMembersSnafu,
        LocalMemberMissingSnafu,
        PublishChangesError,
        RuntimeStartupError,
        SecuritySnafu,
        SnapshotRowsError,
        StoreGroupSnafu,
        StoreStartupSnafu,
        SummaryError,
        inbound,
        publish,
        snapshot,
        summary,
    },
    in_memory::{
        LoadedGroupMeta,
        PendingUpdateSet,
        PreparedLocalChanges,
        TouchedGroupRows,
        apply_one_update,
        collect_group_row_scope,
        collect_record_row_scope,
        validate_inbound_update_read_versions,
        validate_update_mapping,
    },
    messages::{
        BootstrapGroupKey,
        BootstrapGroupMessage,
        BootstrapMemberKeyMessage,
        RuntimeMessage,
        SummaryMessage,
        SummaryRequestMessage,
        UpdateMessage,
        UpdateRangeMessage,
        WireRuntimeMessage,
        WireSummaryMessage,
        WireUpdateBatchMessage,
        WireUpdateMessage,
    },
    replay,
    summary_request_manager::SummaryRequestManagerMessage,
};
#[cfg(any(test, feature = "test-support"))]
use crate::test_support::test_group_key;
use crate::{
    MAX_VERSION_VALUE,
    api::{
        ApiError,
        ApiExternalSnafu,
        BatchProvider,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DatasetId,
        DatasetRowPatch,
        DatasetRowWrite,
        DatasetUpdateRecord,
        EncryptedGroupSecurityMaterial,
        GroupMigration,
        ProviderExternalSnafu,
        PublishChangesRequest,
        PublishReceipt,
        ReadToken,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationGroupRecord,
        ReplicationRowRecord,
        ReplicationStore,
        ReplicationStoreReadTransaction,
        ReplicationStoreTransaction,
        ReplicationUpdateFilter,
        ReplicationUpdateRecord,
        RowChange,
        RowId,
        RowKey,
        RowMutation,
        RowProviderError,
        SchemaSource,
        SnapshotRow,
        SnapshotRowBatch,
        SnapshotRows,
        SnapshotRowsRequest,
        StoreError,
        Summary,
        SummaryRequest,
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
        group_broadcast::{GroupBroadcastDeliver, GroupMessageHeader},
        reliable_delivery::{
            ReliableDeliveryDeliver,
            ReliableDeliverySubmit,
            ReliableMessageEnvelope,
            ReliableMessageHeader,
        },
        security::DeliverySecurity,
        shared::{DeliveryClass, MessageId, PlaintextPayload},
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::{GroupMembers, GroupMemberships, SharedGroupMemberships},
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::OwnedRow;
use flotsync_messages::proto::{DecodeProtoView, EncodeProto};
use flotsync_security::GROUP_CIPHER_SUITE_CHACHA20_POLY1305;
use flotsync_utils::{BoxFuture, KClaimablePromise, ResultExt as _};
use futures_util::FutureExt;
use itertools::Itertools;
use kompact::prelude::*;
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};
use uuid::Uuid;

/// Security records and plaintext bootstrap message prepared for group creation.
pub(super) struct PreparedGroupBootstrap {
    security_material: EncryptedGroupSecurityMaterial,
    bootstrap_message: Arc<BootstrapGroupMessage>,
}

impl PreparedGroupBootstrap {
    /// Return the plaintext bootstrap message prepared for group creation.
    #[cfg(test)]
    pub(super) fn bootstrap_message(&self) -> &BootstrapGroupMessage {
        self.bootstrap_message.as_ref()
    }
}

/// One local publish batch after local apply, encoding, and delivery-envelope preparation.
struct PreparedLocalPublish {
    group_id: GroupId,
    update_id: UpdateId,
    read_token: ReadToken,
    payload: bytes::Bytes,
    row_changes: Vec<RowChange>,
}

/// Source-specific sender validation for an inbound update.
enum InboundUpdateOrigin {
    /// Direct `Update` delivery where the envelope sender must be the producer.
    Producer { sender: MemberIdentity },
    /// Catch-up delivery where the envelope sender forwards another producer's update.
    Forwarder { sender: MemberIdentity },
}

/// Result of processing one inbound update.
struct InboundUpdateOutcome {
    /// Listener batches produced by updates that became newly applicable.
    event_batches: Vec<ListenerDataChanges>,
    /// Producer-version ranges still needed before blocked updates can apply.
    needed_ranges: Vec<UpdateRangeMessage>,
    /// Producer versions now known to be present in the local update log.
    observed_available: Vec<UpdateRangeMessage>,
}

/// Catch-up notifications derived from one observed summary message.
struct SummaryCatchUpObservation {
    /// Group whose local progress was compared with the observed summary.
    group_id: GroupId,
    /// Local pending update-log versions that can be advertised as available.
    observed_available: Vec<UpdateRangeMessage>,
    /// Versions the summary says exist elsewhere and are not already pending locally.
    needed_ranges: Vec<UpdateRangeMessage>,
}

/// One listener notification batch paired with the read token reached by that batch.
struct ListenerDataChanges {
    read_token: ReadToken,
    row_changes: Vec<RowChange>,
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

/// Convert a wire summary using an explicit membership snapshot for context.
fn summary_from_wire(
    sender: MemberIdentity,
    message: WireSummaryMessage,
    memberships: &GroupMemberships,
) -> Result<Summary, InboundDeliveryError> {
    let group_id = message.group_id;
    let members = memberships
        .members(&group_id)
        .context(inbound::UnknownHostedGroupSnafu { group_id })?;
    let member_count = NonZeroUsize::new(members.len()).expect("group members must not be empty");
    message
        .into_summary(sender, member_count)
        .context(inbound::DecodeReadVersionsSnafu { group_id })
}

/// Snapshot provider backed by one store read transaction.
///
/// The transaction pins a consistent store view while the provider batches rows
/// from requested datasets. Dropping the provider releases the transaction
/// through the store implementation's release-on-drop path.
struct StoreSnapshotRowProvider {
    transaction: Option<Box<dyn ReplicationStoreReadTransaction>>,
    group_id: GroupId,
    datasets: HashSet<DatasetId>,
    current_dataset: Option<DatasetId>,
    /// Exclusive lower bound for the current dataset scan.
    ///
    /// `None` starts before the first row. `Some(row_key)` means the next store
    /// scan requests rows with `row_key > after_row_key`.
    after_row_key: Option<RowKey>,
    max_rows_per_batch: NonZeroUsize,
    include_tombstones: bool,
}

impl StoreSnapshotRowProvider {
    async fn release_transaction(&mut self) -> Result<(), RowProviderError> {
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };
        transaction
            .release()
            .await
            .boxed()
            .context(ProviderExternalSnafu)
    }

    fn select_dataset(&mut self) -> Option<DatasetId> {
        if self.current_dataset.is_none() {
            self.current_dataset = self.datasets.iter().next().cloned();
        }
        self.current_dataset.clone()
    }

    fn finish_current_dataset(&mut self) {
        if let Some(dataset_id) = self.current_dataset.take() {
            self.datasets.remove(&dataset_id);
        }
        self.after_row_key = None;
    }

    fn snapshot_row_from_record(
        group_id: GroupId,
        dataset_id: DatasetId,
        record: ReplicationRowRecord,
    ) -> SnapshotRow {
        let row_id = RowId {
            group_id,
            dataset_id,
            row_key: record.row_id,
        };
        let fields = record
            .snapshot
            .into_owned_fields()
            .into_iter()
            .collect::<HashMap<_, _>>();
        SnapshotRow {
            row_id,
            row: Arc::new(OwnedRow::new(fields)),
            deleted: record.tombstoned,
        }
    }
}

impl BatchProvider for StoreSnapshotRowProvider {
    type Batch = SnapshotRowBatch;

    fn new_batch(&self) -> Self::Batch {
        SnapshotRowBatch::with_capacity(self.max_rows_per_batch.get())
    }

    fn fill_batch(
        &mut self,
        mut reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        async move {
            reuse.clear();
            while reuse.is_empty() {
                let Some(dataset_id) = self.select_dataset() else {
                    self.release_transaction().await?;
                    return Ok(None);
                };
                let group_id = self.group_id;
                let after = self.after_row_key;
                let max_rows_per_batch = self.max_rows_per_batch;
                let Some(transaction) = self.transaction.as_mut() else {
                    return Ok(None);
                };
                let batch = transaction
                    .scan_dataset_row_batch(&group_id, &dataset_id, after, max_rows_per_batch)
                    .await
                    .boxed()
                    .context(ProviderExternalSnafu)?;

                'rows: for record in batch.rows {
                    if record.tombstoned && !self.include_tombstones {
                        continue 'rows;
                    }
                    reuse.push(Self::snapshot_row_from_record(
                        self.group_id,
                        dataset_id.clone(),
                        record,
                    ));
                }

                if let Some(next_after) = batch.next_after {
                    self.after_row_key = Some(next_after);
                } else {
                    self.finish_current_dataset();
                }
            }

            Ok(Some(reuse))
        }
        .boxed()
    }
}

/// Local-actor messages understood by [`ReplicationRuntimeComponent`].
///
/// This is the imperative bridge surface between the public async runtime
/// handle and the Kompact-hosted replication component.
#[derive(Debug)]
pub enum ReplicationRuntimeMessage {
    /// Submit one local publish request through the component interface.
    PublishChanges(Ask<PublishChangesRequest, Result<PublishReceipt, ApiError>>),
    /// Request a local snapshot stream through the component interface.
    SnapshotRows(Ask<SnapshotRowsRequest, Result<SnapshotRows, ApiError>>),
    /// Ask one group member for its current group version vector.
    RequestSummary(Ask<SummaryRequest, Result<Summary, ApiError>>),
    /// Create one new fixed-membership group through the component interface.
    CreateGroup(Ask<CreateGroupRequest, Result<GroupId, ApiError>>),
    /// Request one group-membership change through the component interface.
    ChangeGroupMembership(Ask<ChangeGroupMembershipRequest, Result<GroupMigration, ApiError>>),
    /// Test-support command channel for runtime fixture setup and assertions.
    #[cfg(any(test, feature = "test-support"))]
    Test(ReplicationRuntimeTestMessage),
}

/// Delivery path that should carry one inbound summary response.
enum SummaryReplyRoute {
    /// Reply to a recipient-addressed reliable request and acknowledge it after
    /// the response is submitted.
    Reliable {
        recipient: MemberIdentity,
        processed: KClaimablePromise<()>,
    },
    /// Reply to a group-broadcast request by publishing the summary to the group.
    GroupBroadcast,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
#[allow(
    private_interfaces,
    reason = "test-only ask plumbing reuses internal error types"
)]
pub enum ReplicationRuntimeTestMessage {
    /// Confirm that the runtime component is alive and able to process one
    /// mailbox turn after startup.
    #[cfg(test)]
    Ping(Ask<(), ()>),
    /// Install a group directly in runtime state for integration scenarios that
    /// need pre-existing group membership until production group setup exists.
    InstallGroup(Ask<(GroupId, GroupMembers), Result<(), GroupInstallError>>),
    /// Apply one inbound update directly to runtime logic tests.
    #[cfg(test)]
    ApplyUpdate(Ask<(MemberIdentity, UpdateMessage), Result<(), InboundDeliveryError>>),
    /// Apply one inbound update batch directly to runtime logic tests.
    #[cfg(test)]
    ApplyUpdateBatch(Ask<(MemberIdentity, UpdateBatchMessage), Result<(), InboundDeliveryError>>),
}

#[cfg(any(test, feature = "test-support"))]
impl ReplicationRuntimeMessage {
    #[cfg(test)]
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

    #[cfg(test)]
    pub(super) fn test_apply_update(
        promise: KPromise<Result<(), InboundDeliveryError>>,
        sender: MemberIdentity,
        message: UpdateMessage,
    ) -> Self {
        Self::Test(ReplicationRuntimeTestMessage::ApplyUpdate(Ask::new(
            promise,
            (sender, message),
        )))
    }

    #[cfg(test)]
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
    security: DeliverySecurity,
    group_memberships: SharedGroupMemberships,
    summary_request_manager: ActorRefStrong<SummaryRequestManagerMessage>,
    catch_up_manager: ActorRefStrong<CatchUpManagerMessage>,
    /// Resolved group-size limit for including inline public key bundles in bootstrap messages.
    max_inline_bootstrap_public_key_bundles: usize,
}

impl ReplicationRuntimeComponent {
    /// Construct one replication runtime component for one local member.
    pub(super) fn new(
        local_member: MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        security: DeliverySecurity,
        group_memberships: SharedGroupMemberships,
        summary_request_manager: ActorRefStrong<SummaryRequestManagerMessage>,
        catch_up_manager: ActorRefStrong<CatchUpManagerMessage>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            group_broadcast: RequiredPort::uninitialised(),
            reliable_delivery: RequiredPort::uninitialised(),
            local_member,
            store,
            listener,
            security,
            group_memberships,
            summary_request_manager,
            catch_up_manager,
            max_inline_bootstrap_public_key_bundles:
                DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES,
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

    async fn snapshot_rows_from_store(
        &mut self,
        request: SnapshotRowsRequest,
    ) -> Result<SnapshotRows, SnapshotRowsError> {
        ensure!(!request.datasets.is_empty(), snapshot::EmptyDatasetsSnafu);
        let hosted_group_ids = self
            .group_memberships
            .snapshot()
            .group_ids()
            .copied()
            .collect::<HashSet<_>>();
        ensure!(
            hosted_group_ids.contains(&request.group_id),
            snapshot::UnknownGroupSnafu {
                group_id: request.group_id,
            }
        );

        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(snapshot::StoreAccessSnafu)?;
        let groups = transaction
            .load_replication_groups_for_ids(&hosted_group_ids)
            .await
            .context(snapshot::StoreAccessSnafu)?;
        let read_token = Self::read_token_from_groups(groups);
        ensure!(
            read_token.group_version(&request.group_id).is_some(),
            snapshot::UnknownGroupSnafu {
                group_id: request.group_id,
            }
        );

        let provider = StoreSnapshotRowProvider {
            transaction: Some(transaction),
            group_id: request.group_id,
            datasets: request.datasets,
            current_dataset: None,
            after_row_key: None,
            max_rows_per_batch: request.max_rows_per_batch,
            include_tombstones: request.include_tombstones,
        };
        Ok(SnapshotRows {
            group_id: request.group_id,
            read_token,
            rows: Box::new(provider),
        })
    }

    /// Persist one set of explicit row patches back into the replication store.
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
        security_material: crate::api::EncryptedGroupSecurityMaterial,
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
            security_material,
        }
    }

    /// Build one opaque application read token from currently stored group progress.
    fn read_token_from_groups(
        groups: impl IntoIterator<Item = ReplicationGroupRecord>,
    ) -> ReadToken {
        let group_versions = groups
            .into_iter()
            .map(|group| (group.group_id, group.version_vector))
            .collect();
        ReadToken::from_group_versions(group_versions)
    }

    /// Return catch-up ranges required before an inbound update can apply.
    ///
    /// The update can be blocked both by versions in its read frontier and by
    /// earlier versions from the same producer. The latter are not present in
    /// the read frontier because producer updates do not read themselves.
    fn missing_ranges_for_update_frontier(
        local_versions: &VersionVector,
        group_id: GroupId,
        update_id: UpdateId,
        read_versions: &VersionVector,
    ) -> Result<Vec<UpdateRangeMessage>, InboundDeliveryError> {
        let mut ranges = local_versions
            .missing_version_ranges_to(read_versions)
            .into_iter()
            .map(UpdateRangeMessage::from)
            .collect_vec();
        let producer_index = update_id.node_index as usize;
        ensure!(
            producer_index < local_versions.num_members().get(),
            inbound::UpdateProducerIndexNotInGroupSnafu {
                group_id,
                producer_index: MemberIndex::new(update_id.node_index),
            }
        );
        let expected_producer_version = local_versions.version_at(producer_index) + 1;
        if update_id.version > expected_producer_version {
            ranges.push(UpdateRangeMessage {
                producer_index: update_id.node_index,
                start_version: expected_producer_version,
                end_version: update_id.version - 1,
            });
        }
        Ok(ranges)
    }

    /// Validate the envelope sender for one inbound update and return the producer identity.
    ///
    /// Direct producer sends must come from the canonical producer index.
    /// Forwarded catch-up updates only require the envelope sender to be a
    /// group member; the persisted sender remains the canonical producer.
    fn validated_inbound_update_producer(
        local_group: &LoadedGroupMeta,
        group_id: GroupId,
        origin: InboundUpdateOrigin,
        update_id: UpdateId,
    ) -> Result<MemberIdentity, InboundDeliveryError> {
        let producer_index = MemberIndex::new(update_id.node_index);
        let producer = local_group
            .members
            .member_at_index(producer_index)
            .context(inbound::UpdateProducerIndexNotInGroupSnafu {
                group_id,
                producer_index,
            })?;
        match origin {
            InboundUpdateOrigin::Producer { sender } => {
                let expected_sender_index = local_group.members.member_index(&sender).context(
                    inbound::UpdateSenderNotInGroupSnafu {
                        group_id,
                        sender: sender.clone(),
                    },
                )?;
                ensure!(
                    expected_sender_index == producer_index,
                    inbound::UpdateSenderIndexMismatchSnafu {
                        group_id,
                        sender: sender.clone(),
                        expected_index: expected_sender_index,
                        actual_index: producer_index,
                    }
                );
                Ok(sender)
            }
            InboundUpdateOrigin::Forwarder { sender } => {
                local_group
                    .members
                    .member_index(&sender)
                    .context(inbound::UpdateSenderNotInGroupSnafu { group_id, sender })?;
                Ok(producer)
            }
        }
    }

    /// Validate that a bootstrap message authorises both the sender and local member.
    fn validate_bootstrap_membership(
        group_id: GroupId,
        members: &GroupMembers,
        local_member: &MemberIdentity,
        sender: &MemberIdentity,
    ) -> Result<(), InboundDeliveryError> {
        if !members.contains(sender) {
            return Err(InboundDeliveryError::BootstrapSenderNotInGroup {
                group_id,
                sender: sender.clone(),
            });
        }
        if !members.contains(local_member) {
            return Err(InboundDeliveryError::BootstrapMissingLocalMember {
                group_id,
                local_member: local_member.clone(),
            });
        }
        Ok(())
    }

    fn notify_catch_up_needed(&self, group_id: GroupId, ranges: Vec<UpdateRangeMessage>) {
        if ranges.is_empty() {
            return;
        }
        self.catch_up_manager
            .tell(CatchUpManagerMessage::NeedVersions(NeedVersions {
                group_id,
                ranges,
            }));
    }

    fn notify_catch_up_available(&self, group_id: GroupId, ranges: Vec<UpdateRangeMessage>) {
        if ranges.is_empty() {
            return;
        }
        self.catch_up_manager
            .tell(CatchUpManagerMessage::ObservedAvailable(
                ObservedAvailable { group_id, ranges },
            ));
    }

    fn validate_summary_request(&self, request: &SummaryRequest) -> Result<(), SummaryError> {
        let memberships = self.group_memberships.snapshot();
        let members =
            memberships
                .members(&request.group_id)
                .context(summary::UnknownGroupSnafu {
                    group_id: request.group_id,
                })?;
        ensure!(
            members.contains(&request.target),
            summary::TargetNotInGroupSnafu {
                group_id: request.group_id,
                target: request.target.clone(),
            }
        );
        Ok(())
    }

    async fn load_summary_versions_from_store(
        store: Arc<dyn ReplicationStore>,
        group_id: GroupId,
    ) -> Result<Option<VersionVector>, StoreError> {
        let mut transaction = store.begin_read_transaction().await?;
        let group = transaction.load_replication_group(&group_id).await?;
        Ok(group.map(|group| group.version_vector))
    }

    fn summary_message_for_request(
        message: SummaryRequestMessage,
        has_versions: VersionVector,
    ) -> RuntimeMessage {
        RuntimeMessage::Summary(SummaryMessage::new(
            message.group_id,
            message.correlation_id,
            has_versions,
        ))
    }

    async fn load_summary_message_from_store(
        store: Arc<dyn ReplicationStore>,
        message: SummaryRequestMessage,
    ) -> Result<RuntimeMessage, InboundDeliveryError> {
        let has_versions = Self::load_summary_versions_from_store(store, message.group_id)
            .await
            .context(inbound::StoreAccessSnafu)?;
        let has_versions = has_versions.context(inbound::UnknownHostedGroupSnafu {
            group_id: message.group_id,
        })?;
        Ok(Self::summary_message_for_request(message, has_versions))
    }

    fn summary_for_request(
        group_id: GroupId,
        responder: MemberIdentity,
        has_versions: VersionVector,
    ) -> Summary {
        Summary {
            group_id,
            responder,
            has_versions,
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
            .begin_read_transaction()
            .await
            .context(StoreStartupSnafu)?;
        let persisted_groups = transaction
            .load_replication_groups()
            .await
            .context(StoreStartupSnafu)?;

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
    fn submit_group_update(&mut self, prepared_publish: &PreparedLocalPublish) {
        self.group_broadcast.trigger(
            GroupBroadcastPortRequest::build_submit(DeliveryClass::BestEffort)
                .for_member_in_group(self.local_member.clone(), prepared_publish.group_id)
                .with_payload(prepared_publish.payload.clone()),
        );
    }

    fn submit_reliable_runtime_message(
        &mut self,
        recipient: MemberIdentity,
        message: &RuntimeMessage,
    ) {
        let payload = message.encode_proto_to_bytes();
        self.reliable_delivery
            .trigger(ReliableDeliveryPortRequest::Submit(
                ReliableDeliverySubmit {
                    envelope: ReliableMessageEnvelope::<PlaintextPayload> {
                        header: ReliableMessageHeader {
                            sender: self.local_member.clone(),
                            recipient,
                            message_id: MessageId(Uuid::new_v4()),
                        },
                        payload: PlaintextPayload { bytes: payload },
                    },
                },
            ));
    }

    /// Submit reliable bootstrap messages after the group is locally installed.
    fn submit_group_bootstrap_messages(&mut self, bootstrap_message: Arc<BootstrapGroupMessage>) {
        let local_member = self.local_member.clone();
        for recipient in bootstrap_message
            .members()
            .iter()
            .filter(|member| *member != &local_member)
            .cloned()
        {
            let message = RuntimeMessage::BootstrapGroup(Arc::clone(&bootstrap_message));
            self.submit_reliable_runtime_message(recipient, &message);
        }
    }

    /// Validate one create-group request and derive the canonical persisted
    /// group record that should be written if the request succeeds.
    fn prepare_created_group(
        &self,
        req: CreateGroupRequest,
    ) -> Result<(GroupId, GroupMembers), CreateGroupError> {
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
        Ok((group_id, members))
    }

    /// Read the maximum group size for inlining bootstrap public key bundles.
    fn read_max_inline_bootstrap_public_key_bundles(&self) -> usize {
        match self
            .ctx
            .config()
            .read_or_default(&config_keys::BOOTSTRAP_MAX_INLINE_PUBLIC_KEY_BUNDLES)
        {
            Ok(limit) => limit,
            Err(error) => {
                warn!(
                    self.log(),
                    "failed to read bootstrap inline public-key bundle limit config; using default {}: {}",
                    DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES,
                    error
                );
                DEFAULT_MAX_INLINE_BOOTSTRAP_PUBLIC_KEY_BUNDLES
            }
        }
    }

    pub(super) async fn prepare_group_bootstrap(
        security: &DeliverySecurity,
        max_inline_public_key_bundles: usize,
        group_id: GroupId,
        members: &GroupMembers,
    ) -> Result<PreparedGroupBootstrap, CreateGroupError> {
        let group_key = DeliverySecurity::generate_group_key()
            .boxed()
            .context(SecuritySnafu)?;
        let public_keys = security
            .public_keys_for_members(members)
            .await
            .boxed()
            .context(SecuritySnafu)?;
        let inline_public_bundles = members.len() <= max_inline_public_key_bundles;
        let bootstrap_member_keys = public_keys.map_values(|public_keys| {
            if inline_public_bundles {
                BootstrapMemberKeyMessage::from_public_keys(public_keys)
            } else {
                BootstrapMemberKeyMessage::from_fingerprint(public_keys.fingerprint())
            }
        });
        let security_material = security
            .seal_group_secret(group_id.0, &group_key)
            .boxed()
            .context(SecuritySnafu)?;
        let bootstrap_message = BootstrapGroupMessage::new(
            group_id,
            members.ordered_members(),
            bootstrap_member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            BootstrapGroupKey::from_group_key(group_key),
        )
        .boxed()
        .context(SecuritySnafu)?;
        Ok(PreparedGroupBootstrap {
            security_material,
            bootstrap_message: Arc::new(bootstrap_message),
        })
    }

    fn next_local_update_id(
        local_group: &LoadedGroupMeta,
        group_id: GroupId,
    ) -> Result<UpdateId, PublishChangesError> {
        let applied_version = local_group.applied_version(local_group.local_member_index);
        ensure!(
            applied_version < MAX_VERSION_VALUE,
            publish::ExhaustedUpdateIdsSnafu { group_id }
        );
        let next_local_version = applied_version + 1;
        Ok(UpdateId {
            version: next_local_version,
            node_index: local_group.local_member_index.as_u32(),
        })
    }

    /// Stage local row mutations as schema operations for one replication update.
    ///
    /// This is the local `SchemaOperation -> Update(ReadVV)` boundary: the
    /// caller supplies the freshly allocated `UpdateId`, and every effective
    /// schema operation produced for the publish batch is encoded with exactly
    /// that change id. The surrounding publish transaction captures `ReadVV`
    /// before applying the mutations, persists the same `UpdateId` and `ReadVV`,
    /// and sends both values in the outbound `Update`.
    ///
    /// `last_changed_versions` is the causal frontier of the new update and is
    /// stored on every row image affected by the publish.
    fn build_local_dataset_updates(
        group_id: GroupId,
        dataset_state: &mut replay::PublishDatasetState,
        changes: Vec<RowMutation>,
        update_id: UpdateId,
        last_changed_versions: &VersionVector,
    ) -> Result<PreparedLocalChanges, PublishChangesError> {
        let mut encoded_operations: HashMap<
            DatasetId,
            Vec<flotsync_messages::datamodel::SchemaOperation>,
        > = HashMap::new();
        let mut row_writes: HashMap<DatasetId, Vec<DatasetRowWrite>> = HashMap::new();
        let mut row_changes = Vec::new();

        'changes: for mutation in changes {
            let row_id = mutation.row_id().clone();
            let applied_operation = match mutation {
                RowMutation::Upsert { row, .. } => {
                    dataset_state.apply_upsert(&row_id, row, update_id)?
                }
                RowMutation::Delete { .. } => {
                    let applied_delete = dataset_state.apply_delete(&row_id, update_id)?;
                    Some(applied_delete)
                }
            };
            let Some(applied_operation) = applied_operation else {
                continue 'changes;
            };
            encoded_operations
                .entry(row_id.dataset_id.clone())
                .or_default()
                .push(applied_operation.encoded_operation);
            if let Some(row_change) = applied_operation.row_change {
                row_changes.push(row_change);
            }
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
                last_changed_versions: last_changed_versions.clone(),
            })
            .collect();
        Ok(PreparedLocalChanges {
            dataset_updates,
            row_patches,
            row_changes,
        })
    }

    /// Convert one runtime update message into the stored update-record shape.
    fn build_replication_update_record(
        sender: MemberIdentity,
        message: UpdateMessage,
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
                .map(DatasetUpdateRecord::from)
                .collect(),
            applied_locally,
        }
    }

    /// Publish one local change batch through one store transaction.
    #[allow(
        clippy::too_many_lines,
        reason = "The transactional publish flow is kept together so store writes, listener notifications, and rollback boundaries stay visible."
    )]
    async fn publish_changes_transactionally(
        &mut self,
        request: PublishChangesRequest,
    ) -> Result<PreparedLocalPublish, PublishChangesError> {
        let PublishChangesRequest {
            read_token,
            changes,
        } = request;
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
        let read_versions = read_token
            .group_version(&group_id)
            .cloned()
            .context(publish::ReadTokenMissingGroupSnafu { group_id })?;
        ensure!(
            read_versions.num_members() == local_group.member_count(),
            publish::ReadTokenMemberCountMismatchSnafu {
                group_id,
                read_token_member_count: read_versions.num_members().get(),
                persisted_member_count: local_group.member_count().get(),
            }
        );
        ensure!(
            local_group.version_vector >= read_versions,
            publish::ReadTokenAheadOfLocalStateSnafu { group_id }
        );
        let mut dataset_state = replay::load_publish_dataset_state(
            transaction.as_mut(),
            &loaded_schemas,
            group_id,
            local_group.member_count(),
            dataset_rows,
            &read_versions,
        )
        .await?;
        let update_id = Self::next_local_update_id(&local_group, group_id)?;
        let last_changed_versions = read_versions.with_update_applied(update_id);
        let prepared_local_changes = Self::build_local_dataset_updates(
            group_id,
            &mut dataset_state,
            changes,
            update_id,
            &last_changed_versions,
        )?;
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
        let read_token = read_token.with_update_applied(group_id, update_id);
        transaction
            .commit()
            .await
            .context(publish::StoreAccessSnafu)?;

        let message = RuntimeMessage::Update(Box::new(UpdateMessage {
            group_id,
            update_id,
            read_versions,
            dataset_updates: prepared_local_changes
                .dataset_updates
                .into_iter()
                .map(Into::into)
                .collect(),
        }));
        let payload = message.encode_proto_to_bytes();
        Ok(PreparedLocalPublish {
            group_id,
            update_id,
            read_token,
            payload,
            row_changes: prepared_local_changes.row_changes,
        })
    }

    fn submit_summary_reply(
        &mut self,
        route: SummaryReplyRoute,
        message: SummaryRequestMessage,
        summary: &RuntimeMessage,
    ) -> Result<(), InboundDeliveryError> {
        match route {
            SummaryReplyRoute::Reliable {
                recipient,
                processed,
            } => {
                self.submit_reliable_runtime_message(recipient, summary);
                processed
                    .complete()
                    .context(inbound::CompleteProcessedPromiseSnafu {
                        group_id: message.group_id,
                    })?;
            }
            SummaryReplyRoute::GroupBroadcast => {
                self.group_broadcast.trigger(
                    GroupBroadcastPortRequest::build_submit(DeliveryClass::BestEffort)
                        .for_member_in_group(self.local_member.clone(), message.group_id)
                        .with_payload(summary.encode_proto_to_bytes()),
                );
            }
        }
        Ok(())
    }

    fn handle_inbound_summary_request(
        &mut self,
        context: InboundDeliveryContext,
        route: SummaryReplyRoute,
        message: SummaryRequestMessage,
    ) -> HandlerResult {
        let store = self.store.clone();
        Handled::block_on(self, async move |mut async_self| {
            let reply = async {
                let summary = Self::load_summary_message_from_store(store, message).await?;
                async_self.submit_summary_reply(route, message, &summary)
            }
            .await;
            if let Err(error) = reply {
                let failure = InboundDeliveryFailure::new(context, error);
                let action = async_self.record_inbound_failure(&failure);
                panic_if_fatal_inbound_failure(action, &failure);
            }
            Handled::OK
        })
    }

    /// Validate, persist, and install one reliable-delivery bootstrap message.
    fn handle_bootstrap_group_delivery(
        &mut self,
        context: InboundDeliveryContext,
        deliver: ReliableDeliveryDeliver,
        message: Arc<BootstrapGroupMessage>,
    ) -> HandlerResult {
        let sender = deliver.envelope.header.sender.clone();
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .install_bootstrap_group_delivery(deliver, message, sender)
                .await;
            if let Err(error) = reply {
                let failure = InboundDeliveryFailure::new(context, error);
                let action = async_self.record_inbound_failure(&failure);
                panic_if_fatal_inbound_failure(action, &failure);
            }
            Handled::OK
        })
    }

    async fn install_bootstrap_group_delivery(
        &mut self,
        deliver: ReliableDeliveryDeliver,
        message: Arc<BootstrapGroupMessage>,
        sender: MemberIdentity,
    ) -> Result<(), InboundDeliveryError> {
        let group_id = message.group_id();
        let members = GroupMembers::from_ordered_members(message.members().to_vec())
            .context(inbound::InvalidBootstrapMembersSnafu)?;
        Self::validate_bootstrap_membership(group_id, &members, &self.local_member, &sender)?;
        let security_material = self
            .security
            .prepare_security_material_from_bootstrap_msg(message.as_ref(), &sender)
            .await
            .boxed()
            .context(inbound::BootstrapSecuritySnafu)?;
        let record = self.build_replication_group_record(group_id, &members, security_material);
        let persisted_group = self
            .store_new_replication_group(record)
            .await
            .context(inbound::InstallBootstrapGroupSnafu { group_id })?;
        self.install_group_membership_view(persisted_group)
            .context(inbound::InstallBootstrapGroupSnafu { group_id })?;
        // Complete `processed` only after the bootstrap group is stored and the
        // local membership view is installed. Failure paths intentionally withhold
        // this completion, so the sender-side recipient-ack timeout can redeliver
        // the same reliable message later.
        deliver
            .processed
            .complete()
            .context(inbound::CompleteProcessedPromiseSnafu { group_id })
    }

    fn handle_reliable_delivery(
        &mut self,
        deliver: ReliableDeliveryDeliver,
    ) -> Result<HandlerResult, InboundDeliveryFailure> {
        let context = InboundDeliveryContext::reliable(&deliver.envelope.header);
        let message =
            match WireRuntimeMessage::decode_proto_view_from_slice(&deliver.envelope.payload.bytes)
                .context(inbound::DecodeMessageSnafu)
            {
                Ok(message) => message,
                Err(error) => return Err(InboundDeliveryFailure::new(context, error)),
            };
        match message {
            WireRuntimeMessage::BootstrapGroup(message) => {
                Ok(self.handle_bootstrap_group_delivery(context, deliver, message))
            }
            WireRuntimeMessage::Update(_) => Err(InboundDeliveryFailure::new(
                context,
                InboundDeliveryError::UnexpectedReliableMessage,
            )),
            WireRuntimeMessage::NeedRange(_) | WireRuntimeMessage::UpdateBatch(_) => {
                Err(InboundDeliveryFailure::new(
                    context,
                    InboundDeliveryError::UnexpectedReliableMessage,
                ))
            }
            WireRuntimeMessage::SummaryRequest(message) => {
                let sender = deliver.envelope.header.sender.clone();
                Ok(self.handle_inbound_summary_request(
                    context,
                    SummaryReplyRoute::Reliable {
                        recipient: sender,
                        processed: deliver.processed,
                    },
                    message,
                ))
            }
            WireRuntimeMessage::Summary(message) => {
                let sender = deliver.envelope.header.sender.clone();
                let memberships = self.group_memberships.snapshot();
                let summary = summary_from_wire(sender, message, memberships.as_ref())
                    .map_err(|error| InboundDeliveryFailure::new(context.clone(), error))?;
                Ok(self.handle_observed_summary(summary))
            }
        }
    }

    fn handle_group_delivery(
        &mut self,
        deliver: &GroupBroadcastDeliver,
    ) -> Result<HandlerResult, InboundDeliveryFailure> {
        let context = InboundDeliveryContext::group(&deliver.envelope.header);
        let sender = deliver.envelope.header.sender.clone();
        let message =
            match WireRuntimeMessage::decode_proto_view_from_slice(&deliver.envelope.payload.bytes)
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
            WireRuntimeMessage::Update(message) => Ok(self.handle_update(context, sender, message)),
            WireRuntimeMessage::UpdateBatch(message) => {
                Ok(self.handle_update_batch(context, sender, message))
            }
            WireRuntimeMessage::NeedRange(_) => {
                // CatchUpManagerComponent owns NeedRange processing on group broadcast.
                Ok(Handled::OK)
            }
            WireRuntimeMessage::Summary(message) => {
                let memberships = self.group_memberships.snapshot();
                let summary = summary_from_wire(sender, message, memberships.as_ref())
                    .map_err(|error| InboundDeliveryFailure::new(context.clone(), error))?;
                Ok(self.handle_observed_summary(summary))
            }
            WireRuntimeMessage::SummaryRequest(message) => Ok(self.handle_inbound_summary_request(
                context,
                SummaryReplyRoute::GroupBroadcast,
                message,
            )),
        }
    }

    /// Persist one inbound update and apply every newly-ready successor inside
    /// the same store transaction.
    #[allow(
        clippy::too_many_lines,
        reason = "The inbound transaction is intentionally linear to keep causality checks and rollback handling auditable."
    )]
    async fn persist_and_apply_update(
        &mut self,
        origin: InboundUpdateOrigin,
        message: WireUpdateMessage,
    ) -> Result<InboundUpdateOutcome, InboundDeliveryError> {
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
        let producer = Self::validated_inbound_update_producer(
            &local_group,
            group_id,
            origin,
            message.update_id,
        )?;
        let incoming_available_range = UpdateRangeMessage::from(message.update_id);
        let mut needed_ranges = Self::missing_ranges_for_update_frontier(
            &local_group.version_vector,
            message.group_id,
            message.update_id,
            &message.read_versions,
        )?;
        let inbound_update = Self::build_replication_update_record(producer, message, false);
        validate_inbound_update_read_versions(&inbound_update)?;
        if local_group.has_applied(inbound_update.update_id) {
            transaction
                .commit()
                .await
                .context(inbound::StoreAccessSnafu)?;
            return Ok(InboundUpdateOutcome {
                event_batches: Vec::new(),
                needed_ranges,
                observed_available: vec![incoming_available_range],
            });
        }

        if let Some(existing_update) = transaction
            .load_replication_update(&group_id, inbound_update.update_id)
            .await
            .context(inbound::StoreAccessSnafu)?
        {
            ensure!(
                existing_update == inbound_update,
                inbound::ConflictingPersistedUpdateSnafu {
                    group: group_id,
                    update: inbound_update.update_id,
                }
            );
        } else {
            let touched_dataset_ids = inbound_update
                .dataset_updates
                .iter()
                .map(|dataset_update| dataset_update.dataset_id.clone())
                .collect::<HashSet<_>>();
            let loaded_schemas =
                Self::load_dataset_schemas(self.store.clone(), touched_dataset_ids)
                    .await
                    .map_err(|error| match error {
                        DatasetSchemaLoadError::Load { dataset_id, source } => {
                            InboundDeliveryError::LoadDatasetSchema { dataset_id, source }
                        }
                        DatasetSchemaLoadError::Missing { dataset_id } => {
                            InboundDeliveryError::MissingDatasetSchema { dataset_id }
                        }
                    })?;
            validate_update_mapping(&inbound_update, &loaded_schemas)?;
            transaction
                .append_replication_update(inbound_update.clone())
                .await
                .context(inbound::StoreAccessSnafu)?;
        }

        let pending_updates = transaction
            .load_replication_updates(&group_id, ReplicationUpdateFilter::PendingApply, None)
            .await
            .context(inbound::StoreAccessSnafu)?;
        let mut observed_available = vec![incoming_available_range];
        observed_available.extend(
            pending_updates
                .iter()
                .map(|update| UpdateRangeMessage::from(update.update_id)),
        );
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
            for blocked_update in &apply_plan.blocked_updates {
                let blocked_ranges = Self::missing_ranges_for_update_frontier(
                    &local_group.version_vector,
                    blocked_update.group_id,
                    blocked_update.update_id,
                    &blocked_update.read_versions,
                )?;
                needed_ranges.extend(blocked_ranges);
            }
            transaction
                .commit()
                .await
                .context(inbound::StoreAccessSnafu)?;
            return Ok(InboundUpdateOutcome {
                event_batches: Vec::new(),
                needed_ranges,
                observed_available,
            });
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
        let touched_dataset_slices = replay::load_touched_dataset_slices(
            transaction.as_mut(),
            group_id,
            &touched_dataset_rows,
        )
        .await
        .context(inbound::StoreAccessSnafu)?;
        let mut working_datasets =
            replay::materialise_dataset_slices(&loaded_schemas, touched_dataset_slices);
        let hosted_group_ids = self
            .group_memberships
            .snapshot()
            .group_ids()
            .copied()
            .collect::<HashSet<_>>();
        let hosted_groups = transaction
            .load_replication_groups_for_ids(&hosted_group_ids)
            .await
            .context(inbound::StoreAccessSnafu)?;
        let mut listener_read_token = Self::read_token_from_groups(hosted_groups);
        if listener_read_token.group_version(&group_id).is_none() {
            listener_read_token = listener_read_token
                .with_group_version(group_id, local_group.version_vector.clone());
        }
        let mut event_batches = Vec::new();
        for ready_update in &apply_plan.ready_chain {
            let applied_batch =
                apply_one_update(&mut local_group, &mut working_datasets, ready_update)?;
            listener_read_token = listener_read_token
                .with_group_version(group_id, local_group.version_vector.clone());
            if !applied_batch.row_changes.is_empty() {
                event_batches.push(ListenerDataChanges {
                    read_token: listener_read_token.clone(),
                    row_changes: applied_batch.row_changes,
                });
            }
            Self::apply_dataset_row_patches(transaction.as_mut(), applied_batch.row_patches)
                .await
                .context(inbound::StoreAccessSnafu)?;
            transaction
                .mark_replication_update_applied(&group_id, ready_update.update_id)
                .await
                .context(inbound::StoreAccessSnafu)?;
        }
        for blocked_update in &apply_plan.blocked_updates {
            let blocked_ranges = Self::missing_ranges_for_update_frontier(
                &local_group.version_vector,
                blocked_update.group_id,
                blocked_update.update_id,
                &blocked_update.read_versions,
            )?;
            needed_ranges.extend(blocked_ranges);
        }
        let applied_versions = local_group.version_vector.clone();
        transaction
            .update_replication_group_version_vector(&group_id, applied_versions)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)?;
        Ok(InboundUpdateOutcome {
            event_batches,
            needed_ranges,
            observed_available,
        })
    }

    /// Persist one catch-up batch before emitting catch-up notifications.
    ///
    /// Each update is still processed in its own store transaction, but the
    /// derived catch-up notifications are accumulated until the batch finishes
    /// or fails. Listener notifications still follow each committed update so a
    /// later batch failure does not hide already-applied data changes.
    async fn persist_apply_and_notify_update_batch(
        &mut self,
        batch_sender: MemberIdentity,
        message: WireUpdateBatchMessage,
    ) -> Result<(), InboundDeliveryError> {
        let group_id = message.group_id;
        let mut observed_available = Vec::new();
        let mut needed_ranges = Vec::new();
        for update in message.updates {
            let outcome = match self
                .persist_and_apply_update(
                    InboundUpdateOrigin::Forwarder {
                        sender: batch_sender.clone(),
                    },
                    update,
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.notify_catch_up_available(group_id, observed_available);
                    self.notify_catch_up_needed(group_id, needed_ranges);
                    return Err(error);
                }
            };
            observed_available.extend(outcome.observed_available);
            needed_ranges.extend(outcome.needed_ranges);
            if let Err(error) =
                notify_listener_batches(self.listener.clone(), outcome.event_batches).await
            {
                self.notify_catch_up_available(group_id, observed_available);
                self.notify_catch_up_needed(group_id, needed_ranges);
                return Err(error);
            }
        }
        self.notify_catch_up_available(group_id, observed_available);
        self.notify_catch_up_needed(group_id, needed_ranges);
        Ok(())
    }

    fn handle_update(
        &mut self,
        context: InboundDeliveryContext,
        sender: MemberIdentity,
        message: WireUpdateMessage,
    ) -> HandlerResult {
        Handled::block_on(self, async move |mut async_self| {
            let group_id = message.group_id;
            let reply = async_self
                .persist_and_apply_update(InboundUpdateOrigin::Producer { sender }, message)
                .await;
            let error = match reply {
                Ok(outcome) => {
                    async_self.notify_catch_up_available(group_id, outcome.observed_available);
                    async_self.notify_catch_up_needed(group_id, outcome.needed_ranges);
                    notify_listener_batches(async_self.listener.clone(), outcome.event_batches)
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
            Handled::OK
        })
    }

    fn handle_update_batch(
        &mut self,
        context: InboundDeliveryContext,
        batch_sender: MemberIdentity,
        message: WireUpdateBatchMessage,
    ) -> HandlerResult {
        // Keep batch application on the component's blocking path. These
        // helpers borrow component state mutably across awaits, and the
        // component is intentionally not Sync, so the future cannot be safely
        // detached with spawn_local. Blocking also prevents live updates or
        // publishes from interleaving between batch elements and making
        // listener ordering or catch-up notifications reflect an artificial
        // intermediate state.
        Handled::block_on(self, async move |mut async_self| {
            let error = async_self
                .persist_apply_and_notify_update_batch(batch_sender, message)
                .await
                .err();
            if let Some(error) = error {
                let failure = InboundDeliveryFailure::new(context, error);
                let action = async_self.record_inbound_failure(&failure);
                panic_if_fatal_inbound_failure(action, &failure);
            }
            Handled::OK
        })
    }

    fn handle_publish_changes(
        &mut self,
        ask: Ask<PublishChangesRequest, Result<PublishReceipt, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = match async_self.publish_changes_transactionally(request).await {
                Ok(prepared_publish) => {
                    let update_id = prepared_publish.update_id;
                    let read_token = prepared_publish.read_token.clone();
                    async_self.submit_group_update(&prepared_publish);
                    async_self.notify_catch_up_available(
                        prepared_publish.group_id,
                        vec![UpdateRangeMessage::from(update_id)],
                    );
                    match notify_listener_batches(
                        async_self.listener.clone(),
                        vec![ListenerDataChanges {
                            read_token: read_token.clone(),
                            row_changes: prepared_publish.row_changes,
                        }],
                    )
                    .await
                    {
                        Ok(()) => Ok(PublishReceipt {
                            update_id,
                            read_token,
                        }),
                        Err(error) => Err(error).boxed().context(ApiExternalSnafu),
                    }
                }
                Err(error) => Err(error).boxed().context(ApiExternalSnafu),
            };
            async_self.reply_api(promise, "publish_changes", reply);
            Handled::OK
        })
    }

    fn handle_snapshot_rows(
        &mut self,
        ask: Ask<SnapshotRowsRequest, Result<SnapshotRows, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .snapshot_rows_from_store(request)
                .await
                .boxed()
                .context(ApiExternalSnafu);
            async_self.reply_api(promise, "snapshot_rows", reply);
            Handled::OK
        })
    }

    fn handle_request_summary(
        &mut self,
        ask: Ask<SummaryRequest, Result<Summary, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        if request.target == self.local_member {
            if let Err(error) = self.validate_summary_request(&request) {
                let reply = Err(error).boxed().context(ApiExternalSnafu);
                self.reply_api(promise, "request_summary", reply);
                return Handled::OK;
            }
            let responder = self.local_member.clone();
            let store = self.store.clone();
            return Handled::block_on(self, async move |async_self| {
                let reply = async {
                    let has_versions =
                        Self::load_summary_versions_from_store(store, request.group_id)
                            .await
                            .context(summary::StoreAccessSnafu)?;
                    let has_versions = has_versions.context(summary::UnknownGroupSnafu {
                        group_id: request.group_id,
                    })?;
                    Ok::<_, SummaryError>(Self::summary_for_request(
                        request.group_id,
                        responder,
                        has_versions,
                    ))
                }
                .await
                .boxed()
                .context(ApiExternalSnafu);
                async_self.reply_api(promise, "request_summary", reply);
                Handled::OK
            });
        }
        // Temporary glue: public runtime API requests currently enter through
        // this component and are then routed to the dedicated summary manager.
        // See flotsync-ozi for revisiting this component boundary.
        self.summary_request_manager
            .tell(SummaryRequestManagerMessage::RequestSummary(Ask::new(
                promise, request,
            )));
        Handled::OK
    }

    /// Inspect local progress after a peer summary and derive catch-up notifications.
    async fn inspect_summary_catch_up(
        store: Arc<dyn ReplicationStore>,
        local_member: MemberIdentity,
        summary: Summary,
    ) -> Result<Option<SummaryCatchUpObservation>, InboundDeliveryError> {
        let mut transaction = store
            .begin_read_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let Some(persisted_group) = transaction
            .load_replication_group(&summary.group_id)
            .await
            .context(inbound::StoreAccessSnafu)?
        else {
            return Ok(None);
        };
        let pending_update_ids = transaction
            .load_replication_update_ids(
                &summary.group_id,
                ReplicationUpdateFilter::PendingApply,
                None,
            )
            .await
            .context(inbound::StoreAccessSnafu)?;
        let local_group =
            LoadedGroupMeta::from_replication_group_record(&local_member, persisted_group)
                .context(inbound::InvalidPersistedGroupSnafu {
                    group_id: summary.group_id,
                })?;
        let summary_needed_ranges = local_group
            .version_vector
            .missing_version_ranges_to(&summary.has_versions)
            .into_iter()
            .map(UpdateRangeMessage::from)
            .collect_vec();
        let observed_available = pending_update_ids
            .into_iter()
            .map(UpdateRangeMessage::from)
            .collect_vec();
        let needed_ranges = subtract_available_ranges(&summary_needed_ranges, &observed_available);
        Ok(Some(SummaryCatchUpObservation {
            group_id: summary.group_id,
            observed_available,
            needed_ranges,
        }))
    }

    fn handle_observed_summary(&mut self, summary: Summary) -> HandlerResult {
        // Summary catch-up is advisory: it reads a store snapshot and sends
        // catch-up notifications, but it does not mutate runtime component
        // state. Running it asynchronously can at worst produce stale
        // best-effort ranges, which the catch-up manager de-duplicates against
        // later availability observations.
        self.spawn_local(move |async_self| async move {
            let store = async_self.store.clone();
            let local_member = async_self.local_member.clone();
            if let Some(observation) = Self::inspect_summary_catch_up(store, local_member, summary)
                .await
                .whatever_benign("failed to inspect local progress for summary catch-up")?
            {
                async_self.notify_catch_up_available(
                    observation.group_id,
                    observation.observed_available,
                );
                async_self.notify_catch_up_needed(observation.group_id, observation.needed_ranges);
            } else {
                // The summary refers to a group this runtime no longer hosts.
            }
            Handled::OK
        });
        Handled::OK
    }

    fn handle_create_group(
        &mut self,
        ask: Ask<CreateGroupRequest, Result<GroupId, ApiError>>,
    ) -> HandlerResult {
        let (promise, req) = ask.take();
        let created_group = self.prepare_created_group(req);
        let (group_id, members) = match created_group {
            Ok(created_group) => created_group,
            Err(error) => {
                let reply = Err(error).boxed().context(ApiExternalSnafu);
                self.reply_api(promise, "create_group", reply);
                return Handled::OK;
            }
        };
        Handled::block_on(self, async move |mut async_self| {
            let prepared_bootstrap = Self::prepare_group_bootstrap(
                &async_self.security,
                async_self.max_inline_bootstrap_public_key_bundles,
                group_id,
                &members,
            )
            .await;
            let prepared_bootstrap = match prepared_bootstrap {
                Ok(prepared_bootstrap) => prepared_bootstrap,
                Err(error) => {
                    let reply = Err(error).boxed().context(ApiExternalSnafu);
                    async_self.reply_api(promise, "create_group", reply);
                    return Handled::OK;
                }
            };
            let record = async_self.build_replication_group_record(
                group_id,
                &members,
                prepared_bootstrap.security_material,
            );
            let persisted_group = async_self.store_new_replication_group(record).await;
            let reply = match persisted_group {
                Ok(persisted_group) => {
                    match async_self.install_group_membership_view(persisted_group) {
                        Ok(()) => {
                            async_self.submit_group_bootstrap_messages(
                                prepared_bootstrap.bootstrap_message,
                            );
                            Ok::<GroupId, GroupInstallError>(group_id)
                                .boxed()
                                .context(ApiExternalSnafu)
                        }
                        Err(error) => Err(error).boxed().context(ApiExternalSnafu),
                    }
                }
                Err(error) => Err(error).boxed().context(ApiExternalSnafu),
            };
            async_self.reply_api(promise, "create_group", reply);
            Handled::OK
        })
    }

    fn handle_change_group_membership(
        &mut self,
        ask: Ask<ChangeGroupMembershipRequest, Result<GroupMigration, ApiError>>,
    ) -> HandlerResult {
        let (promise, req) = ask.take();
        let _ = req;
        let reply = Err(unavailable_api("change_group_membership"));
        self.reply_api(promise, "change_group_membership", reply);
        Handled::OK
    }

    #[cfg(any(test, feature = "test-support"))]
    fn handle_test_install_group(
        &mut self,
        ask: Ask<(GroupId, GroupMembers), Result<(), GroupInstallError>>,
    ) -> HandlerResult {
        let (promise, (group_id, members)) = ask.take();
        // TODO(flotsync-sec.9): Remove this test-only message once production
        // group creation installs real group secrets through the normal API.
        let group_key = test_group_key(group_id);
        let security_material = match self.security.seal_group_secret(group_id.0, &group_key) {
            Ok(security_material) => security_material,
            Err(source) => {
                let _ = promise.fulfil(Err(GroupInstallError::TestGroupSecurity {
                    group_id,
                    source,
                }));
                return Handled::OK;
            }
        };
        let record = self.build_replication_group_record(group_id, &members, security_material);
        Handled::block_on(self, async move |mut async_self| {
            let persisted_group = async_self.store_new_replication_group(record).await;
            let reply = match persisted_group {
                Ok(persisted_group) => async_self.install_group_membership_view(persisted_group),
                Err(error) => Err(error),
            };
            let _ = promise.fulfil(reply);
            Handled::OK
        })
    }

    #[cfg(test)]
    #[allow(
        clippy::unused_self,
        reason = "Kompact test messages use the same component method shape as production handlers."
    )]
    fn handle_test_ping(&mut self, ask: Ask<(), ()>) -> HandlerResult {
        let (promise, ()) = ask.take();
        let _ = promise.fulfil(());
        Handled::OK
    }

    #[cfg(test)]
    fn handle_test_apply_update(
        &mut self,
        ask: Ask<(MemberIdentity, UpdateMessage), Result<(), InboundDeliveryError>>,
    ) -> HandlerResult {
        let (promise, (sender, message)) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = match async_self
                .persist_and_apply_update(
                    InboundUpdateOrigin::Producer { sender },
                    WireUpdateMessage::from(message),
                )
                .await
            {
                Ok(outcome) => {
                    notify_listener_batches(async_self.listener.clone(), outcome.event_batches)
                        .await
                }
                Err(error) => Err(error),
            };
            let _ = promise.fulfil(reply);
            Handled::OK
        })
    }

    #[cfg(test)]
    fn handle_test_apply_update_batch(
        &mut self,
        ask: Ask<(MemberIdentity, UpdateBatchMessage), Result<(), InboundDeliveryError>>,
    ) -> HandlerResult {
        let (promise, (batch_sender, message)) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .persist_apply_and_notify_update_batch(
                    batch_sender,
                    WireUpdateBatchMessage::from(message),
                )
                .await;
            let _ = promise.fulfil(reply);
            Handled::OK
        })
    }
}

impl ComponentLifecycle for ReplicationRuntimeComponent {
    fn on_start(&mut self) -> HandlerResult {
        self.max_inline_bootstrap_public_key_bundles =
            self.read_max_inline_bootstrap_public_key_bundles();
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
            Handled::OK
        })
    }

    fn on_stop(&mut self) -> HandlerResult {
        Handled::OK
    }

    fn on_kill(&mut self) -> HandlerResult {
        Handled::OK
    }
}

impl Require<ReliableDeliveryPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> HandlerResult {
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
    fn handle(&mut self, indication: GroupBroadcastPortIndication) -> HandlerResult {
        let GroupBroadcastPortIndication::Deliver(deliver) = indication;
        match self.handle_group_delivery(&deliver) {
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

    fn receive_local(&mut self, msg: Self::Message) -> HandlerResult {
        match msg {
            ReplicationRuntimeMessage::PublishChanges(ask) => self.handle_publish_changes(ask),
            ReplicationRuntimeMessage::SnapshotRows(ask) => self.handle_snapshot_rows(ask),
            ReplicationRuntimeMessage::RequestSummary(ask) => self.handle_request_summary(ask),
            ReplicationRuntimeMessage::CreateGroup(ask) => self.handle_create_group(ask),
            ReplicationRuntimeMessage::ChangeGroupMembership(ask) => {
                self.handle_change_group_membership(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeMessage::Test(ReplicationRuntimeTestMessage::Ping(ask)) => {
                self.handle_test_ping(ask)
            }
            #[cfg(any(test, feature = "test-support"))]
            ReplicationRuntimeMessage::Test(ReplicationRuntimeTestMessage::InstallGroup(ask)) => {
                self.handle_test_install_group(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeMessage::Test(ReplicationRuntimeTestMessage::ApplyUpdate(ask)) => {
                self.handle_test_apply_update(ask)
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
) -> HandlerResult {
    panic_if_fatal_inbound_failure(action, failure);
    Handled::OK
}

async fn notify_listener_batches(
    listener: Arc<dyn ReplicationEventListener>,
    event_batches: Vec<ListenerDataChanges>,
) -> Result<(), InboundDeliveryError> {
    for event_batch in event_batches {
        if event_batch.row_changes.is_empty() {
            continue;
        }
        listener
            .on_event(ReplicationEvent::DataChanged {
                read_token: event_batch.read_token,
                rows: Box::new(VecRowProvider::new(event_batch.row_changes)),
            })
            .await
            .context(inbound::NotifyListenerSnafu)?;
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
            InboundDeliveryError::BootstrapSenderNotInGroup {
                group_id,
                sender: MemberIdentity::from_array(["runtime", "sender"]),
            }
            .failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::ConflictingPersistedUpdate {
                group: group_id,
                update: update_id,
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

    #[test]
    fn bootstrap_membership_validation_rejects_sender_outside_group() {
        let group_id = GroupId(Uuid::from_u128(92));
        let local_member = MemberIdentity::from_array(["runtime", "local"]);
        let sender = MemberIdentity::from_array(["runtime", "sender"]);
        let members = GroupMembers::from_ordered_members(vec![local_member.clone()])
            .expect("member set should build");

        let error = ReplicationRuntimeComponent::validate_bootstrap_membership(
            group_id,
            &members,
            &local_member,
            &sender,
        )
        .expect_err("sender outside the group should be rejected");

        match error {
            InboundDeliveryError::BootstrapSenderNotInGroup {
                group_id: rejected_group_id,
                sender: rejected_sender,
            } => {
                assert_eq!(rejected_group_id, group_id);
                assert_eq!(rejected_sender, sender);
            }
            other => panic!("unexpected bootstrap membership error: {other:?}"),
        }
    }
}

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
        ChangeGroupMembershipError,
        ConflictingExistingGroupSnafu,
        CreateGroupError,
        CreatorNotInMembersSnafu,
        DuplicateGroupSnafu,
        GroupActivationError,
        GroupInstallError,
        InboundDeliveryError,
        InboundFailureAction,
        InvalidGroupSnafu,
        InvalidMembersSnafu,
        LocalMemberMissingSnafu,
        PendingGroupActivationResumeSnafu,
        PublishChangesError,
        ReplayPendingDecisionSnafu,
        RuntimeStartupError,
        SecuritySnafu,
        SnapshotRowsError,
        StoreGroupSnafu,
        StoreStartupSnafu,
        SummaryError,
        activation,
        change_membership,
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
    pending_group,
    replay,
    summary_request_manager::SummaryRequestManagerMessage,
};
#[cfg(any(test, feature = "test-support"))]
use crate::api::MemberKeyId;
#[cfg(test)]
use crate::codecs::messages::UpdateBatchMessage;
#[cfg(any(test, feature = "test-support"))]
use crate::test_support::{test_group_key, test_public_member_keys};
use crate::{
    MAX_VERSION_VALUE,
    api::{
        ApiError,
        ApiExternalSnafu,
        BatchProvider,
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        DatasetId,
        DatasetRowStatePatch,
        DatasetRowStateWrite,
        DatasetUpdateRecord,
        EncryptedGroupSecurityMaterial,
        GroupInvitation,
        GroupInvitationResponder,
        GroupMemberKeys,
        GroupSchema,
        InitialSnapshot,
        ListenerError,
        MigrationId,
        MigrationProposal,
        MigrationProposalResponder,
        PendingGroupActivationRecord,
        PendingGroupDecisionRecord,
        PendingGroupWorkKey,
        PolicyDecision,
        ProviderExternalSnafu,
        PublishChangesRequest,
        PublishReceipt,
        ReadToken,
        RejectionReason,
        ReplicationConfig,
        ReplicationEvent,
        ReplicationEventListener,
        ReplicationGroupMaterialRecord,
        ReplicationGroupRecord,
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
        SnapshotRowsRequest,
        SnapshotValueRowBatch,
        SnapshotValueRows,
        StoreError,
        Summary,
        SummaryRequest,
        providers::VecRowProvider,
        security::{
            AssessPublicKeyBundleRequest,
            PublicKeyBundleReport,
            RecordPublicKeyBundleFeedbackRequest,
        },
    },
    codecs::messages::{
        BootstrapMemberKeyMessage,
        GroupInvitationMessage,
        GroupSetupKey,
        GroupSetupMessage,
        MigrationProposalMessage,
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
        shared::{DeliveryClass, MessageId, PlaintextPayload, ReliableMessageScope},
    },
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::{GroupMembers, GroupMemberships, SharedGroupMemberships},
    versions::{UpdateId, VersionVector},
};
use flotsync_messages::proto::{DecodeProtoView, EncodeProto};
use flotsync_security::{GROUP_CIPHER_SUITE_CHACHA20_POLY1305, PublicKeyBundle};
use flotsync_utils::{
    BoxFuture,
    KClaimablePromise,
    OptionExt as _,
    ResultExt as _,
    kompact_config::ConfigReadExt as _,
};
use futures_util::FutureExt;
use itertools::Itertools;
use kompact::prelude::*;
use roaring::RoaringBitmap;
use smallvec::{SmallVec, smallvec};
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};
use uuid::Uuid;

/// Security records and private wire setup prepared for one new group.
pub(super) struct PreparedGroupSetup {
    security_material: EncryptedGroupSecurityMaterial,
    group_setup: Arc<GroupSetupMessage>,
}

impl PreparedGroupSetup {
    /// Return the plaintext group setup prepared for group creation.
    #[cfg(test)]
    pub(super) fn group_setup(&self) -> &GroupSetupMessage {
        self.group_setup.as_ref()
    }
}

/// Listener notification batches with inline storage for the common single batch.
type ListenerDataChangeBatches = SmallVec<[ListenerDataChanges; 1]>;

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
    event_batches: ListenerDataChangeBatches,
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

/// Locally prepared membership migration before outbound reliable messages are sent.
struct PreparedMembershipMigration {
    migration_id: MigrationId,
    final_versions: VersionVector,
    group_schema: GroupSchema,
    initial_snapshot: InitialSnapshot,
    prepared_setup: PreparedGroupSetup,
    /// Sparse proposed-member indices for newly added recipients.
    added_member_indices: RoaringBitmap,
    group_name: Option<String>,
    message: Option<String>,
}

/// Proposed member set plus recipient classifications for a membership change.
struct ProposedMembershipChange {
    proposed_members: GroupMembers,
    added_member_indices: RoaringBitmap,
}

/// Encoded migration payloads and sparse recipient classes ready for fan-out.
struct PreparedMembershipDispatch {
    migration_id: MigrationId,
    group_setup: Arc<GroupSetupMessage>,
    migration_payload: bytes::Bytes,
    invitation_payload: bytes::Bytes,
    added_member_indices: RoaringBitmap,
}

/// Result of activating accepted group work into externally readable row state.
struct PendingGroupActivationOutcome {
    read_token: ReadToken,
    row_changes: Vec<RowChange>,
}

/// Verified target-group material together with its current activation state.
struct ValidatedInboundGroupSetup {
    material: ReplicationGroupMaterialRecord,
    already_active: bool,
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
    /// Store transaction that pins the snapshot view until the provider is drained or dropped.
    transaction: Option<Box<dyn ReplicationStoreReadTransaction>>,
    /// Group whose local row state is being streamed.
    group_id: GroupId,
    /// Dataset ids still waiting to be scanned.
    datasets: HashSet<DatasetId>,
    /// Schemas for all requested datasets, loaded up front so snapshot batches
    /// can project stored state into value rows without exposing CRDT state.
    schemas: HashMap<DatasetId, SchemaSource>,
    /// Dataset currently being scanned across batches.
    current_dataset: Option<DatasetId>,
    /// Exclusive lower bound for the current dataset scan.
    ///
    /// `None` starts before the first row. `Some(row_key)` means the next store
    /// scan requests rows with `row_key > after_row_key`.
    after_row_key: Option<RowKey>,
    max_rows_per_batch: NonZeroUsize,
    /// Whether retained tombstones should be included as tombstoned value rows.
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
}

impl BatchProvider for StoreSnapshotRowProvider {
    type Batch = SnapshotValueRowBatch;

    fn new_batch(&self) -> Self::Batch {
        SnapshotValueRowBatch::empty()
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

                let schema = self
                    .schemas
                    .get(&dataset_id)
                    .expect("snapshot provider datasets must have loaded schemas")
                    .clone();
                let rows = reuse.prepare(schema, self.max_rows_per_batch.get());
                for record in batch.rows {
                    if record.tombstoned && !self.include_tombstones {
                        continue;
                    }
                    let row_id = RowId {
                        group_id: self.group_id,
                        dataset_id: dataset_id.clone(),
                        row_key: record.row_id,
                    };
                    rows.push_row_read(row_id, record.tombstoned, &record.snapshot)
                        .boxed()
                        .context(ProviderExternalSnafu)?;
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

/// Application response to one pending group decision replayed through the listener.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingGroupDecisionResponse {
    /// Stable idempotence key of the pending decision being resolved.
    key: PendingGroupWorkKey,
    /// Requested resolution for the pending decision.
    response: PendingGroupDecisionResponseKind,
}

/// Resolution requested by a listener-facing pending group responder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PendingGroupDecisionResponseKind {
    /// Accept the pending decision and move it into activation work.
    Accept,
    /// Reject the pending decision and discard it.
    Reject {
        /// Application-supplied reason for the rejection.
        reason: RejectionReason,
    },
}

/// Local-actor messages understood by [`ReplicationRuntimeComponent`].
///
/// This is the imperative bridge surface between the public async runtime
/// handle and the Kompact-hosted replication component.
#[derive(Debug)]
pub enum ReplicationRuntimeMessage {
    /// Return the local member's shareable public key bundle through the component interface.
    LocalPublicKeyBundle(Ask<(), Result<PublicKeyBundle, ApiError>>),
    /// Assess one decoded public key bundle through the component interface.
    AssessPublicKeyBundle(
        Ask<AssessPublicKeyBundleRequest, Result<PublicKeyBundleReport, ApiError>>,
    ),
    /// Record public key bundle feedback through the component interface.
    RecordPublicKeyBundleFeedback(Ask<RecordPublicKeyBundleFeedbackRequest, Result<(), ApiError>>),
    /// Submit one local publish request through the component interface.
    PublishChanges(Ask<PublishChangesRequest, Result<PublishReceipt, ApiError>>),
    /// Request a local snapshot stream through the component interface.
    SnapshotRows(Ask<SnapshotRowsRequest, Result<SnapshotValueRows, ApiError>>),
    /// Ask one group member for its current group version vector.
    RequestSummary(Ask<SummaryRequest, Result<Summary, ApiError>>),
    /// Create one new fixed-membership group through the component interface.
    CreateGroup(Ask<CreateGroupRequest, Result<GroupId, ApiError>>),
    /// Request one group-membership change through the component interface.
    ChangeGroupMembership(Ask<ChangeGroupMembershipRequest, Result<MigrationId, ApiError>>),
    /// Resolve one listener-mediated pending group decision through the component.
    PendingGroupDecisionResponse(Ask<PendingGroupDecisionResponse, Result<(), ApiError>>),
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
    config: ReplicationConfig,
    security: DeliverySecurity,
    group_memberships: SharedGroupMemberships,
    summary_request_manager: ActorRefStrong<SummaryRequestManagerMessage>,
    catch_up_manager: ActorRefStrong<CatchUpManagerMessage>,
    /// Resolved group-size limit for including inline public key bundles in bootstrap messages.
    max_inline_bootstrap_public_key_bundles: usize,
}

/// Identity and membership views shared by runtime logic components.
#[derive(Clone)]
pub(super) struct RuntimeIdentityContext {
    pub(super) local_member: MemberIdentity,
    pub(super) group_memberships: SharedGroupMemberships,
}

/// Application-facing services consumed by the replication runtime component.
#[derive(Clone)]
pub(super) struct RuntimeApplicationServices {
    pub(super) store: Arc<dyn ReplicationStore>,
    pub(super) listener: Arc<dyn ReplicationEventListener>,
    pub(super) config: ReplicationConfig,
}

/// Security services used for delivery and bootstrap material handling.
#[derive(Clone)]
pub(super) struct RuntimeSecurityContext {
    pub(super) security: DeliverySecurity,
}

/// Actor dependencies created by the runtime logic topology.
pub(super) struct RuntimeComponentActors {
    pub(super) summary_request_manager: ActorRefStrong<SummaryRequestManagerMessage>,
    pub(super) catch_up_manager: ActorRefStrong<CatchUpManagerMessage>,
}

struct ComponentBackedPendingGroupResponder {
    /// Runtime component mailbox used to apply listener decisions on the component thread.
    runtime_ref: ActorRefStrong<ReplicationRuntimeMessage>,
    /// Stable store key for the pending work that produced this responder.
    work_key: PendingGroupWorkKey,
}

impl ComponentBackedPendingGroupResponder {
    /// Send the listener's decision back through the runtime component mailbox.
    fn respond(
        self,
        response: PendingGroupDecisionResponseKind,
    ) -> BoxFuture<'static, Result<(), ApiError>> {
        let Self {
            runtime_ref,
            work_key,
        } = self;
        let request = PendingGroupDecisionResponse {
            key: work_key,
            response,
        };
        let future = runtime_ref.ask_with(move |promise| {
            ReplicationRuntimeMessage::PendingGroupDecisionResponse(Ask::new(promise, request))
        });
        async move {
            match future.await {
                Ok(reply) => reply,
                Err(_) => Err(ApiError::RuntimeUnavailable),
            }
        }
        .boxed()
    }
}

impl GroupInvitationResponder for ComponentBackedPendingGroupResponder {
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>> {
        (*self).respond(PendingGroupDecisionResponseKind::Accept)
    }

    fn reject(
        self: Box<Self>,
        reason: RejectionReason,
    ) -> BoxFuture<'static, Result<(), ApiError>> {
        (*self).respond(PendingGroupDecisionResponseKind::Reject { reason })
    }
}

impl MigrationProposalResponder for ComponentBackedPendingGroupResponder {
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>> {
        (*self).respond(PendingGroupDecisionResponseKind::Accept)
    }

    fn reject(
        self: Box<Self>,
        reason: RejectionReason,
    ) -> BoxFuture<'static, Result<(), ApiError>> {
        (*self).respond(PendingGroupDecisionResponseKind::Reject { reason })
    }
}

impl ReplicationRuntimeComponent {
    /// Construct one replication runtime component for one local member.
    pub(super) fn new(
        identity: RuntimeIdentityContext,
        services: RuntimeApplicationServices,
        security: RuntimeSecurityContext,
        actors: RuntimeComponentActors,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            group_broadcast: RequiredPort::uninitialised(),
            reliable_delivery: RequiredPort::uninitialised(),
            local_member: identity.local_member,
            store: services.store,
            listener: services.listener,
            config: services.config,
            security: security.security,
            group_memberships: identity.group_memberships,
            summary_request_manager: actors.summary_request_manager,
            catch_up_manager: actors.catch_up_manager,
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

    /// Validate stored material against the members and schema from accepted work.
    fn validate_activation_group_material(
        &self,
        group_id: GroupId,
        proposed_members: Vec<MemberIdentity>,
        group_schema: &GroupSchema,
        material: &ReplicationGroupMaterialRecord,
    ) -> Result<ReplicationGroupRecord, GroupActivationError> {
        let expected_members = GroupMembers::from_ordered_members(proposed_members)
            .context(activation::InvalidMembersSnafu)?;
        ensure!(
            expected_members.contains(&self.local_member),
            activation::LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );
        let initial_versions = VersionVector::initial(material.member_count());
        let group_record = material.clone().activate(initial_versions);
        let local_group = LoadedGroupMeta::from_replication_group_record(
            &self.local_member,
            group_record.clone(),
        )
        .context(activation::InvalidPersistedGroupSnafu { group_id })?;
        ensure!(
            local_group.members.ordered_members() == expected_members.ordered_members()
                && group_record.group_schema == *group_schema,
            activation::ConflictingGroupMaterialSnafu { group_id }
        );
        Ok(group_record)
    }

    async fn snapshot_rows_from_store(
        &mut self,
        request: SnapshotRowsRequest,
    ) -> Result<SnapshotValueRows, SnapshotRowsError> {
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

        let mut schemas = HashMap::with_capacity(request.datasets.len());
        for dataset_id in &request.datasets {
            let schema = self
                .store
                .load_dataset_schema(dataset_id)
                .await
                .context(snapshot::StoreAccessSnafu)?;
            let schema = schema.context(snapshot::MissingDatasetSchemaSnafu {
                dataset_id: dataset_id.clone(),
            })?;
            schemas.insert(dataset_id.clone(), schema);
        }

        let provider = StoreSnapshotRowProvider {
            transaction: Some(transaction),
            group_id: request.group_id,
            datasets: request.datasets,
            schemas,
            current_dataset: None,
            after_row_key: None,
            max_rows_per_batch: request.max_rows_per_batch,
            include_tombstones: request.include_tombstones,
        };
        Ok(SnapshotValueRows {
            group_id: request.group_id,
            read_token,
            rows: Box::new(provider),
        })
    }

    /// Persist one set of explicit row patches back into the replication store.
    async fn apply_dataset_row_patches(
        transaction: &mut dyn ReplicationStoreTransaction,
        patches: Vec<DatasetRowStatePatch>,
    ) -> Result<(), StoreError> {
        for patch in patches {
            transaction.apply_dataset_row_patch(patch).await?;
        }
        Ok(())
    }

    /// Build the canonical persisted record for one group definition whose
    /// exact member keys have already passed local membership validation.
    fn build_replication_group_record(
        &self,
        group_id: GroupId,
        member_keys: GroupMemberKeys,
        group_schema: GroupSchema,
        security_material: crate::api::EncryptedGroupSecurityMaterial,
    ) -> ReplicationGroupRecord {
        let material = self.build_replication_group_material_record(
            group_id,
            member_keys,
            group_schema,
            security_material,
        );
        let version_vector = VersionVector::initial(material.member_count());
        material.activate(version_vector)
    }

    /// Build stored group material without making the group active.
    fn build_replication_group_material_record(
        &self,
        group_id: GroupId,
        member_keys: GroupMemberKeys,
        group_schema: GroupSchema,
        security_material: crate::api::EncryptedGroupSecurityMaterial,
    ) -> ReplicationGroupMaterialRecord {
        NonZeroUsize::new(member_keys.len())
            .expect("group installation must keep members non-empty");
        let local_member_index = member_keys
            .member_index(&self.local_member)
            .expect("group installation validates the local member before persistence");
        ReplicationGroupMaterialRecord {
            group_id,
            member_keys,
            local_member_index,
            group_schema,
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

    /// Validate that group setup authorises both its sender and the local member.
    fn validate_group_setup_membership(
        group_id: GroupId,
        members: &GroupMembers,
        local_member: &MemberIdentity,
        sender: &MemberIdentity,
    ) -> Result<(), InboundDeliveryError> {
        let Some(sender_index) = members.member_index(sender) else {
            return Err(InboundDeliveryError::GroupSetupSenderNotInGroup {
                group_id,
                sender: sender.clone(),
            });
        };
        if sender_index != MemberIndex::new(0) {
            return Err(InboundDeliveryError::GroupSetupSenderNotFirstMember {
                group_id,
                sender: sender.clone(),
                sender_index: Some(sender_index),
            });
        }
        if !members.contains(local_member) {
            return Err(InboundDeliveryError::GroupSetupMissingLocalMember {
                group_id,
                local_member: local_member.clone(),
            });
        }
        Ok(())
    }

    /// Validate that pending work names a coherent member set including this runtime.
    fn validate_pending_group_members(
        &self,
        group_id: GroupId,
        proposed_members: &[MemberIdentity],
    ) -> Result<GroupMembers, InboundDeliveryError> {
        let members = GroupMembers::from_ordered_members(proposed_members.iter().cloned())
            .context(inbound::InvalidPendingGroupMembersSnafu)?;
        ensure!(
            members.contains(&self.local_member),
            inbound::PendingGroupMissingLocalMemberSnafu {
                group_id,
                local_member: self.local_member.clone(),
            }
        );
        Ok(members)
    }

    /// Resolve local policy for one validated group invitation.
    fn invitation_policy_decision(
        &self,
        invitation: &GroupInvitation,
    ) -> Result<PolicyDecision, InboundDeliveryError> {
        self.validate_pending_group_members(invitation.group_id, &invitation.proposed_members)?;
        Ok(match invitation.source {
            crate::api::GroupInvitationSource::Creation => {
                self.config.group_invitation_policy.creation
            }
            crate::api::GroupInvitationSource::Migration { .. } => {
                self.config.group_invitation_policy.migration_added_member
            }
        })
    }

    /// Resolve local policy for one migration proposal by comparing old and proposed members.
    async fn migration_policy_decision(
        &mut self,
        proposal: &MigrationProposal,
    ) -> Result<PolicyDecision, InboundDeliveryError> {
        let group_id = proposal.migration_id.new_group_id;
        let proposed_members =
            self.validate_pending_group_members(group_id, &proposal.proposed_members)?;
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let old_group_id = proposal.migration_id.old_group_id;
        let persisted_group = transaction
            .load_replication_group(&old_group_id)
            .await
            .context(inbound::StoreAccessSnafu)?
            .context(inbound::UnknownHostedGroupSnafu {
                group_id: old_group_id,
            })?;
        transaction
            .release()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                .context(inbound::InvalidPersistedGroupSnafu {
                    group_id: old_group_id,
                })?;
        let policy = &self.config.group_migration_policy;
        let mut decision = policy.epoch_change;

        for old_member in local_group.members.iter() {
            if !proposed_members.contains(&old_member) {
                decision = decision.most_restrictive(policy.member_removed);
            }
        }
        for proposed_member in proposed_members.iter() {
            if !local_group.members.contains(&proposed_member) {
                decision = decision.most_restrictive(policy.member_added);
            }
        }
        Ok(decision)
    }

    /// Resolve local policy for pending group work before accepting activation.
    async fn pending_group_policy_decision(
        &mut self,
        record: &PendingGroupDecisionRecord,
    ) -> Result<PolicyDecision, InboundDeliveryError> {
        if record.requires_snapshot_fetch() {
            return Ok(PolicyDecision::AutoReject);
        }
        match record {
            PendingGroupDecisionRecord::GroupInvitation(invitation) => {
                self.invitation_policy_decision(invitation)
            }
            PendingGroupDecisionRecord::MigrationProposal(proposal) => {
                self.migration_policy_decision(proposal).await
            }
        }
    }

    /// Notify the listener about already stored pending work.
    async fn notify_pending_group_decision_listener(
        &mut self,
        record: PendingGroupDecisionRecord,
    ) -> Result<(), InboundDeliveryError> {
        let key = record.key();
        let runtime_ref = self
            .ctx
            .actor_ref()
            .hold()
            .expect("replication runtime actor ref must be available while running");
        let responder = ComponentBackedPendingGroupResponder {
            runtime_ref,
            work_key: key,
        };
        self.listener
            .on_event(record.to_event(responder))
            .await
            .context(inbound::NotifyPendingGroupDecisionSnafu)
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
                existing.matches_definition(&record),
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

    /// Re-fire unresolved listener-mediated group decisions after startup.
    async fn replay_pending_group_decisions(
        &mut self,
        runtime_ref: ActorRefStrong<ReplicationRuntimeMessage>,
    ) -> Result<(), RuntimeStartupError> {
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(StoreStartupSnafu)?;
        let pending_work_items = transaction
            .load_pending_group_decisions()
            .await
            .context(StoreStartupSnafu)?;
        transaction.release().await.context(StoreStartupSnafu)?;

        for work in pending_work_items {
            let responder = ComponentBackedPendingGroupResponder {
                runtime_ref: runtime_ref.clone(),
                work_key: work.key(),
            };
            let event = work.to_event(responder);
            self.listener
                .on_event(event)
                .await
                .context(ReplayPendingDecisionSnafu)?;
        }
        Ok(())
    }

    /// Resume accepted group activations without re-asking the listener.
    async fn resume_pending_group_activations(&mut self) -> Result<(), RuntimeStartupError> {
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(StoreStartupSnafu)?;
        let pending_activations = transaction
            .load_pending_group_activations()
            .await
            .context(StoreStartupSnafu)?;
        transaction.release().await.context(StoreStartupSnafu)?;

        for activation in pending_activations {
            let outcome = self
                .activate_pending_group_record(activation)
                .await
                .context(PendingGroupActivationResumeSnafu)?;
            notify_pending_activation_data_changes(self.listener.clone(), outcome)
                .await
                .context(activation::NotifyListenerSnafu)
                .context(PendingGroupActivationResumeSnafu)?;
        }
        Ok(())
    }

    /// Apply one listener response to a replayed pending group decision.
    async fn apply_pending_group_decision_response(
        &mut self,
        response: PendingGroupDecisionResponse,
    ) -> Result<(), ApiError> {
        let mut accepted_activation = None;
        let mut unsupported_activation = None;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .boxed()
            .context(ApiExternalSnafu)?;
        match response.response {
            PendingGroupDecisionResponseKind::Accept => {
                let group_id = response.key.group_id();
                let pending_work = transaction
                    .load_pending_group_decision(&group_id)
                    .await
                    .boxed()
                    .context(ApiExternalSnafu)?;
                if let Some(work) = pending_work.filter(|value| value.key() == response.key) {
                    if work.requires_snapshot_fetch() {
                        transaction
                            .remove_pending_group_decision(response.key)
                            .await
                            .boxed()
                            .context(ApiExternalSnafu)?;
                        transaction
                            .remove_inactive_replication_group_material(group_id)
                            .await
                            .boxed()
                            .context(ApiExternalSnafu)?;
                        unsupported_activation = Some(group_id);
                    } else {
                        let activation = work.into_activation();
                        transaction
                            .upsert_pending_group_activation(activation.clone())
                            .await
                            .boxed()
                            .context(ApiExternalSnafu)?;
                        accepted_activation = Some(activation);
                    }
                }
            }
            PendingGroupDecisionResponseKind::Reject { .. } => {
                let rejected_group_id = match response.key {
                    PendingGroupWorkKey::GroupInvitation { group_id, .. } => group_id,
                    PendingGroupWorkKey::MigrationProposal { migration_id } => {
                        migration_id.new_group_id
                    }
                };
                transaction
                    .remove_pending_group_decision(response.key)
                    .await
                    .boxed()
                    .context(ApiExternalSnafu)?;
                transaction
                    .remove_inactive_replication_group_material(rejected_group_id)
                    .await
                    .boxed()
                    .context(ApiExternalSnafu)?;
            }
        }
        transaction
            .commit()
            .await
            .boxed()
            .context(ApiExternalSnafu)?;
        if let Some(group_id) = unsupported_activation {
            return activation::UnsupportedInitialSnapshotSnafu { group_id }
                .fail::<()>()
                .boxed()
                .context(ApiExternalSnafu);
        }
        if let Some(activation) = accepted_activation {
            let outcome = self
                .activate_pending_group_record(activation)
                .await
                .boxed()
                .context(ApiExternalSnafu)?;
            notify_pending_activation_data_changes(self.listener.clone(), outcome)
                .await
                .boxed()
                .context(ApiExternalSnafu)?;
        }
        Ok(())
    }

    /// Submit one encoded live update to the group-broadcast layer.
    fn submit_group_update(&mut self, prepared_publish: &PreparedLocalPublish) {
        self.group_broadcast.trigger(
            GroupBroadcastPortRequest::build_submit(DeliveryClass::BestEffort)
                .for_member_in_group(self.local_member.clone(), prepared_publish.group_id)
                .with_payload(prepared_publish.payload.clone()),
        );
    }

    /// Submit one runtime envelope through reliable delivery using its authority scope.
    fn submit_reliable_runtime_message(
        &mut self,
        recipient: MemberIdentity,
        message: &RuntimeMessage,
    ) {
        let payload = message.encode_proto_to_bytes();
        self.submit_reliable_runtime_payload(recipient, message.group_id(), payload);
    }

    /// Submit one already encoded runtime payload through reliable delivery.
    fn submit_reliable_runtime_payload(
        &mut self,
        recipient: MemberIdentity,
        scope_group_id: GroupId,
        payload: bytes::Bytes,
    ) {
        self.reliable_delivery
            .trigger(ReliableDeliveryPortRequest::Submit(
                ReliableDeliverySubmit {
                    envelope: ReliableMessageEnvelope::<PlaintextPayload> {
                        header: ReliableMessageHeader {
                            sender: self.local_member.clone(),
                            recipient,
                            message_id: MessageId(Uuid::new_v4()),
                            scope: ReliableMessageScope::Group {
                                group_id: scope_group_id,
                            },
                        },
                        payload: PlaintextPayload { bytes: payload },
                    },
                },
            ));
    }

    /// Fan one encoded creation invitation out to every remote target-group member.
    fn submit_group_creation_invitation_messages(
        &mut self,
        group_id: GroupId,
        group_setup: &GroupSetupMessage,
        payload: &bytes::Bytes,
    ) {
        let local_member = self.local_member.clone();
        for recipient in group_setup
            .members()
            .iter()
            .filter(|member| *member != &local_member)
            .cloned()
        {
            self.submit_reliable_runtime_payload(recipient, group_id, payload.clone());
        }
    }

    /// Send old-group migration proposals and new-group invitations for one change.
    fn submit_membership_migration_messages(&mut self, dispatch: &PreparedMembershipDispatch) {
        let proposed_members = dispatch.group_setup.members();
        // Index zero is the local member. Dispatch classifies only remote
        // recipients; every remote index absent from `added_member_indices`
        // necessarily names a continuing old-group member.
        for (member_index, recipient) in proposed_members.iter().enumerate().skip(1) {
            let member_index = u32::try_from(member_index).expect("group member indices fit u32");
            if dispatch.added_member_indices.contains(member_index) {
                self.submit_reliable_runtime_payload(
                    recipient.clone(),
                    dispatch.migration_id.new_group_id,
                    dispatch.invitation_payload.clone(),
                );
            } else {
                self.submit_reliable_runtime_payload(
                    recipient.clone(),
                    dispatch.migration_id.old_group_id,
                    dispatch.migration_payload.clone(),
                );
            }
        }
    }

    /// Encode one proposal and one invitation before local activation consumes the snapshot.
    fn prepare_membership_dispatch(
        prepared: &PreparedMembershipMigration,
    ) -> PreparedMembershipDispatch {
        let proposed_members = prepared.prepared_setup.group_setup.members().to_vec();
        let proposal = MigrationProposal {
            migration_id: prepared.migration_id,
            final_versions: prepared.final_versions.clone(),
            proposed_members: proposed_members.clone(),
            group_schema: prepared.group_schema.clone(),
            initial_snapshot: prepared.initial_snapshot.clone(),
            group_name: prepared.group_name.clone(),
            message: prepared.message.clone(),
        };
        let proposal_message = MigrationProposalMessage::try_new(
            proposal,
            Arc::clone(&prepared.prepared_setup.group_setup),
        )
        .expect("prepared proposal members match prepared group setup");
        let migration_payload =
            RuntimeMessage::MigrationProposal(proposal_message).encode_proto_to_bytes();

        let invitation = GroupInvitation::new_migration(
            prepared.migration_id,
            proposed_members,
            prepared.group_schema.clone(),
            prepared.initial_snapshot.clone(),
            prepared.group_name.clone(),
            prepared.message.clone(),
        );
        let invitation_message = GroupInvitationMessage::try_new(
            invitation,
            Arc::clone(&prepared.prepared_setup.group_setup),
        )
        .expect("prepared invitation members match prepared group setup");
        let invitation_payload =
            RuntimeMessage::GroupInvitation(invitation_message).encode_proto_to_bytes();

        PreparedMembershipDispatch {
            migration_id: prepared.migration_id,
            group_setup: Arc::clone(&prepared.prepared_setup.group_setup),
            migration_payload,
            invitation_payload,
            added_member_indices: prepared.added_member_indices.clone(),
        }
    }

    /// Validate one create-group request and derive the canonical persisted
    /// group record that should be written if the request succeeds.
    fn prepare_created_group(
        &self,
        req: CreateGroupRequest,
    ) -> Result<(GroupId, GroupMembers, GroupSchema), CreateGroupError> {
        let requested_members = creator_first_member_order(req.members, &self.local_member)?;
        let members =
            GroupMembers::from_ordered_members(requested_members).context(InvalidMembersSnafu)?;
        ensure!(
            members.contains(&self.local_member),
            LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );

        let group_id = GroupId(Uuid::new_v4());
        Ok((group_id, members, req.group_schema))
    }

    /// Read the maximum group size for inlining bootstrap public key bundles.
    fn read_max_inline_bootstrap_public_key_bundles(&self) -> usize {
        self.ctx.config().read_or_default_warn(
            self.log(),
            &config_keys::BOOTSTRAP_MAX_INLINE_PUBLIC_KEY_BUNDLES,
        )
    }

    pub(super) async fn prepare_group_setup(
        security: &DeliverySecurity,
        max_inline_public_key_bundles: usize,
        group_id: GroupId,
        members: &GroupMembers,
    ) -> Result<PreparedGroupSetup, CreateGroupError> {
        let group_key = DeliverySecurity::generate_group_key()
            .boxed()
            .context(SecuritySnafu)?;
        let public_keys = security
            .public_keys_for_members(members)
            .await
            .boxed()
            .context(SecuritySnafu)?;
        let inline_public_bundles = members.len() <= max_inline_public_key_bundles;
        let setup_member_keys = public_keys.map_values(|public_keys| {
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
        let group_setup = GroupSetupMessage::new(
            members.ordered_members(),
            setup_member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            GroupSetupKey::from_group_key(group_key),
        )
        .boxed()
        .context(SecuritySnafu)?;
        Ok(PreparedGroupSetup {
            security_material,
            group_setup: Arc::new(group_setup),
        })
    }

    /// Translate group-bootstrap preparation errors into membership-change failures.
    fn map_create_group_error(error: CreateGroupError) -> ChangeGroupMembershipError {
        match error {
            CreateGroupError::LocalMemberMissing { local_member } => {
                ChangeGroupMembershipError::LocalMemberMissing { local_member }
            }
            CreateGroupError::CreatorNotInMembers { creator } => {
                ChangeGroupMembershipError::LocalMemberMissing {
                    local_member: creator,
                }
            }
            CreateGroupError::InvalidMembers { source } => {
                ChangeGroupMembershipError::InvalidMembers { source }
            }
            CreateGroupError::Security { source } => {
                ChangeGroupMembershipError::Security { source }
            }
        }
    }

    /// Compute canonical proposed membership and outbound recipient classifications.
    fn proposed_members_for_change(
        &self,
        current_members: &GroupMembers,
        req: &ChangeGroupMembershipRequest,
    ) -> Result<ProposedMembershipChange, ChangeGroupMembershipError> {
        let current_member_set = current_members.iter().collect::<HashSet<_>>();
        ensure!(
            current_member_set.contains(&self.local_member),
            change_membership::LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );
        let mut final_members = current_member_set.clone();
        for member in &req.remove_members {
            final_members.remove(member);
        }
        final_members.extend(req.add_members.iter().cloned());
        ensure!(
            final_members.remove(&self.local_member),
            change_membership::LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
        );

        let mut proposed_member_list = Vec::with_capacity(final_members.len() + 1);
        proposed_member_list.push(self.local_member.clone());
        let mut added_member_indices = RoaringBitmap::new();
        for member in final_members {
            let member_index = u32::try_from(proposed_member_list.len())
                .expect("group member indices fit into u32");
            if !current_member_set.contains(&member) {
                added_member_indices.insert(member_index);
            }
            proposed_member_list.push(member);
        }
        let proposed_members = GroupMembers::from_ordered_members(proposed_member_list)
            .context(change_membership::InvalidMembersSnafu)?;
        Ok(ProposedMembershipChange {
            proposed_members,
            added_member_indices,
        })
    }

    /// Prepare local state and outbound payloads for a membership change request.
    async fn prepare_membership_migration(
        &mut self,
        req: ChangeGroupMembershipRequest,
    ) -> Result<PreparedMembershipMigration, ChangeGroupMembershipError> {
        let old_group_id = req.group_id;
        let mut transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(change_membership::StoreAccessSnafu)?;
        let persisted_group = transaction
            .load_replication_group(&old_group_id)
            .await
            .context(change_membership::StoreAccessSnafu)?
            .context(change_membership::UnknownGroupSnafu {
                group_id: old_group_id,
            })?;
        let group_schema = persisted_group.group_schema.clone();
        let local_group =
            LoadedGroupMeta::from_replication_group_record(&self.local_member, persisted_group)
                .context(change_membership::InvalidPersistedGroupSnafu {
                    group_id: old_group_id,
                })?;
        let proposed_change = self.proposed_members_for_change(&local_group.members, &req)?;
        let final_versions = local_group.version_vector.clone();
        let initial_snapshot = pending_group::build_inline_initial_snapshot(
            transaction.as_mut(),
            old_group_id,
            &group_schema,
        )
        .await?;
        transaction
            .release()
            .await
            .context(change_membership::StoreAccessSnafu)?;

        let new_group_id = GroupId(Uuid::new_v4());
        let prepared_setup = Self::prepare_group_setup(
            &self.security,
            self.max_inline_bootstrap_public_key_bundles,
            new_group_id,
            &proposed_change.proposed_members,
        )
        .await
        .map_err(Self::map_create_group_error)?;
        let migration_id = MigrationId {
            old_group_id,
            new_group_id,
        };
        Ok(PreparedMembershipMigration {
            migration_id,
            final_versions,
            group_schema,
            initial_snapshot,
            prepared_setup,
            added_member_indices: proposed_change.added_member_indices,
            group_name: req.group_name,
            message: req.message,
        })
    }

    /// Build the stored group record represented by prepared bootstrap material.
    fn prepared_migration_group_record(
        &self,
        prepared: &PreparedMembershipMigration,
    ) -> Result<ReplicationGroupRecord, GroupActivationError> {
        let member_keys = GroupMemberKeys::from_ordered_member_keys(
            prepared.prepared_setup.group_setup.ordered_member_key_ids(),
        )
        .context(activation::InvalidMembersSnafu)?;
        Ok(self.build_replication_group_record(
            prepared.migration_id.new_group_id,
            member_keys,
            prepared.group_schema.clone(),
            prepared.prepared_setup.security_material.clone(),
        ))
    }

    /// Install locally prepared membership-change state before notifying remote members.
    async fn activate_prepared_membership_migration(
        &mut self,
        prepared: PreparedMembershipMigration,
    ) -> Result<PendingGroupActivationOutcome, GroupActivationError> {
        let group_id = prepared.migration_id.new_group_id;
        let record = self.prepared_migration_group_record(&prepared)?;
        let member_count = record.member_keys.len();
        let member_count =
            NonZeroUsize::new(member_count).expect("prepared migration members are non-empty");
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(activation::StoreAccessSnafu)?;
        let existing = transaction
            .load_replication_group(&group_id)
            .await
            .context(activation::StoreAccessSnafu)?;
        if existing.is_none() {
            transaction
                .insert_replication_group(record.clone())
                .await
                .context(activation::StoreAccessSnafu)?;
        }
        let row_changes = match prepared.initial_snapshot {
            InitialSnapshot::Empty => Vec::new(),
            InitialSnapshot::Inline(initial_state) => {
                pending_group::embed_inline_initial_snapshot(
                    transaction.as_mut(),
                    group_id,
                    member_count,
                    &prepared.group_schema,
                    initial_state,
                )
                .await?
            }
            InitialSnapshot::Metadata(_) => {
                return activation::UnsupportedInitialSnapshotSnafu { group_id }.fail();
            }
        };
        let active_groups = transaction
            .load_replication_groups()
            .await
            .context(activation::StoreAccessSnafu)?;
        let read_token = Self::read_token_from_groups(active_groups);
        transaction
            .commit()
            .await
            .context(activation::StoreAccessSnafu)?;
        self.install_group_membership_view(record)
            .context(activation::InstallGroupSnafu { group_id })?;
        Ok(PendingGroupActivationOutcome {
            read_token,
            row_changes,
        })
    }

    /// Activate accepted pending work using its atomically stored group material.
    async fn activate_pending_group_record(
        &mut self,
        record: PendingGroupActivationRecord,
    ) -> Result<PendingGroupActivationOutcome, GroupActivationError> {
        let activation_record = record.into_activation_record();
        let key = activation_record.key;
        let group_id = activation_record.group_id;
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(activation::StoreAccessSnafu)?;
        let material = transaction
            .load_replication_group_material(&group_id)
            .await
            .context(activation::StoreAccessSnafu)?
            .context(activation::MissingGroupMaterialSnafu { group_id })?;
        let group_record = self.validate_activation_group_material(
            group_id,
            activation_record.proposed_members,
            &activation_record.group_schema,
            &material,
        )?;
        let member_count = group_record.member_count();
        transaction
            .activate_replication_group(group_id, group_record.version_vector.clone())
            .await
            .context(activation::StoreAccessSnafu)?;
        let row_changes = match activation_record.initial_snapshot {
            InitialSnapshot::Empty => Vec::new(),
            InitialSnapshot::Inline(initial_state) => {
                pending_group::embed_inline_initial_snapshot(
                    transaction.as_mut(),
                    group_id,
                    member_count,
                    &activation_record.group_schema,
                    initial_state,
                )
                .await?
            }
            InitialSnapshot::Metadata(_) => {
                return activation::UnsupportedInitialSnapshotSnafu { group_id }.fail();
            }
        };
        transaction
            .remove_pending_group_activation(key)
            .await
            .context(activation::StoreAccessSnafu)?;
        let active_groups = transaction
            .load_replication_groups()
            .await
            .context(activation::StoreAccessSnafu)?;
        let read_token = Self::read_token_from_groups(active_groups);
        transaction
            .commit()
            .await
            .context(activation::StoreAccessSnafu)?;
        self.install_group_membership_view(group_record)
            .context(activation::InstallGroupSnafu { group_id })?;
        Ok(PendingGroupActivationOutcome {
            read_token,
            row_changes,
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
        let mut row_writes: HashMap<DatasetId, Vec<DatasetRowStateWrite>> = HashMap::new();
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
            .map(|(dataset_id, actions)| DatasetRowStatePatch {
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

    fn handle_inbound_pending_group_decision(
        &mut self,
        context: InboundDeliveryContext,
        deliver: ReliableDeliveryDeliver,
        record: PendingGroupDecisionRecord,
        group_setup: Arc<GroupSetupMessage>,
    ) -> HandlerResult {
        let group_id = record.group_id();
        let sender = deliver.envelope.header.sender.clone();
        let processed = deliver.processed;
        Handled::block_on(self, async move |mut async_self| {
            let reply = async {
                async_self
                    .install_pending_group_delivery(record, group_setup, sender)
                    .await?;
                processed
                    .complete()
                    .context(inbound::CompleteProcessedPromiseSnafu { group_id })
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

    /// Validate inbound setup and reuse matching encrypted material on replay.
    ///
    /// Validation intentionally finishes its read transaction before security
    /// checks because those checks may perform independent security-store
    /// operations. Kompact serialises work through this component, so its group
    /// state cannot change through the runtime between this phase and pending
    /// persistence. The later write transaction remains authoritative: it
    /// ensures exact group material and rejects conflicting target-keyed work.
    async fn validate_inbound_group_setup(
        &mut self,
        record: &PendingGroupDecisionRecord,
        group_setup: &GroupSetupMessage,
        sender: &MemberIdentity,
    ) -> Result<ValidatedInboundGroupSetup, InboundDeliveryError> {
        let group_id = record.group_id();
        let members = GroupMembers::from_ordered_members(group_setup.members().to_vec())
            .context(inbound::InvalidGroupSetupMembersSnafu)?;
        Self::validate_group_setup_membership(group_id, &members, &self.local_member, sender)?;
        let member_keys =
            GroupMemberKeys::from_ordered_member_keys(group_setup.ordered_member_key_ids())
                .context(inbound::InvalidGroupSetupMembersSnafu)?;
        let local_member_index = member_keys
            .member_index(&self.local_member)
            .expect("group setup membership validation requires the local member");
        let group_schema = record.group_schema().clone();
        let mut read_transaction = self
            .store
            .begin_read_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        let existing_material = read_transaction
            .load_replication_group_material(&group_id)
            .await
            .context(inbound::StoreAccessSnafu)?;
        let already_active = read_transaction
            .load_replication_group(&group_id)
            .await
            .context(inbound::StoreAccessSnafu)?
            .is_some();
        read_transaction
            .release()
            .await
            .context(inbound::StoreAccessSnafu)?;

        let material = if let Some(existing_material) = existing_material {
            let definition_matches = existing_material.matches_definition(
                group_id,
                &member_keys,
                local_member_index,
                &group_schema,
            );
            if !definition_matches {
                let result: Result<(), GroupInstallError> =
                    ConflictingExistingGroupSnafu { group_id }.fail();
                result.context(inbound::InstallGroupSetupSnafu { group_id })?;
            }
            self.security
                .validate_existing_group_setup_security(
                    group_id,
                    group_setup,
                    sender,
                    &existing_material.security_material,
                )
                .await
                .boxed()
                .context(inbound::GroupSetupSecuritySnafu)?;
            existing_material
        } else {
            let security_material = self
                .security
                .prepare_security_material_from_group_setup(group_id, group_setup, sender)
                .await
                .boxed()
                .context(inbound::GroupSetupSecuritySnafu)?;
            ReplicationGroupMaterialRecord {
                group_id,
                member_keys,
                local_member_index,
                group_schema,
                security_material,
            }
        };
        Ok(ValidatedInboundGroupSetup {
            material,
            already_active,
        })
    }

    /// Remove any inactive state for work rejected by current policy.
    async fn reject_pending_group_delivery(
        &mut self,
        key: PendingGroupWorkKey,
        group_id: GroupId,
    ) -> Result<(), InboundDeliveryError> {
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .remove_pending_group_decision(key)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .remove_pending_group_activation(key)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .remove_inactive_replication_group_material(group_id)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)
    }

    /// Atomically persist verified material and unresolved listener work.
    async fn persist_pending_group_listener_decision(
        &mut self,
        material: ReplicationGroupMaterialRecord,
        record: PendingGroupDecisionRecord,
    ) -> Result<(), InboundDeliveryError> {
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .ensure_replication_group_material(material)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .upsert_pending_group_decision(record)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)
    }

    /// Atomically persist verified material and accepted activation work.
    async fn persist_pending_group_auto_activation(
        &mut self,
        material: ReplicationGroupMaterialRecord,
        activation: PendingGroupActivationRecord,
    ) -> Result<(), InboundDeliveryError> {
        let mut transaction = self
            .store
            .begin_transaction()
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .ensure_replication_group_material(material)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .upsert_pending_group_activation(activation)
            .await
            .context(inbound::StoreAccessSnafu)?;
        transaction
            .commit()
            .await
            .context(inbound::StoreAccessSnafu)
    }

    /// Verify group setup and atomically persist the policy-selected pending state.
    async fn install_pending_group_delivery(
        &mut self,
        record: PendingGroupDecisionRecord,
        group_setup: Arc<GroupSetupMessage>,
        sender: MemberIdentity,
    ) -> Result<(), InboundDeliveryError> {
        let group_id = record.group_id();
        let validated = self
            .validate_inbound_group_setup(&record, group_setup.as_ref(), &sender)
            .await?;
        if validated.already_active {
            return Ok(());
        }

        match self.pending_group_policy_decision(&record).await? {
            PolicyDecision::AutoReject => {
                self.reject_pending_group_delivery(record.key(), group_id)
                    .await
            }
            PolicyDecision::AskListener => {
                self.persist_pending_group_listener_decision(validated.material, record.clone())
                    .await?;
                self.notify_pending_group_decision_listener(record).await
            }
            PolicyDecision::AutoAccept => {
                let activation = record.into_activation();
                self.persist_pending_group_auto_activation(validated.material, activation.clone())
                    .await?;
                let outcome = self
                    .activate_pending_group_record(activation)
                    .await
                    .context(inbound::PendingGroupActivationSnafu)?;
                notify_pending_activation_data_changes(self.listener.clone(), outcome)
                    .await
                    .context(inbound::NotifyListenerSnafu)
            }
        }
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
        let message_group_id = message.group_id();
        let ReliableMessageScope::Group {
            group_id: envelope_group_id,
        } = deliver.envelope.header.scope
        else {
            let error = InboundDeliveryError::ReliableMessageMissingGroupScope { message_group_id };
            return Err(InboundDeliveryFailure::new(context, error));
        };
        if envelope_group_id != message_group_id {
            let error = InboundDeliveryError::ReliableMessageGroupMismatch {
                envelope_group_id,
                message_group_id,
            };
            return Err(InboundDeliveryFailure::new(context, error));
        }
        match message {
            WireRuntimeMessage::GroupInvitation(message) => {
                let (invitation, group_setup) = message.into_parts();
                Ok(self.handle_inbound_pending_group_decision(
                    context,
                    deliver,
                    PendingGroupDecisionRecord::GroupInvitation(invitation),
                    group_setup,
                ))
            }
            WireRuntimeMessage::MigrationProposal(message) => {
                let (proposal, group_setup) = message.into_parts();
                Ok(self.handle_inbound_pending_group_decision(
                    context,
                    deliver,
                    PendingGroupDecisionRecord::MigrationProposal(proposal),
                    group_setup,
                ))
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
            WireRuntimeMessage::GroupInvitation(_) | WireRuntimeMessage::MigrationProposal(_) => {
                Err(InboundDeliveryFailure::new(
                    context,
                    InboundDeliveryError::UnexpectedGroupMessage,
                ))
            }
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
                event_batches: ListenerDataChangeBatches::new(),
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
                event_batches: ListenerDataChangeBatches::new(),
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
        let mut event_batches = ListenerDataChangeBatches::new();
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
                        smallvec![ListenerDataChanges {
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

    fn handle_assess_public_key_bundle(
        &mut self,
        ask: Ask<AssessPublicKeyBundleRequest, Result<PublicKeyBundleReport, ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        let security = self.security.clone();
        self.spawn_local(move |async_self| async move {
            let reply = security
                .assess_public_key_bundle(request)
                .await
                .boxed()
                .context(ApiExternalSnafu);
            async_self.reply_api(promise, "assess_public_key_bundle", reply);
            Handled::OK
        });
        Handled::OK
    }

    fn handle_record_public_key_bundle_feedback(
        &mut self,
        ask: Ask<RecordPublicKeyBundleFeedbackRequest, Result<(), ApiError>>,
    ) -> HandlerResult {
        let (promise, request) = ask.take();
        let security = self.security.clone();
        self.spawn_local(move |async_self| async move {
            let reply = security
                .record_public_key_bundle_feedback(request)
                .await
                .boxed()
                .context(ApiExternalSnafu);
            async_self.reply_api(promise, "record_public_key_bundle_feedback", reply);
            Handled::OK
        });
        Handled::OK
    }

    fn handle_local_public_key_bundle(
        &mut self,
        ask: Ask<(), Result<PublicKeyBundle, ApiError>>,
    ) -> HandlerResult {
        let (promise, ()) = ask.take();
        let reply = Ok(self.security.local_public_key_bundle());
        self.reply_api(promise, "local_public_key_bundle", reply);
        Handled::OK
    }

    fn handle_snapshot_rows(
        &mut self,
        ask: Ask<SnapshotRowsRequest, Result<SnapshotValueRows, ApiError>>,
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
        let (group_id, members, group_schema) = match created_group {
            Ok(created_group) => created_group,
            Err(error) => {
                let reply = Err(error).boxed().context(ApiExternalSnafu);
                self.reply_api(promise, "create_group", reply);
                return Handled::OK;
            }
        };
        Handled::block_on(self, async move |mut async_self| {
            let prepared_setup = Self::prepare_group_setup(
                &async_self.security,
                async_self.max_inline_bootstrap_public_key_bundles,
                group_id,
                &members,
            )
            .await;
            let prepared_setup = match prepared_setup {
                Ok(prepared_setup) => prepared_setup,
                Err(error) => {
                    let reply = Err(error).boxed().context(ApiExternalSnafu);
                    async_self.reply_api(promise, "create_group", reply);
                    return Handled::OK;
                }
            };
            let member_keys = match GroupMemberKeys::from_ordered_member_keys(
                prepared_setup.group_setup.ordered_member_key_ids(),
            ) {
                Ok(member_keys) => member_keys,
                Err(source) => {
                    let reply = Err(CreateGroupError::InvalidMembers { source })
                        .boxed()
                        .context(ApiExternalSnafu);
                    async_self.reply_api(promise, "create_group", reply);
                    return Handled::OK;
                }
            };
            let invitation = GroupInvitation::new_creation(
                group_id,
                prepared_setup.group_setup.members().to_vec(),
                group_schema.clone(),
                InitialSnapshot::Empty,
                None,
                None,
            );
            let invitation_message = GroupInvitationMessage::try_new(
                invitation,
                Arc::clone(&prepared_setup.group_setup),
            )
            .expect("created-group invitation members match prepared group setup");
            let invitation_payload =
                RuntimeMessage::GroupInvitation(invitation_message).encode_proto_to_bytes();
            let record = async_self.build_replication_group_record(
                group_id,
                member_keys,
                group_schema,
                prepared_setup.security_material,
            );
            let persisted_group = async_self.store_new_replication_group(record).await;
            let reply = match persisted_group {
                Ok(persisted_group) => {
                    match async_self.install_group_membership_view(persisted_group) {
                        Ok(()) => {
                            async_self.submit_group_creation_invitation_messages(
                                group_id,
                                &prepared_setup.group_setup,
                                &invitation_payload,
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
        ask: Ask<ChangeGroupMembershipRequest, Result<MigrationId, ApiError>>,
    ) -> HandlerResult {
        let (promise, req) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = match async_self.prepare_membership_migration(req).await {
                Ok(prepared) => {
                    let migration_id = prepared.migration_id;
                    let dispatch = Self::prepare_membership_dispatch(&prepared);
                    match async_self
                        .activate_prepared_membership_migration(prepared)
                        .await
                    {
                        Ok(outcome) => {
                            async_self.submit_membership_migration_messages(&dispatch);
                            match notify_listener_data_changes(
                                async_self.listener.clone(),
                                smallvec![ListenerDataChanges {
                                    read_token: outcome.read_token,
                                    row_changes: outcome.row_changes,
                                }],
                            )
                            .await
                            {
                                Ok(()) => Ok(migration_id),
                                Err(error) => Err(ChangeGroupMembershipError::NotifyListener {
                                    source: error,
                                })
                                .boxed()
                                .context(ApiExternalSnafu),
                            }
                        }
                        Err(error) => Err(error)
                            .context(change_membership::ActivateGroupSnafu)
                            .boxed()
                            .context(ApiExternalSnafu),
                    }
                }
                Err(error) => Err(error).boxed().context(ApiExternalSnafu),
            };
            async_self.reply_api(promise, "change_group_membership", reply);
            Handled::OK
        })
    }

    fn handle_pending_group_decision_response(
        &mut self,
        ask: Ask<PendingGroupDecisionResponse, Result<(), ApiError>>,
    ) -> HandlerResult {
        let (promise, response) = ask.take();
        Handled::block_on(self, async move |mut async_self| {
            let reply = async_self
                .apply_pending_group_decision_response(response)
                .await;
            async_self.reply_api(promise, "pending_group_decision_response", reply);
            Handled::OK
        })
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
        let member_keys = members
            .ordered_members()
            .into_iter()
            .map(|member_id| MemberKeyId {
                fingerprint: test_public_member_keys(&member_id).fingerprint(),
                member_id,
            })
            .collect::<Vec<_>>();
        let member_keys = GroupMemberKeys::from_ordered_member_keys(member_keys)
            .expect("test install group members were already validated");
        let record = self.build_replication_group_record(
            group_id,
            member_keys,
            GroupSchema::default(),
            security_material,
        );
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
            let hydrated_memberships = async_self
                .load_hydrated_runtime_memberships()
                .await
                .whatever_unrecoverable("replication runtime startup failed")?;
            async_self.group_memberships.replace(hydrated_memberships);
            let runtime_ref = async_self
                .ctx
                .actor_ref()
                .hold()
                .whatever_unrecoverable("replication runtime actor ref was unavailable")?;
            async_self
                .replay_pending_group_decisions(runtime_ref)
                .await
                .whatever_unrecoverable("replication runtime startup failed")?;
            async_self
                .resume_pending_group_activations()
                .await
                .whatever_unrecoverable("replication runtime startup failed")?;
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
            ReplicationRuntimeMessage::LocalPublicKeyBundle(ask) => {
                self.handle_local_public_key_bundle(ask)
            }
            ReplicationRuntimeMessage::AssessPublicKeyBundle(ask) => {
                self.handle_assess_public_key_bundle(ask)
            }
            ReplicationRuntimeMessage::RecordPublicKeyBundleFeedback(ask) => {
                self.handle_record_public_key_bundle_feedback(ask)
            }
            ReplicationRuntimeMessage::PublishChanges(ask) => self.handle_publish_changes(ask),
            ReplicationRuntimeMessage::SnapshotRows(ask) => self.handle_snapshot_rows(ask),
            ReplicationRuntimeMessage::RequestSummary(ask) => self.handle_request_summary(ask),
            ReplicationRuntimeMessage::CreateGroup(ask) => self.handle_create_group(ask),
            ReplicationRuntimeMessage::ChangeGroupMembership(ask) => {
                self.handle_change_group_membership(ask)
            }
            ReplicationRuntimeMessage::PendingGroupDecisionResponse(ask) => {
                self.handle_pending_group_decision_response(ask)
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

/// Normalise requested group members so the local creator occupies member index 0.
fn creator_first_member_order(
    mut members: Vec<MemberIdentity>,
    creator: &MemberIdentity,
) -> Result<Vec<MemberIdentity>, CreateGroupError> {
    let creator_position = members
        .iter()
        .position(|member| member == creator)
        .context(CreatorNotInMembersSnafu {
            creator: creator.clone(),
        })?;
    members.swap(0, creator_position);
    Ok(members)
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
    event_batches: ListenerDataChangeBatches,
) -> Result<(), InboundDeliveryError> {
    notify_listener_data_changes(listener, event_batches)
        .await
        .context(inbound::NotifyListenerSnafu)
}

/// Emit non-empty listener data-change batches in their prepared order.
async fn notify_listener_data_changes(
    listener: Arc<dyn ReplicationEventListener>,
    event_batches: ListenerDataChangeBatches,
) -> Result<(), ListenerError> {
    for event_batch in event_batches {
        if event_batch.row_changes.is_empty() {
            continue;
        }
        listener
            .on_event(ReplicationEvent::DataChanged {
                read_token: event_batch.read_token,
                rows: Box::new(VecRowProvider::new(event_batch.row_changes)),
            })
            .await?;
    }
    Ok(())
}

/// Notify listeners when accepted pending activation produced externally visible rows.
async fn notify_pending_activation_data_changes(
    listener: Arc<dyn ReplicationEventListener>,
    outcome: PendingGroupActivationOutcome,
) -> Result<(), ListenerError> {
    notify_listener_data_changes(
        listener,
        smallvec![ListenerDataChanges {
            read_token: outcome.read_token,
            row_changes: outcome.row_changes,
        }],
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ListenerError;
    use std::assert_matches;

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
            InboundDeliveryError::GroupSetupSenderNotInGroup {
                group_id,
                sender: MemberIdentity::from_array(["runtime", "sender"]),
            }
            .failure_action(),
            InboundFailureAction::Drop
        );
        assert_eq!(
            InboundDeliveryError::GroupSetupSenderNotFirstMember {
                group_id,
                sender: MemberIdentity::from_array(["runtime", "sender"]),
                sender_index: Some(MemberIndex::new(1)),
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
    fn group_setup_membership_validation_rejects_sender_outside_group() {
        let group_id = GroupId(Uuid::from_u128(92));
        let local_member = MemberIdentity::from_array(["runtime", "local"]);
        let sender = MemberIdentity::from_array(["runtime", "sender"]);
        let members = GroupMembers::from_ordered_members(vec![local_member.clone()])
            .expect("member set should build");

        let error = ReplicationRuntimeComponent::validate_group_setup_membership(
            group_id,
            &members,
            &local_member,
            &sender,
        )
        .expect_err("sender outside the group should be rejected");

        match error {
            InboundDeliveryError::GroupSetupSenderNotInGroup {
                group_id: rejected_group_id,
                sender: rejected_sender,
            } => {
                assert_eq!(rejected_group_id, group_id);
                assert_eq!(rejected_sender, sender);
            }
            other => panic!("unexpected bootstrap membership error: {other:?}"),
        }
    }

    #[test]
    fn creator_first_member_order_places_creator_at_zero() {
        let creator = MemberIdentity::from_array(["runtime", "creator"]);
        let bob = MemberIdentity::from_array(["runtime", "bob"]);
        let carol = MemberIdentity::from_array(["runtime", "carol"]);

        let ordered =
            creator_first_member_order(vec![bob.clone(), carol.clone(), creator.clone()], &creator)
                .expect("creator in member list should be reordered");

        assert_eq!(ordered, vec![creator, carol, bob]);
    }

    #[test]
    fn creator_first_member_order_rejects_absent_creator() {
        let creator = MemberIdentity::from_array(["runtime", "creator"]);
        let bob = MemberIdentity::from_array(["runtime", "bob"]);

        let error = creator_first_member_order(vec![bob], &creator)
            .expect_err("creator must be part of the requested member list");

        assert_matches!(
            error,
            CreateGroupError::CreatorNotInMembers {
                creator: rejected_creator,
            } if rejected_creator == creator
        );
    }

    #[test]
    fn group_setup_membership_validation_rejects_sender_not_first_member() {
        let group_id = GroupId(Uuid::from_u128(93));
        let local_member = MemberIdentity::from_array(["runtime", "local"]);
        let sender = MemberIdentity::from_array(["runtime", "sender"]);
        let members =
            GroupMembers::from_ordered_members(vec![local_member.clone(), sender.clone()])
                .expect("member set should build");

        let error = ReplicationRuntimeComponent::validate_group_setup_membership(
            group_id,
            &members,
            &local_member,
            &sender,
        )
        .expect_err("sender at non-zero member index should be rejected");

        match error {
            InboundDeliveryError::GroupSetupSenderNotFirstMember {
                group_id: rejected_group_id,
                sender: rejected_sender,
                sender_index,
            } => {
                assert_eq!(rejected_group_id, group_id);
                assert_eq!(rejected_sender, sender);
                assert_eq!(sender_index, Some(MemberIndex::new(1)));
            }
            other => panic!("unexpected bootstrap membership error: {other:?}"),
        }
    }
}

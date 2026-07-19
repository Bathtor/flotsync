#[cfg(any(test, feature = "test-support"))]
use crate::delivery::security::DeliverySecurityError;
use crate::{
    api::{DatasetId, ListenerError, ReplicationGroupLifecycle, RowId, StoreError},
    codecs::messages::RuntimeMessageError,
};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::GroupMembersError,
    versions::UpdateId,
};
use flotsync_data_types::{
    InMemoryValueDataError,
    OperationError,
    schema::{FieldValueBuildError, datamodel::InitialValueRowsEmbeddingError},
};
use flotsync_messages::codecs::datamodel::OperationCodecError;
use kompact::prelude::PromiseErr;
use snafu::{Location, prelude::*};
use uuid::Uuid;

/// Boxed source for errors that would otherwise make high-level runtime errors large.
pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(group_lifecycle))]
pub(crate) enum GroupLifecycleTransitionError {
    #[snafu(display("Old migration group {group_id} is not hosted by this runtime."))]
    UnknownGroup { group_id: GroupId },
    #[snafu(display(
        "Old migration group {group_id} cannot accept a new direction from lifecycle {lifecycle:?}."
    ))]
    NotOpen {
        group_id: GroupId,
        lifecycle: ReplicationGroupLifecycle,
    },
    #[snafu(display(
        "Old migration group {group_id} lifecycle does not match the accepted migration cut."
    ))]
    AcceptedCutMismatch { group_id: GroupId },
    #[snafu(display(
        "Migration cut for group {group_id} has {final_member_count} members, but the old group has {group_member_count}."
    ))]
    MemberCountMismatch {
        group_id: GroupId,
        final_member_count: usize,
        group_member_count: usize,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(accept_migration))]
pub(crate) enum AcceptMigrationError {
    #[snafu(display("Target group material could not be prepared: {source}"))]
    PrepareTarget {
        #[snafu(source(from(GroupActivationError, Box::new)))]
        source: Box<GroupActivationError>,
    },
    #[snafu(display("Replication-store access failed while accepting migration: {source}"))]
    StoreAccess { source: StoreError },
    #[snafu(display("Old-group lifecycle rejected migration acceptance: {source}"))]
    Lifecycle {
        source: GroupLifecycleTransitionError,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub(super) enum CreateGroupError {
    #[snafu(display("Group members must include the local member {local_member}."))]
    LocalMemberMissing { local_member: MemberIdentity },
    #[snafu(display("Group creator {creator} must be included in the requested member list."))]
    CreatorNotInMembers { creator: MemberIdentity },
    #[snafu(display("Group member list is invalid: {source}"))]
    InvalidMembers { source: GroupMembersError },
    #[snafu(display("Failed to prepare secure group bootstrap material: {source}"))]
    Security { source: BoxedError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(snapshot))]
pub(super) enum SnapshotRowsError {
    #[snafu(display("snapshot_rows requires at least one dataset."))]
    EmptyDatasets,
    #[snafu(display("Group {group_id} is not hosted by this runtime."))]
    UnknownGroup { group_id: GroupId },
    #[snafu(display("Group {group_id} is closed to application reads."))]
    GroupClosed { group_id: GroupId },
    #[snafu(display("Dataset {dataset_id} has no schema available for row snapshots."))]
    MissingDatasetSchema { dataset_id: DatasetId },
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(summary))]
pub(super) enum SummaryError {
    #[snafu(display("Group {group_id} is not hosted by this runtime."))]
    UnknownGroup { group_id: GroupId },
    #[snafu(display("Group {group_id} is closed to application summary requests."))]
    GroupClosed { group_id: GroupId },
    #[snafu(display("Summary target {target} is not a member of group {group_id}."))]
    TargetNotInGroup {
        group_id: GroupId,
        target: MemberIdentity,
    },
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum GroupInstallError {
    #[snafu(display("Group {group_id} already exists with a different group definition."))]
    ConflictingExistingGroup { group_id: GroupId },
    #[snafu(display("Group members do not include the local member {local_member}."))]
    InstallMissingLocalMember { local_member: MemberIdentity },
    #[snafu(display(
        "Persisted group {group_id} carried an invalid canonical member set: {source}"
    ))]
    InvalidPersistedMembers {
        group_id: GroupId,
        source: GroupMembersError,
    },
    #[snafu(display(
        "Persisted group {group_id} stored local member {local_member} at index {persisted_local_member_index}, but the canonical member order resolves it to {actual_local_member_index}.",
    ))]
    PersistedLocalMemberIndexMismatch {
        group_id: GroupId,
        local_member: MemberIdentity,
        persisted_local_member_index: MemberIndex,
        actual_local_member_index: MemberIndex,
    },
    #[snafu(display(
        "Persisted group {group_id} stored {persisted_member_count} version-vector members, but the canonical member set has {actual_member_count}.",
    ))]
    PersistedVersionVectorMemberCountMismatch {
        group_id: GroupId,
        persisted_member_count: usize,
        actual_member_count: usize,
    },
    #[snafu(display(
        "Persisted group {group_id} stored a final vector with {final_member_count} members, but the canonical member set has {actual_member_count}."
    ))]
    PersistedFinalVersionVectorMemberCountMismatch {
        group_id: GroupId,
        final_member_count: usize,
        actual_member_count: usize,
    },
    #[snafu(display(
        "Replication-store access failed while installing group {group_id} at {location}: {source}"
    ))]
    StoreGroup {
        group_id: GroupId,
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[cfg(any(test, feature = "test-support"))]
    #[snafu(display("Test group {group_id} security material could not be prepared: {source}"))]
    TestGroupSecurity {
        group_id: GroupId,
        source: DeliverySecurityError,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub(super) enum RuntimeStartupError {
    #[snafu(display(
        "Replication-store access failed while hydrating runtime state at {location}: {source}"
    ))]
    StoreStartup {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Persisted replication runtime state contained duplicate group {group_id}."))]
    DuplicateGroup { group_id: GroupId },
    #[snafu(display(
        "Persisted replication group {group_id} could not be rebuilt into the runtime read model: {source}"
    ))]
    InvalidGroup {
        group_id: GroupId,
        source: GroupInstallError,
    },
    #[snafu(display("Listener rejected one pending group decision replay event: {source}"))]
    ReplayPendingDecision { source: ListenerError },
    #[snafu(display(
        "Pending group activation resume is not implemented; found {activation_count} activation record(s)."
    ))]
    PendingGroupActivationResumeUnsupported { activation_count: usize },
    #[snafu(display("Pending group activation resume failed: {source}"))]
    PendingGroupActivationResume {
        #[snafu(source(from(GroupActivationError, Box::new)))]
        source: Box<GroupActivationError>,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(change_membership))]
pub(crate) enum ChangeGroupMembershipError {
    #[snafu(display("Group {group_id} is not hosted by this runtime."))]
    UnknownGroup { group_id: GroupId },
    #[snafu(display("Group {group_id} no longer accepts membership changes."))]
    GroupNotWritable { group_id: GroupId },
    #[snafu(display("New group members were invalid: {source}"))]
    InvalidMembers { source: GroupMembersError },
    #[snafu(display("New group members must include the local member {local_member}."))]
    LocalMemberMissing { local_member: MemberIdentity },
    #[snafu(display("Persisted group {group_id} was invalid at {location}: {source}"))]
    InvalidPersistedGroup {
        group_id: GroupId,
        #[snafu(source(from(GroupInstallError, Box::new)))]
        source: Box<GroupInstallError>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Migration inline snapshot could not project row {row_id}: {source}"))]
    SnapshotRowValue {
        row_id: RowId,
        #[snafu(source(from(InMemoryValueDataError, Box::new)))]
        source: Box<InMemoryValueDataError>,
    },
    #[snafu(display(
        "Migration inline snapshot scan for dataset '{dataset_id}' in group {group_id} did not exhaust in one storage call."
    ))]
    IncompleteInitialSnapshotScan {
        group_id: GroupId,
        dataset_id: DatasetId,
    },
    #[snafu(display("Failed to prepare secure migration bootstrap material: {source}"))]
    Security { source: BoxedError },
    #[snafu(display("Failed to activate locally prepared membership state: {source}"))]
    ActivateGroup {
        #[snafu(source(from(GroupActivationError, Box::new)))]
        source: Box<GroupActivationError>,
    },
    #[snafu(display("Listener rejected one local migration data-change event: {source}"))]
    NotifyListener { source: ListenerError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(activation))]
pub(crate) enum GroupActivationError {
    #[snafu(display("Accepted group activation {group_id} has no stored group material."))]
    MissingGroupMaterial { group_id: GroupId },
    #[snafu(display(
        "Accepted group activation {group_id} carries metadata-only snapshot state, which this slice cannot fetch."
    ))]
    UnsupportedInitialSnapshot { group_id: GroupId },
    #[snafu(display("Activation group members were invalid: {source}"))]
    InvalidMembers { source: GroupMembersError },
    #[snafu(display("Activation group members must include the local member {local_member}."))]
    LocalMemberMissing { local_member: MemberIdentity },
    #[snafu(display("Persisted group {group_id} was invalid at {location}: {source}"))]
    InvalidPersistedGroup {
        group_id: GroupId,
        #[snafu(source(from(GroupInstallError, Box::new)))]
        source: Box<GroupInstallError>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "Stored group material {group_id} does not match the membership or schema accepted for activation."
    ))]
    ConflictingGroupMaterial { group_id: GroupId },
    #[snafu(display(
        "Accepted activation snapshot for group {group_id} referenced unknown dataset '{dataset_id}'."
    ))]
    MissingInitialDatasetSchema {
        group_id: GroupId,
        dataset_id: DatasetId,
    },
    #[snafu(display(
        "Accepted activation snapshot for dataset '{dataset_id}' in group {group_id} could not be embedded: {source}"
    ))]
    EmbedInitialRows {
        group_id: GroupId,
        dataset_id: DatasetId,
        source: InitialValueRowsEmbeddingError<Uuid>,
    },
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to install activated group {group_id}: {source}"))]
    InstallGroup {
        group_id: GroupId,
        #[snafu(source(from(GroupInstallError, Box::new)))]
        source: Box<GroupInstallError>,
    },
    #[snafu(display("Failed to close the old migration group: {source}"))]
    CloseOldGroup {
        source: GroupLifecycleTransitionError,
    },
    #[snafu(display("Listener rejected one activation data-change event: {source}"))]
    NotifyListener { source: ListenerError },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(publish))]
pub(crate) enum PublishChangesError {
    #[snafu(display("publish_changes requires at least one row mutation."))]
    EmptyChanges,
    #[snafu(display(
        "publish_changes only supports one group per call in the first replication slice; saw both {first_group_id} and {other_group_id}.",
    ))]
    MixedGroups {
        first_group_id: GroupId,
        other_group_id: GroupId,
    },
    #[snafu(display("Group {group_id} is not hosted by this runtime."))]
    UnknownGroup { group_id: GroupId },
    #[snafu(display("Group {group_id} no longer accepts local updates."))]
    GroupNotWritable { group_id: GroupId },
    #[snafu(display("Read token does not contain group {group_id}."))]
    ReadTokenMissingGroup { group_id: GroupId },
    #[snafu(display(
        "Read token for group {group_id} has {read_token_member_count} members, but the persisted group has {persisted_member_count} members.",
    ))]
    ReadTokenMemberCountMismatch {
        group_id: GroupId,
        read_token_member_count: usize,
        persisted_member_count: usize,
    },
    #[snafu(display("Read token for group {group_id} is ahead of the runtime's stored state."))]
    ReadTokenAheadOfLocalState { group_id: GroupId },
    #[snafu(display("Persisted group {group_id} was invalid at {location}: {source}"))]
    InvalidPersistedGroup {
        group_id: GroupId,
        #[snafu(source(from(GroupInstallError, Box::new)))]
        source: Box<GroupInstallError>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Failed to reconstruct row state at read token: {source}"))]
    Replay { source: ReplayError },
    #[snafu(display(
        "Failed to load schema for dataset '{dataset_id}' from the replication store: {source}"
    ))]
    LoadDatasetSchema {
        dataset_id: DatasetId,
        source: StoreError,
    },
    #[snafu(display("No schema was available for dataset '{dataset_id}'."))]
    MissingDatasetSchema { dataset_id: DatasetId },
    #[snafu(display(
        "Row {row_id} referenced unknown schema field '{field_name}' in dataset '{dataset_id}'.",
    ))]
    UnknownSchemaField {
        row_id: RowId,
        dataset_id: DatasetId,
        field_name: String,
    },
    #[snafu(display(
        "Row {row_id} carried a value incompatible with dataset '{dataset_id}': {source}",
    ))]
    InvalidFieldValue {
        row_id: RowId,
        dataset_id: DatasetId,
        source: Box<FieldValueBuildError>,
    },
    #[snafu(display("Applying local mutation for row {row_id} failed: {source}"))]
    ApplyLocalMutation {
        row_id: RowId,
        source: OperationError,
    },
    #[snafu(display("Encoding dataset '{dataset_id}' update for transport failed: {source}"))]
    EncodeOperation {
        dataset_id: DatasetId,
        source: OperationCodecError,
    },
    #[snafu(display(
        "publish_changes produced no effective schema operations for group {group_id}.",
    ))]
    NoEffectiveChanges { group_id: GroupId },
    #[snafu(display("Group {group_id} exhausted its local update id range."))]
    ExhaustedUpdateIds { group_id: GroupId },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(replay))]
pub(crate) enum ReplayError {
    #[snafu(display(
        "Replay for group {group_id} could not resolve a causal order up to the target read token."
    ))]
    Incomplete { group_id: GroupId },
    #[snafu(display("Decoding replay operation for dataset '{dataset_id}' failed: {source}"))]
    DecodeOperation {
        dataset_id: DatasetId,
        source: OperationCodecError,
    },
    #[snafu(display("Applying replay operation for row {row_id} failed: {source}"))]
    ApplyOperation {
        row_id: RowId,
        source: OperationError,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)), module(inbound))]
pub(crate) enum InboundDeliveryError {
    #[snafu(display("Failed to decode inbound runtime message: {source}"))]
    DecodeMessage { source: RuntimeMessageError },
    #[snafu(display("Inbound group setup failed security checks: {source}"))]
    GroupSetupSecurity { source: BoxedError },
    #[snafu(display("Inbound migration acceptance failed: {source}"))]
    AcceptMigration {
        #[snafu(source(from(AcceptMigrationError, Box::new)))]
        source: Box<AcceptMigrationError>,
    },
    #[snafu(display("Reliable delivery unexpectedly carried a group-broadcast update message."))]
    UnexpectedReliableMessage,
    #[snafu(display(
        "Reliable envelope group {envelope_group_id} did not match runtime message scope {message_group_id}."
    ))]
    ReliableMessageGroupMismatch {
        envelope_group_id: GroupId,
        message_group_id: GroupId,
    },
    #[snafu(display(
        "Reliable runtime message for group {message_group_id} arrived without group scope."
    ))]
    ReliableMessageMissingGroupScope { message_group_id: GroupId },
    #[snafu(display("Group broadcast unexpectedly carried a reliable pending-group message."))]
    UnexpectedGroupMessage,
    #[snafu(display("Inbound group setup carried an invalid group member set: {source}"))]
    InvalidGroupSetupMembers { source: GroupMembersError },
    #[snafu(display(
        "Inbound group setup for group {group_id} did not include the local member {local_member}.",
    ))]
    GroupSetupMissingLocalMember {
        group_id: GroupId,
        local_member: MemberIdentity,
    },
    #[snafu(display(
        "Inbound group setup for group {group_id} was signed by {sender}, which is not a group member.",
    ))]
    GroupSetupSenderNotInGroup {
        group_id: GroupId,
        sender: MemberIdentity,
    },
    #[snafu(display(
        "Inbound group setup for group {group_id} was signed by {sender} at member index {sender_index:?}, but new group creators must occupy member index 0.",
    ))]
    GroupSetupSenderNotFirstMember {
        group_id: GroupId,
        sender: MemberIdentity,
        sender_index: Option<MemberIndex>,
    },
    #[snafu(display("Failed to reconcile inbound group setup {group_id} locally: {source}"))]
    InstallGroupSetup {
        group_id: GroupId,
        #[snafu(source(from(GroupInstallError, Box::new)))]
        source: Box<GroupInstallError>,
    },
    #[snafu(display("Failed to complete the processed promise for group {group_id}: {source}"))]
    CompleteProcessedPromise {
        group_id: GroupId,
        source: PromiseErr,
    },
    #[snafu(display("Inbound update targeted unknown hosted group {group_id}."))]
    UnknownHostedGroup { group_id: GroupId },
    #[snafu(display("Inbound pending group work carried an invalid group member set: {source}"))]
    InvalidPendingGroupMembers { source: GroupMembersError },
    #[snafu(display(
        "Inbound pending group work for group {group_id} did not include the local member {local_member}."
    ))]
    PendingGroupMissingLocalMember {
        group_id: GroupId,
        local_member: MemberIdentity,
    },
    #[snafu(display("Persisted group {group_id} was invalid at {location}: {source}"))]
    InvalidPersistedGroup {
        group_id: GroupId,
        #[snafu(source(from(GroupInstallError, Box::new)))]
        source: Box<GroupInstallError>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Replication-store access failed at {location}: {source}"))]
    StoreAccess {
        source: StoreError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "Failed to load schema for inbound dataset '{dataset_id}' from the replication store: {source}"
    ))]
    LoadDatasetSchema {
        dataset_id: DatasetId,
        source: StoreError,
    },
    #[snafu(display("No schema was available for inbound dataset '{dataset_id}'."))]
    MissingDatasetSchema { dataset_id: DatasetId },
    #[snafu(display(
        "Inbound update for group {group_id} came from sender {sender}, which is not a group member.",
    ))]
    UpdateSenderNotInGroup {
        group_id: GroupId,
        sender: MemberIdentity,
    },
    #[snafu(display(
        "Inbound update for group {group_id} claimed sender index {actual_index}, but sender {sender} is canonically at {expected_index}.",
    ))]
    UpdateSenderIndexMismatch {
        group_id: GroupId,
        sender: MemberIdentity,
        expected_index: MemberIndex,
        actual_index: MemberIndex,
    },
    #[snafu(display(
        "Inbound update for group {group_id} claimed producer index {producer_index}, which is outside the group member range.",
    ))]
    UpdateProducerIndexNotInGroup {
        group_id: GroupId,
        producer_index: MemberIndex,
    },
    #[snafu(display(
        "Inbound update for group {group_id} carried invalid read versions: {source}",
    ))]
    DecodeReadVersions {
        group_id: GroupId,
        source: RuntimeMessageError,
    },
    #[snafu(display(
        "Inbound update {update_id} for group {group_id} carried read versions that already include producer version {producer_read_version}.",
    ))]
    SelfDependentReadVersions {
        group_id: GroupId,
        update_id: UpdateId,
        producer_read_version: u64,
    },
    #[snafu(display(
        "Persisted inbound update collision in group {group}: update id {update} already exists with a different payload.",
    ))]
    ConflictingPersistedUpdate { group: GroupId, update: UpdateId },
    #[snafu(display(
        "Inbound update {update} for group {group} carried a schema operation for dataset '{dataset}' with change id {operation_change}.",
    ))]
    UpdateOperationIdMismatch {
        group: GroupId,
        update: UpdateId,
        dataset: DatasetId,
        operation_change: UpdateId,
    },
    #[snafu(display(
        "Failed to decode inbound schema operation for dataset '{dataset_id}': {source}"
    ))]
    DecodeSchemaOperation {
        dataset_id: DatasetId,
        source: OperationCodecError,
    },
    #[snafu(display("Applying inbound mutation for row {row_id} failed: {source}"))]
    ApplyInboundMutation {
        row_id: RowId,
        source: OperationError,
    },
    #[snafu(display("Inbound pending group activation failed: {source}"))]
    PendingGroupActivation {
        #[snafu(source(from(GroupActivationError, Box::new)))]
        source: Box<GroupActivationError>,
    },
    #[snafu(display("Listener rejected one inbound pending group decision event: {source}"))]
    NotifyPendingGroupDecision { source: ListenerError },
    #[snafu(display("Listener rejected one inbound data-change event: {source}"))]
    NotifyListener { source: ListenerError },
}

impl InboundDeliveryError {
    pub(crate) fn failure_action(&self) -> InboundFailureAction {
        match self {
            Self::StoreAccess { .. }
            | Self::LoadDatasetSchema { .. }
            | Self::InvalidPersistedGroup { .. }
            | Self::InstallGroupSetup { .. }
            | Self::AcceptMigration { .. }
            | Self::PendingGroupActivation { .. }
            | Self::CompleteProcessedPromise { .. }
            | Self::NotifyPendingGroupDecision { .. }
            | Self::NotifyListener { .. } => InboundFailureAction::Fatal,
            Self::DecodeMessage { .. }
            | Self::GroupSetupSecurity { .. }
            | Self::UnexpectedReliableMessage
            | Self::ReliableMessageGroupMismatch { .. }
            | Self::ReliableMessageMissingGroupScope { .. }
            | Self::UnexpectedGroupMessage
            | Self::InvalidGroupSetupMembers { .. }
            | Self::GroupSetupMissingLocalMember { .. }
            | Self::GroupSetupSenderNotInGroup { .. }
            | Self::GroupSetupSenderNotFirstMember { .. }
            | Self::UnknownHostedGroup { .. }
            | Self::InvalidPendingGroupMembers { .. }
            | Self::PendingGroupMissingLocalMember { .. }
            | Self::MissingDatasetSchema { .. }
            | Self::UpdateSenderNotInGroup { .. }
            | Self::UpdateSenderIndexMismatch { .. }
            | Self::UpdateProducerIndexNotInGroup { .. }
            | Self::DecodeReadVersions { .. }
            | Self::SelfDependentReadVersions { .. }
            | Self::ConflictingPersistedUpdate { .. }
            | Self::UpdateOperationIdMismatch { .. }
            | Self::DecodeSchemaOperation { .. }
            | Self::ApplyInboundMutation { .. } => InboundFailureAction::Drop,
        }
    }
}

/// Runtime action selected after classifying an inbound delivery failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InboundFailureAction {
    /// Ignore the offending delivery without applying it.
    Drop,
    /// Treat the failure as a component fault and let Kompact supervision handle it.
    Fatal,
}

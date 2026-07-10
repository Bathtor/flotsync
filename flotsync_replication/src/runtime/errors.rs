use super::messages::RuntimeMessageError;
use crate::api::{DatasetId, ListenerError, RowId, StoreError};
#[cfg(any(test, feature = "test-support"))]
use crate::delivery::security::DeliverySecurityError;
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::GroupMembersError,
    versions::UpdateId,
};
use flotsync_data_types::{OperationError, schema::FieldValueBuildError};
use flotsync_messages::codecs::datamodel::OperationCodecError;
use kompact::prelude::PromiseErr;
use snafu::{Location, prelude::*};

/// Boxed source for errors that would otherwise make high-level runtime errors large.
type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub(super) enum CreateGroupError {
    #[snafu(display("Initial group state is not supported in the first replication slice."))]
    InitialStateUnsupported,
    #[snafu(display("Group members must include the local member {local_member}."))]
    LocalMemberMissing { local_member: MemberIdentity },
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
    #[snafu(display("Group {group_id} already exists with a different canonical member order."))]
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
    #[snafu(display("Inbound bootstrap failed security checks: {source}"))]
    BootstrapSecurity { source: BoxedError },
    #[snafu(display(
        "Inbound bootstrap wire group id {wire_group_id} did not match payload group id {payload_group_id}."
    ))]
    BootstrapGroupIdMismatch {
        wire_group_id: GroupId,
        payload_group_id: uuid::Uuid,
    },
    #[snafu(display("Reliable delivery unexpectedly carried a group-broadcast update message."))]
    UnexpectedReliableMessage,
    #[snafu(display("Group broadcast unexpectedly carried a reliable bootstrap message."))]
    UnexpectedGroupMessage,
    #[snafu(display("Inbound bootstrap message carried an invalid group member set: {source}"))]
    InvalidBootstrapMembers { source: GroupMembersError },
    #[snafu(display(
        "Inbound bootstrap for group {group_id} did not include the local member {local_member}.",
    ))]
    BootstrapMissingLocalMember {
        group_id: GroupId,
        local_member: MemberIdentity,
    },
    #[snafu(display(
        "Inbound bootstrap for group {group_id} was signed by {sender}, which is not a group member.",
    ))]
    BootstrapSenderNotInGroup {
        group_id: GroupId,
        sender: MemberIdentity,
    },
    #[snafu(display("Failed to install inbound bootstrap group {group_id} locally: {source}"))]
    InstallBootstrapGroup {
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
    #[snafu(display("Listener rejected one inbound data-change event: {source}"))]
    NotifyListener { source: ListenerError },
}

impl InboundDeliveryError {
    pub(crate) fn failure_action(&self) -> InboundFailureAction {
        match self {
            Self::StoreAccess { .. }
            | Self::LoadDatasetSchema { .. }
            | Self::InvalidPersistedGroup { .. }
            | Self::InstallBootstrapGroup { .. }
            | Self::CompleteProcessedPromise { .. }
            | Self::NotifyListener { .. } => InboundFailureAction::Fatal,
            Self::DecodeMessage { .. }
            | Self::BootstrapSecurity { .. }
            | Self::BootstrapGroupIdMismatch { .. }
            | Self::UnexpectedReliableMessage
            | Self::UnexpectedGroupMessage
            | Self::InvalidBootstrapMembers { .. }
            | Self::BootstrapMissingLocalMember { .. }
            | Self::BootstrapSenderNotInGroup { .. }
            | Self::UnknownHostedGroup { .. }
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

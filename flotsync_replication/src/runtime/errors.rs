use super::messages::RuntimeMessageError;
use crate::{
    GroupMembersError,
    api::{DatasetId, GroupId, ListenerError, MemberIdentity, MemberIndex, RowId, StoreError},
};
use flotsync_core::versions::UpdateId;
use flotsync_data_types::{OperationError, schema::FieldValueBuildError};
use flotsync_messages::codecs::datamodel::OperationCodecError;
use kompact::prelude::PromiseErr;
use snafu::{Location, prelude::*};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub(super) enum CreateGroupError {
    #[snafu(display("Initial group state is not supported in the first replication slice."))]
    InitialStateUnsupported,
    #[snafu(display("Group members must include the local member {local_member}."))]
    LocalMemberMissing { local_member: MemberIdentity },
    #[snafu(display("Group member list is invalid: {source}"))]
    InvalidMembers { source: GroupMembersError },
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
    #[snafu(display("Persisted group {group_id} was invalid at {location}: {source}"))]
    InvalidPersistedGroup {
        group_id: GroupId,
        source: GroupInstallError,
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
#[snafu(visibility(pub(crate)), module(inbound))]
pub(crate) enum InboundDeliveryError {
    #[snafu(display("Failed to decode inbound runtime message: {source}"))]
    DecodeMessage { source: RuntimeMessageError },
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
    #[snafu(display("Failed to install inbound bootstrap group {group_id} locally: {source}"))]
    InstallBootstrapGroup {
        group_id: GroupId,
        source: GroupInstallError,
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
        source: GroupInstallError,
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
        "Persisted inbound update collision in group {group_id}: update id {update_id} already exists with a different payload.",
    ))]
    ConflictingPersistedUpdate {
        group_id: GroupId,
        update_id: UpdateId,
    },
    #[snafu(display(
        "Inbound update {update_id} for group {group_id} carried a schema operation for dataset '{dataset_id}' with change id {operation_change_id}.",
    ))]
    UpdateOperationIdMismatch {
        group_id: GroupId,
        update_id: UpdateId,
        dataset_id: DatasetId,
        operation_change_id: UpdateId,
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
            | Self::UnexpectedReliableMessage
            | Self::UnexpectedGroupMessage
            | Self::InvalidBootstrapMembers { .. }
            | Self::BootstrapMissingLocalMember { .. }
            | Self::UnknownHostedGroup { .. }
            | Self::MissingDatasetSchema { .. }
            | Self::UpdateSenderNotInGroup { .. }
            | Self::UpdateSenderIndexMismatch { .. }
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

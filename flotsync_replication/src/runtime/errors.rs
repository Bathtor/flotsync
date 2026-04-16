use super::*;
use flotsync_data_types::{OperationError, schema::FieldValueBuildError};
use flotsync_messages::codecs::datamodel::OperationCodecError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub(super) enum CreateGroupError {
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
#[snafu(visibility(pub(super)))]
pub(super) enum GroupInstallError {
    #[snafu(display("Group {group_id} already exists with a different canonical member order."))]
    ConflictingExistingGroup { group_id: GroupId },
    #[snafu(display("Group members do not include the local member {local_member}."))]
    InstallMissingLocalMember { local_member: MemberIdentity },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(super)))]
pub(super) enum PublishChangesError {
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
        "Row {row_id} referenced unknown schema field '{field_name}' in dataset '{dataset_id}'.",
    ))]
    UnknownSchemaField {
        row_id: crate::api::RowId,
        dataset_id: DatasetId,
        field_name: String,
    },
    #[snafu(display("Row {row_id} carried a value incompatible with dataset '{dataset_id}'.",))]
    InvalidFieldValue {
        row_id: crate::api::RowId,
        dataset_id: DatasetId,
        source: Box<FieldValueBuildError>,
    },
    #[snafu(display("Applying local mutation for row {row_id} failed."))]
    ApplyLocalMutation {
        row_id: crate::api::RowId,
        source: OperationError,
    },
    #[snafu(display("Encoding dataset '{dataset_id}' update for transport failed."))]
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
#[snafu(visibility(pub(super)))]
pub(super) enum InboundDeliveryError {
    #[snafu(display("Failed to decode inbound runtime message."))]
    DecodeMessage { source: RuntimeMessageError },
    #[snafu(display("Reliable delivery unexpectedly carried a group-broadcast update message."))]
    UnexpectedReliableMessage,
    #[snafu(display("Group broadcast unexpectedly carried a reliable bootstrap message."))]
    UnexpectedGroupMessage,
    #[snafu(display("Inbound bootstrap message carried an invalid group member set."))]
    InvalidBootstrapMembers { source: crate::GroupMembersError },
    #[snafu(display(
        "Inbound bootstrap for group {group_id} did not include the local member {local_member}.",
    ))]
    BootstrapMissingLocalMember {
        group_id: GroupId,
        local_member: MemberIdentity,
    },
    #[snafu(display("Failed to install inbound bootstrap group {group_id} locally."))]
    InstallBootstrapGroup {
        group_id: GroupId,
        source: GroupInstallError,
    },
    #[snafu(display("Failed to complete the processed promise for group {group_id}."))]
    CompleteProcessedPromise {
        group_id: GroupId,
        source: PromiseErr,
    },
    #[snafu(display("Inbound update targeted unknown hosted group {group_id}."))]
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
    #[snafu(display("Inbound update for group {group_id} carried invalid read versions.",))]
    DecodeReadVersions {
        group_id: GroupId,
        source: RuntimeMessageError,
    },
    #[snafu(display(
        "Buffered inbound update collision in group {group_id}: update id {update_id} arrived with a different payload than the one already buffered.",
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
    #[snafu(display("Applying inbound mutation for row {row_id} failed."))]
    ApplyInboundMutation {
        row_id: crate::api::RowId,
        source: OperationError,
    },
    #[snafu(display("Listener rejected one inbound data-change event."))]
    NotifyListener { source: ListenerError },
}

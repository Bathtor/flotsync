use super::*;

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
#[snafu(visibility(pub(super)))]
pub(super) enum PublishChangesError {
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

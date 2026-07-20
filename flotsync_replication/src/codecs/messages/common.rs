//! Shared message errors and decode contexts.

#[allow(
    clippy::wildcard_imports,
    reason = "The private message-codec helper shares its parent's local implementation vocabulary."
)]
use super::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum RuntimeMessageError {
    #[snafu(display("Failed to decode runtime message payload."))]
    Decode {
        source: flotsync_messages::buffa::DecodeError,
    },
    #[snafu(display("Runtime message did not contain a body."))]
    MissingBody,
    #[snafu(display("Group setup must include at least one member."))]
    EmptyGroupSetup,
    #[snafu(display("Pending-group runtime message did not include private group setup."))]
    MissingGroupSetup,
    #[snafu(display(
        "Pending-group members did not match the ordered identities in private group setup."
    ))]
    GroupSetupMemberMismatch,
    #[snafu(display("Group setup must include key references for every member."))]
    EmptyBootstrapMemberKeys,
    #[snafu(display("Bootstrap member-key entry did not include a member id."))]
    MissingBootstrapMemberId,
    #[snafu(display("Group setup is missing a key reference for member {member_id}."))]
    MissingBootstrapMemberKey { member_id: MemberIdentity },
    #[snafu(display(
        "Group setup carried {member_key_count} member-key entries for {member_count} members."
    ))]
    BootstrapMemberKeyCountMismatch {
        member_count: usize,
        member_key_count: usize,
    },
    #[snafu(display("Bootstrap public key bundle for member {member_id} was invalid: {source}"))]
    InvalidBootstrapPublicKeyBundle {
        member_id: MemberIdentity,
        source: SecurityError,
    },
    #[snafu(display(
        "Bootstrap public key bundle for member {member_id} derives fingerprint {actual}, expected {expected}."
    ))]
    BootstrapPublicKeyBundleFingerprintMismatch {
        member_id: MemberIdentity,
        expected: KeyFingerprint,
        actual: KeyFingerprint,
    },
    #[snafu(display(
        "Bootstrap inline public key bundle for member {member_id} was bound to member {public_key_member_id}."
    ))]
    BootstrapMemberKeyBindingMismatch {
        member_id: MemberIdentity,
        public_key_member_id: MemberIdentity,
    },
    #[snafu(display("Group setup cipher suite {actual} is unsupported; expected {expected}."))]
    UnsupportedGroupSetupCipherSuite { actual: u32, expected: u16 },
    #[snafu(display(
        "Runtime message field '{field}' had invalid byte length {actual}; expected {expected}."
    ))]
    InvalidByteLength {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[snafu(display("Update message must include at least one dataset update."))]
    EmptyUpdate,
    #[snafu(display("NeedRange message must include at least one range."))]
    EmptyNeedRange,
    #[snafu(display("UpdateBatch message must include at least one update."))]
    EmptyUpdateBatch,
    #[snafu(display(
        "NeedRange entry for producer {producer_index} had invalid range {start_version}..={end_version}."
    ))]
    InvalidNeedRange {
        producer_index: u32,
        start_version: u64,
        end_version: u64,
    },
    #[snafu(display(
        "NeedRange entry for producer {producer_index} used unsupported version bound {version}; maximum supported bound is {MAX_VERSION_VALUE}."
    ))]
    NeedRangeBoundTooLarge { producer_index: u32, version: u64 },
    #[snafu(display(
        "Update id {update_id} used unsupported version bound {version}; maximum supported bound is {MAX_VERSION_VALUE}."
    ))]
    UpdateVersionBoundTooLarge { update_id: UpdateId, version: u64 },
    #[snafu(display(
        "UpdateBatch for group {batch_group} contained update {update} for different group {update_group}."
    ))]
    UpdateBatchGroupMismatch {
        batch_group: GroupId,
        update_group: GroupId,
        update: UpdateId,
    },
    #[snafu(display(
        "Update dataset entry for '{dataset_id}' must include at least one operation."
    ))]
    EmptyDatasetUpdate { dataset_id: String },
    #[snafu(display("Update message did not include an update id."))]
    MissingUpdateId,
    #[snafu(display("Update message did not include read versions."))]
    MissingReadVersions,
    #[snafu(display("Summary message did not include versions."))]
    MissingSummaryVersions,
    #[snafu(display("Runtime message field '{field}' was invalid: {source}"))]
    InvalidWireValue {
        field: &'static str,
        source: WireValueDecodeError,
    },
    #[snafu(display("Runtime message pending-group payload was invalid: {source}"))]
    InvalidPendingGroupPayload { source: PendingGroupPayloadError },
    #[snafu(display("Runtime message field '{field}' was not a valid UUID: {source}"))]
    InvalidCorrelationId {
        field: &'static str,
        source: uuid::Error,
    },
    #[snafu(display("Update field '{field}' was invalid: {source}"))]
    InvalidUpdateId {
        field: &'static str,
        source: DatamodelCodecError,
    },
    #[snafu(display("Version-vector field '{field}' was invalid: {source}"))]
    InvalidReadVersions {
        field: &'static str,
        source: WireVersionVectorError,
    },
    #[snafu(display("Update dataset id '{value}' was invalid: {source}"))]
    InvalidDatasetId {
        value: String,
        source: DatasetIdError,
    },
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum WireVersionVectorError {
    #[snafu(display("Version-vector payload was missing."))]
    MissingVersionsBody,
    #[snafu(display("Full version vector must include at least one entry."))]
    EmptyFullVector,
    #[snafu(display(
        "Compact read versions expected {expected_members} members, but the hosted group has {actual_members}."
    ))]
    MemberCountMismatch {
        expected_members: usize,
        actual_members: usize,
    },
    #[snafu(display(
        "Override read versions used invalid override position {override_position} for {num_members} members."
    ))]
    InvalidOverridePosition {
        num_members: usize,
        override_position: u32,
    },
    #[snafu(display(
        "Override read versions were invalid: group version {group_version}, override position {override_position}, override version {override_version}."
    ))]
    InvalidOverride {
        group_version: u64,
        override_position: u32,
        override_version: u64,
    },
    #[snafu(display(
        "Version-vector field '{field}' used unsupported version bound {version}; maximum supported bound is {MAX_VERSION_VALUE}."
    ))]
    VersionBoundTooLarge { field: &'static str, version: u64 },
}

impl proto::FromProtoDecodeError for RuntimeMessageError {
    fn from_proto_decode_error(source: flotsync_messages::buffa::DecodeError) -> Self {
        Self::Decode { source }
    }
}

/// Required-oneof contexts in runtime delivery messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuntimeMessageOneofContext {
    /// `RuntimeMessage.body`.
    Body,
}

impl MissingRequiredProto for RuntimeMessageError {
    type Context = RuntimeMessageOneofContext;

    fn missing_required(context: Self::Context) -> Self {
        match context {
            RuntimeMessageOneofContext::Body => Self::MissingBody,
        }
    }
}

/// Hosted-group member count needed by compact runtime protobuf decoders.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MemberCountContext {
    /// Number of members in the hosted replication group.
    member_count: NonZeroUsize,
}

impl MemberCountContext {
    /// Create a new member-count decode context.
    pub(crate) const fn new(member_count: NonZeroUsize) -> Self {
        Self { member_count }
    }

    /// Return the hosted-group member count.
    pub(crate) const fn member_count(self) -> NonZeroUsize {
        self.member_count
    }
}

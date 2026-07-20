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
    /// A compact vector referenced a group absent from the membership snapshot.
    #[snafu(display("Runtime message for group {group_id} requires hosted group-member context."))]
    MissingGroupMemberContext { group_id: GroupId },
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
        source: VersionVectorCodecError,
    },
    #[snafu(display("Update dataset id '{value}' was invalid: {source}"))]
    InvalidDatasetId {
        value: String,
        source: DatasetIdError,
    },
}

impl proto::FromProtoDecodeError for RuntimeMessageError {
    fn from_proto_decode_error(source: flotsync_messages::buffa::DecodeError) -> Self {
        Self::Decode { source }
    }
}

/// Failure while validating a compact or self-describing version-vector protobuf.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum VersionVectorCodecError {
    /// The protobuf omitted its compact representation body.
    #[snafu(display("Version-vector payload was missing."))]
    MissingVersionsBody,
    /// A self-describing vector declared zero members.
    #[snafu(display("Version-vector member count must be greater than zero."))]
    InvalidMemberCount,
    /// An explicit full-vector representation contained no member entries.
    #[snafu(display("Full version vector must include at least one entry."))]
    EmptyFullVector,
    /// An explicit full-vector length disagreed with its member-count context.
    #[snafu(display(
        "Version vector contained {actual_members} members, but its context requires {expected_members}."
    ))]
    MemberCountMismatch {
        expected_members: usize,
        actual_members: usize,
    },
    /// An override position fell outside the owning group's membership order.
    #[snafu(display(
        "Version vector used invalid override position {override_position} for {num_members} members."
    ))]
    InvalidOverridePosition {
        num_members: usize,
        override_position: u32,
    },
    /// Override versions violated the runtime vector invariants.
    #[snafu(display(
        "Version-vector override was invalid: group version {group_version}, override position {override_position}, override version {override_version}."
    ))]
    InvalidOverride {
        group_version: u64,
        override_position: u32,
        override_version: u64,
    },
    /// A version used the reserved upper bound unsupported by runtime arithmetic.
    #[snafu(display(
        "Version-vector field '{field}' used unsupported version bound {version}; maximum supported bound is {MAX_VERSION_VALUE}."
    ))]
    VersionBoundTooLarge { field: &'static str, version: u64 },
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

/// Hosted-group context available while decoding one runtime message.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RuntimeMessageDecodeContext<'a> {
    /// Current immutable membership snapshot for locally hosted groups.
    memberships: &'a GroupMemberships,
}

impl<'a> RuntimeMessageDecodeContext<'a> {
    /// Create a runtime-message context from the current membership snapshot.
    pub(crate) const fn new(memberships: &'a GroupMemberships) -> Self {
        Self { memberships }
    }

    /// Return the member count for a compact vector scoped to `group_id`.
    pub(crate) fn member_count_for(
        self,
        group_id: GroupId,
    ) -> Result<MemberCountContext, RuntimeMessageError> {
        let members = self
            .memberships
            .members(&group_id)
            .context(MissingGroupMemberContextSnafu { group_id })?;
        let member_count =
            NonZeroUsize::new(members.len()).expect("hosted replication groups must not be empty");
        Ok(MemberCountContext::new(member_count))
    }
}

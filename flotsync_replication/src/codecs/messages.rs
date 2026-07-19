use crate::{
    MAX_VERSION_VALUE,
    api::{
        DatasetId,
        DatasetIdError,
        DatasetUpdateRecord,
        GroupInvitation,
        MemberKeyId,
        MigrationProposal,
        ReplicationUpdateRecord,
        Summary,
    },
    codecs::pending_group::{PendingGroupPayloadDecodeContext, PendingGroupPayloadError},
    delivery::wire::{
        WireValueDecodeError,
        group_id_from_wire,
        member_identity_from_wire,
        member_identity_to_wire_format,
    },
};
use borrowize::View;
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::TrieMap,
    versions::{OverrideVersion, PureVersionVector, UpdateId, VersionVector, VersionVectorGap},
};
use flotsync_messages::{
    buffa::MessageField,
    codecs::datamodel::{CodecError as DatamodelCodecError, decode_update_id, encode_update_id},
    datamodel as datamodel_proto,
    proto::{
        self,
        DecodeProto,
        DecodeProtoOneof,
        DecodeProtoView,
        DecodeProtoViewWith,
        DecodeProtoWith,
        EncodeProto,
        EncodeProtoOneof,
        MissingRequiredProto,
    },
    replication as replication_proto,
    security as security_proto,
    versions as versions_proto,
    wire as message_wire,
};
use flotsync_security::{
    GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
    GROUP_KEY_LENGTH,
    GroupCipherSuite,
    GroupKey,
    KEY_FINGERPRINT_LENGTH,
    KeyFingerprint,
    PublicMemberKeys,
    SecurityError,
};
use flotsync_utils::option_when;
use snafu::prelude::*;
use std::{fmt, num::NonZeroUsize, sync::Arc};
use uuid::Uuid;

#[derive(Debug, Snafu)]
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

/// Replication runtime payload carried by group broadcast or reliable delivery.
///
/// All current variants are scoped to one replication group. Non-group runtime
/// messages should add an explicit scope API rather than reusing [`Self::group_id`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RuntimeMessage {
    Update(Box<UpdateMessage>),
    SummaryRequest(SummaryRequestMessage),
    Summary(SummaryMessage),
    NeedRange(NeedRangeMessage),
    UpdateBatch(UpdateBatchMessage),
    GroupInvitation(GroupInvitationMessage),
    MigrationProposal(MigrationProposalMessage),
}

impl RuntimeMessage {
    /// Return the replication group that scopes every current runtime message variant.
    pub(crate) fn group_id(&self) -> GroupId {
        match self {
            Self::Update(message) => message.group_id,
            Self::SummaryRequest(message) => message.group_id,
            Self::Summary(message) => message.group_id,
            Self::NeedRange(message) => message.group_id,
            Self::UpdateBatch(message) => message.group_id,
            Self::GroupInvitation(message) => message.invitation.group_id,
            Self::MigrationProposal(message) => message.proposal.migration_id.old_group_id,
        }
    }
}

impl EncodeProto for RuntimeMessage {
    type Proto = replication_proto::RuntimeMessage;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::RuntimeMessage {
            body: Some(EncodeProtoOneof::encode_proto(self)),
            ..replication_proto::RuntimeMessage::default()
        }
    }
}

impl EncodeProtoOneof for RuntimeMessage {
    type Proto = replication_proto::runtime_message::Body;

    fn encode_proto(&self) -> Self::Proto {
        match self {
            RuntimeMessage::Update(message) => Self::Proto::Update(message.encode_proto_boxed()),
            RuntimeMessage::SummaryRequest(message) => {
                Self::Proto::SummaryRequest(message.encode_proto_boxed())
            }
            RuntimeMessage::Summary(message) => Self::Proto::Summary(message.encode_proto_boxed()),
            RuntimeMessage::NeedRange(message) => {
                Self::Proto::NeedRange(message.encode_proto_boxed())
            }
            RuntimeMessage::UpdateBatch(message) => {
                Self::Proto::UpdateBatch(message.encode_proto_boxed())
            }
            RuntimeMessage::GroupInvitation(message) => {
                Self::Proto::GroupInvitation(message.encode_proto_boxed())
            }
            RuntimeMessage::MigrationProposal(message) => {
                Self::Proto::MigrationProposal(message.encode_proto_boxed())
            }
        }
    }
}

/// One decoded wire message before all runtime context is available.
///
/// Bootstrap messages can decode directly into their runtime form, but inbound
/// updates and summaries may still carry compact version encodings that need
/// the hosted group member count before they can become a full `VersionVector`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum WireRuntimeMessage {
    Update(WireUpdateMessage),
    SummaryRequest(SummaryRequestMessage),
    Summary(WireSummaryMessage),
    NeedRange(NeedRangeMessage),
    UpdateBatch(WireUpdateBatchMessage),
    GroupInvitation(GroupInvitationMessage),
    MigrationProposal(MigrationProposalMessage),
}

impl WireRuntimeMessage {
    /// Return the replication group that scopes this message on delivery.
    pub(crate) fn group_id(&self) -> GroupId {
        match self {
            Self::Update(message) => message.group_id,
            Self::SummaryRequest(message) => message.group_id,
            Self::Summary(message) => message.group_id,
            Self::NeedRange(message) => message.group_id,
            Self::UpdateBatch(message) => message.group_id,
            Self::GroupInvitation(message) => message.invitation.group_id,
            Self::MigrationProposal(message) => message.proposal.migration_id.old_group_id,
        }
    }
}

impl DecodeProto for WireRuntimeMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::RuntimeMessage;

    fn decode_proto(message: Self::Proto) -> Result<Self, Self::Error> {
        <Self as DecodeProtoOneof>::decode_required_proto(
            message.body,
            RuntimeMessageOneofContext::Body,
        )
    }
}

impl DecodeProtoOneof for WireRuntimeMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::runtime_message::Body;

    fn decode_proto(body: Self::Proto) -> Result<Self, Self::Error> {
        match body {
            replication_proto::runtime_message::Body::Update(message) => {
                let message = WireUpdateMessage::decode_proto(*message)?;
                Ok(WireRuntimeMessage::Update(message))
            }
            replication_proto::runtime_message::Body::SummaryRequest(message) => {
                let message = SummaryRequestMessage::decode_proto(*message)?;
                Ok(WireRuntimeMessage::SummaryRequest(message))
            }
            replication_proto::runtime_message::Body::Summary(message) => {
                let message = WireSummaryMessage::decode_proto(*message)?;
                Ok(WireRuntimeMessage::Summary(message))
            }
            replication_proto::runtime_message::Body::NeedRange(message) => {
                let message = NeedRangeMessage::decode_proto(*message)?;
                Ok(WireRuntimeMessage::NeedRange(message))
            }
            replication_proto::runtime_message::Body::UpdateBatch(message) => {
                let message = WireUpdateBatchMessage::decode_proto(*message)?;
                Ok(WireRuntimeMessage::UpdateBatch(message))
            }
            replication_proto::runtime_message::Body::GroupInvitation(message) => {
                let message = GroupInvitationMessage::decode_proto(*message)?;
                Ok(WireRuntimeMessage::GroupInvitation(message))
            }
            replication_proto::runtime_message::Body::MigrationProposal(message) => {
                let message = MigrationProposalMessage::decode_proto(*message)?;
                Ok(WireRuntimeMessage::MigrationProposal(message))
            }
        }
    }
}

impl DecodeProtoView for WireRuntimeMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::RuntimeMessageView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let Some(body) = message.body.as_ref() else {
            return MissingBodySnafu.fail();
        };
        match body {
            replication_proto::runtime_message::BodyView::Update(message) => {
                let message = WireUpdateMessage::decode_proto_view(message)?;
                Ok(WireRuntimeMessage::Update(message))
            }
            replication_proto::runtime_message::BodyView::SummaryRequest(message) => {
                let message = SummaryRequestMessage::decode_proto_view(message)?;
                Ok(WireRuntimeMessage::SummaryRequest(message))
            }
            replication_proto::runtime_message::BodyView::Summary(message) => {
                let message = WireSummaryMessage::decode_proto_view(message)?;
                Ok(WireRuntimeMessage::Summary(message))
            }
            replication_proto::runtime_message::BodyView::NeedRange(message) => {
                let message = NeedRangeMessage::decode_proto_view(message)?;
                Ok(WireRuntimeMessage::NeedRange(message))
            }
            replication_proto::runtime_message::BodyView::UpdateBatch(message) => {
                let message = WireUpdateBatchMessage::decode_proto_view(message)?;
                Ok(WireRuntimeMessage::UpdateBatch(message))
            }
            replication_proto::runtime_message::BodyView::GroupInvitation(message) => {
                // TODO(flotsync-git-i20): add borrowed contextual decoders for
                // pending-group payloads so view decoding does not first own
                // the generated payload.
                let message =
                    flotsync_messages::buffa::MessageView::to_owned_message(message.as_ref())
                        .context(DecodeSnafu)?;
                let message = GroupInvitationMessage::decode_proto(message)?;
                Ok(WireRuntimeMessage::GroupInvitation(message))
            }
            replication_proto::runtime_message::BodyView::MigrationProposal(message) => {
                // TODO(flotsync-git-i20): add borrowed contextual decoders for
                // pending-group payloads so view decoding does not first own
                // the generated payload.
                let message =
                    flotsync_messages::buffa::MessageView::to_owned_message(message.as_ref())
                        .context(DecodeSnafu)?;
                let message = MigrationProposalMessage::decode_proto(message)?;
                Ok(WireRuntimeMessage::MigrationProposal(message))
            }
        }
    }
}

/// Reliable group invitation plus recipient-protected target-group setup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupInvitationMessage {
    invitation: GroupInvitation,
    group_setup: Arc<GroupSetupMessage>,
}

impl GroupInvitationMessage {
    /// Combine listener-safe invitation data with matching private group setup.
    pub(crate) fn try_new(
        invitation: GroupInvitation,
        group_setup: Arc<GroupSetupMessage>,
    ) -> Result<Self, RuntimeMessageError> {
        ensure!(
            invitation.proposed_members == group_setup.members(),
            GroupSetupMemberMismatchSnafu
        );
        Ok(Self {
            invitation,
            group_setup,
        })
    }

    /// Split the wire message into listener-safe and private setup parts.
    pub(crate) fn into_parts(self) -> (GroupInvitation, Arc<GroupSetupMessage>) {
        (self.invitation, self.group_setup)
    }
}

impl EncodeProto for GroupInvitationMessage {
    type Proto = replication_proto::GroupInvitationPayload;

    fn encode_proto(&self) -> Self::Proto {
        let mut message = self.invitation.encode_proto();
        message.group_setup = MessageField::some(self.group_setup.encode_proto());
        message
    }
}

impl DecodeProto for GroupInvitationMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::GroupInvitationPayload;

    fn decode_proto(mut message: Self::Proto) -> Result<Self, Self::Error> {
        let Some(group_setup) = message.group_setup.take() else {
            return MissingGroupSetupSnafu.fail();
        };
        let group_setup = GroupSetupMessage::decode_proto(group_setup)?;
        let invitation = GroupInvitation::decode_proto_with(
            message,
            PendingGroupPayloadDecodeContext::default(),
        )
        .context(InvalidPendingGroupPayloadSnafu)?;
        Self::try_new(invitation, Arc::new(group_setup))
    }
}

/// Reliable migration proposal plus recipient-protected target-group setup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MigrationProposalMessage {
    proposal: MigrationProposal,
    group_setup: Arc<GroupSetupMessage>,
}

impl MigrationProposalMessage {
    /// Combine listener-safe proposal data with matching private group setup.
    pub(crate) fn try_new(
        proposal: MigrationProposal,
        group_setup: Arc<GroupSetupMessage>,
    ) -> Result<Self, RuntimeMessageError> {
        ensure!(
            proposal.proposed_members == group_setup.members(),
            GroupSetupMemberMismatchSnafu
        );
        Ok(Self {
            proposal,
            group_setup,
        })
    }

    /// Split the wire message into listener-safe and private setup parts.
    pub(crate) fn into_parts(self) -> (MigrationProposal, Arc<GroupSetupMessage>) {
        (self.proposal, self.group_setup)
    }
}

impl EncodeProto for MigrationProposalMessage {
    type Proto = replication_proto::MigrationProposalPayload;

    fn encode_proto(&self) -> Self::Proto {
        let mut message = self.proposal.encode_proto();
        message.group_setup = MessageField::some(self.group_setup.encode_proto());
        message
    }
}

impl DecodeProto for MigrationProposalMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::MigrationProposalPayload;

    fn decode_proto(mut message: Self::Proto) -> Result<Self, Self::Error> {
        let Some(group_setup) = message.group_setup.take() else {
            return MissingGroupSetupSnafu.fail();
        };
        let group_setup = GroupSetupMessage::decode_proto(group_setup)?;
        // TODO(flotsync-git-i20): this empty context cannot expand compact
        // `Synced` or `Override` final-version vectors because their old-group
        // member count is only available from runtime state. Keep the compact
        // wire shape, defer resolution until that context can be supplied, and
        // add proposal decode tests for both compact variants with that work.
        let proposal = MigrationProposal::decode_proto_with(
            message,
            PendingGroupPayloadDecodeContext::default(),
        )
        .context(InvalidPendingGroupPayloadSnafu)?;
        Self::try_new(proposal, Arc::new(group_setup))
    }
}

/// Private target-group key material carried by an invitation or proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GroupSetupMessage {
    members: Vec<MemberIdentity>,
    member_keys: TrieMap<BootstrapMemberKeyMessage>,
    group_cipher_suite: GroupCipherSuite,
    group_key: GroupSetupKey,
}

impl GroupSetupMessage {
    /// Build setup material whose member list and member-key map cover
    /// exactly the same identities.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeMessageError`] if the group is empty, no member-key
    /// references are present, the member-key map does not match the member
    /// list, or an inline public bundle is bound to a different member.
    pub(crate) fn new(
        members: Vec<MemberIdentity>,
        member_keys: TrieMap<BootstrapMemberKeyMessage>,
        group_cipher_suite: GroupCipherSuite,
        group_key: GroupSetupKey,
    ) -> Result<Self, RuntimeMessageError> {
        validate_bootstrap_member_key_coverage(&members, &member_keys)?;
        Ok(Self {
            members,
            member_keys,
            group_cipher_suite,
            group_key,
        })
    }

    pub(crate) fn members(&self) -> &[MemberIdentity] {
        &self.members
    }

    pub(crate) fn member_keys(&self) -> &TrieMap<BootstrapMemberKeyMessage> {
        &self.member_keys
    }

    /// Return ordered exact member-key references for persisted group metadata.
    pub(crate) fn ordered_member_key_ids(&self) -> Vec<MemberKeyId> {
        self.members
            .iter()
            .map(|member_id| {
                let member_key = self
                    .member_keys
                    .get(member_id)
                    .expect("bootstrap member-key coverage is validated at construction");
                MemberKeyId {
                    member_id: member_id.clone(),
                    fingerprint: member_key.fingerprint(),
                }
            })
            .collect()
    }

    pub(crate) fn group_key(&self) -> &GroupSetupKey {
        &self.group_key
    }
}

impl proto::ProtoCodec for GroupSetupMessage {
    type DecodeError = RuntimeMessageError;
    type Proto = replication_proto::GroupSetup;

    fn to_proto(&self) -> Self::Proto {
        let member_keys = self
            .members
            .iter()
            .map(|member| {
                let member_key = self
                    .member_keys
                    .get(member)
                    .expect("bootstrap member key references must cover every member");
                BootstrapMemberKeyProtoSource {
                    member_id: member,
                    member_key,
                }
                .encode_proto()
            })
            .collect();
        replication_proto::GroupSetup {
            member_keys,
            group_cipher_suite: u32::from(self.group_cipher_suite.as_u16()),
            group_key: self.group_key.to_bytes().to_vec(),
            ..replication_proto::GroupSetup::default()
        }
    }

    fn from_proto(message: Self::Proto) -> Result<Self, Self::DecodeError> {
        if message.member_keys.is_empty() {
            return EmptyGroupSetupSnafu.fail();
        }
        let mut members = Vec::with_capacity(message.member_keys.len());
        let mut member_keys = TrieMap::new();
        for member_key in message.member_keys {
            let entry = BootstrapMemberKeyEntry::decode_proto(member_key)?;
            members.push(entry.member_id.clone());
            member_keys.insert(entry.member_id, entry.member_key);
        }
        let expected = GROUP_CIPHER_SUITE_CHACHA20_POLY1305.as_u16();
        ensure!(
            message.group_cipher_suite == u32::from(expected),
            UnsupportedGroupSetupCipherSuiteSnafu {
                actual: message.group_cipher_suite,
                expected,
            }
        );
        let group_key =
            fixed_bytes_field::<GROUP_KEY_LENGTH>("group_setup.group_key", &message.group_key)?;
        Self::new(
            members,
            member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            GroupSetupKey::from_bytes(group_key),
        )
    }
}

impl DecodeProtoView for GroupSetupMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::GroupSetupView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        if message.member_keys.is_empty() {
            return EmptyGroupSetupSnafu.fail();
        }
        let mut members = Vec::with_capacity(message.member_keys.len());
        let mut member_keys = TrieMap::new();
        for member_key in &message.member_keys {
            let entry = BootstrapMemberKeyEntry::decode_proto_view(member_key)?;
            members.push(entry.member_id.clone());
            member_keys.insert(entry.member_id, entry.member_key);
        }
        let expected = GROUP_CIPHER_SUITE_CHACHA20_POLY1305.as_u16();
        ensure!(
            message.group_cipher_suite == u32::from(expected),
            UnsupportedGroupSetupCipherSuiteSnafu {
                actual: message.group_cipher_suite,
                expected,
            }
        );
        let group_key =
            fixed_bytes_field::<GROUP_KEY_LENGTH>("group_setup.group_key", message.group_key)?;
        Self::new(
            members,
            member_keys,
            GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
            GroupSetupKey::from_bytes(group_key),
        )
    }
}

/// Expected key fingerprint and optional inline public bundle for one bootstrap member.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BootstrapMemberKeyMessage {
    fingerprint: KeyFingerprint,
    public_keys: Option<PublicMemberKeys>,
}

impl BootstrapMemberKeyMessage {
    /// Build a fingerprint-only bootstrap member-key reference.
    pub(crate) const fn from_fingerprint(fingerprint: KeyFingerprint) -> Self {
        Self {
            fingerprint,
            public_keys: None,
        }
    }

    /// Copy typed public keys into the inline bootstrap member-key representation.
    pub(crate) fn from_public_keys(public_keys: &PublicMemberKeys) -> Self {
        Self {
            fingerprint: public_keys.fingerprint(),
            public_keys: Some(public_keys.clone()),
        }
    }

    pub(crate) const fn fingerprint(&self) -> KeyFingerprint {
        self.fingerprint
    }

    pub(crate) fn public_keys(&self) -> Option<&PublicMemberKeys> {
        self.public_keys.as_ref()
    }
}

/// Borrowed source for encoding one bootstrap member-key entry.
struct BootstrapMemberKeyProtoSource<'a> {
    /// Member identity that owns the referenced public key material.
    member_id: &'a MemberIdentity,
    /// Fingerprint and optional inline public bundle for the member.
    member_key: &'a BootstrapMemberKeyMessage,
}

impl EncodeProto for BootstrapMemberKeyProtoSource<'_> {
    type Proto = replication_proto::BootstrapMemberKey;

    fn encode_proto(&self) -> Self::Proto {
        let public_key_bundle = match self.member_key.public_keys() {
            Some(public_keys) => MessageField::some(public_keys.encode_proto()),
            None => MessageField::none(),
        };
        replication_proto::BootstrapMemberKey {
            member_id: MessageField::some(member_identity_to_wire_format(self.member_id)),
            key_fingerprint: self.member_key.fingerprint().as_ref().to_vec(),
            public_key_bundle,
            ..replication_proto::BootstrapMemberKey::default()
        }
    }
}

/// Decoded bootstrap member-key entry before coverage validation.
struct BootstrapMemberKeyEntry {
    /// Member identity decoded from the entry key field.
    member_id: MemberIdentity,
    /// Key fingerprint and optional inline public bundle decoded for the member.
    member_key: BootstrapMemberKeyMessage,
}

impl DecodeProto for BootstrapMemberKeyEntry {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::BootstrapMemberKey;

    fn decode_proto(mut message: Self::Proto) -> Result<Self, Self::Error> {
        let Some(member_id_wire) = message.member_id.take() else {
            return MissingBootstrapMemberIdSnafu.fail();
        };
        let member_id =
            member_identity_from_wire(member_id_wire, "group_setup.member_keys.member_id")
                .context(InvalidWireValueSnafu {
                    field: "group_setup.member_keys.member_id",
                })?;
        let fingerprint = fixed_bytes_field::<KEY_FINGERPRINT_LENGTH>(
            "group_setup.member_keys.key_fingerprint",
            &message.key_fingerprint,
        )
        .map(KeyFingerprint::from_bytes)?;
        let public_keys = message
            .public_key_bundle
            .take()
            .map(|bundle| decode_bootstrap_public_key_bundle(&member_id, fingerprint, bundle))
            .transpose()?;
        Ok(Self {
            member_id,
            member_key: BootstrapMemberKeyMessage {
                fingerprint,
                public_keys,
            },
        })
    }
}

impl DecodeProtoView for BootstrapMemberKeyEntry {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::BootstrapMemberKeyView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let Some(member_id_wire) = message.member_id.as_option() else {
            return MissingBootstrapMemberIdSnafu.fail();
        };
        let member_id = message_wire::member_identity_from_wire_view(
            member_id_wire,
            "group_setup.member_keys.member_id",
        )
        .map_err(WireValueDecodeError::from)
        .context(InvalidWireValueSnafu {
            field: "group_setup.member_keys.member_id",
        })?;
        let fingerprint = fixed_bytes_field::<KEY_FINGERPRINT_LENGTH>(
            "group_setup.member_keys.key_fingerprint",
            message.key_fingerprint,
        )
        .map(KeyFingerprint::from_bytes)?;
        let public_keys = message
            .public_key_bundle
            .as_option()
            .map(|bundle| decode_bootstrap_public_key_bundle_view(&member_id, fingerprint, bundle))
            .transpose()?;
        Ok(Self {
            member_id,
            member_key: BootstrapMemberKeyMessage {
                fingerprint,
                public_keys,
            },
        })
    }
}

/// Decode and validate one owned inline public key bundle from a bootstrap entry.
fn decode_bootstrap_public_key_bundle(
    member_id: &MemberIdentity,
    expected_fingerprint: KeyFingerprint,
    bundle: security_proto::PublicKeyBundle,
) -> Result<PublicMemberKeys, RuntimeMessageError> {
    let public_keys = PublicMemberKeys::decode_proto_with(bundle, member_id.clone()).context(
        InvalidBootstrapPublicKeyBundleSnafu {
            member_id: member_id.clone(),
        },
    )?;
    validate_bootstrap_public_key_bundle_matches_fingerprint(
        member_id,
        expected_fingerprint,
        &public_keys,
    )?;
    Ok(public_keys)
}

/// Decode and validate one borrowed inline public key bundle from a bootstrap entry.
fn decode_bootstrap_public_key_bundle_view(
    member_id: &MemberIdentity,
    expected_fingerprint: KeyFingerprint,
    bundle: &security_proto::PublicKeyBundleView<'_>,
) -> Result<PublicMemberKeys, RuntimeMessageError> {
    let public_keys = PublicMemberKeys::decode_proto_view_with(bundle, member_id.clone()).context(
        InvalidBootstrapPublicKeyBundleSnafu {
            member_id: member_id.clone(),
        },
    )?;
    validate_bootstrap_public_key_bundle_matches_fingerprint(
        member_id,
        expected_fingerprint,
        &public_keys,
    )?;
    Ok(public_keys)
}

/// Ensure an inline public key bundle derives its advertised bootstrap fingerprint.
pub(crate) fn validate_bootstrap_public_key_bundle_matches_fingerprint(
    member_id: &MemberIdentity,
    expected_fingerprint: KeyFingerprint,
    public_keys: &PublicMemberKeys,
) -> Result<(), RuntimeMessageError> {
    let actual = public_keys.fingerprint();
    ensure!(
        actual == expected_fingerprint,
        BootstrapPublicKeyBundleFingerprintMismatchSnafu {
            member_id: member_id.clone(),
            expected: expected_fingerprint,
            actual,
        }
    );
    Ok(())
}

/// Group key carried in a bootstrap payload.
///
/// Runtime messages are cloned in tests and route handoff, so the key is shared
/// through an `Arc`. The underlying [`GroupKey`] owns zeroisation on final drop.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GroupSetupKey(Arc<GroupKey>);

impl GroupSetupKey {
    pub(crate) fn from_group_key(group_key: GroupKey) -> Self {
        Self(Arc::new(group_key))
    }

    pub(crate) fn from_bytes(bytes: [u8; GROUP_KEY_LENGTH]) -> Self {
        Self(Arc::new(GroupKey::from_bytes(bytes)))
    }

    pub(crate) fn as_group_key(&self) -> &GroupKey {
        &self.0
    }

    fn to_bytes(&self) -> [u8; GROUP_KEY_LENGTH] {
        self.0.to_bytes()
    }
}

impl fmt::Debug for GroupSetupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GroupSetupKey").field(&"<redacted>").finish()
    }
}

#[derive(Clone, Debug, PartialEq, View)]
pub(crate) struct UpdateMessage {
    pub(crate) group_id: GroupId,
    pub(crate) update_id: UpdateId,
    pub(crate) read_versions: VersionVector,
    pub(crate) dataset_updates: Vec<DatasetUpdateMessage>,
}

impl EncodeProto for UpdateMessage {
    type Proto = replication_proto::Update;

    fn encode_proto(&self) -> Self::Proto {
        self.view().encode_proto()
    }
}

impl proto::ProtoCodecWith<MemberCountContext> for UpdateMessage {
    type DecodeError = RuntimeMessageError;

    fn from_proto_with(
        proto: <Self as EncodeProto>::Proto,
        context: MemberCountContext,
    ) -> Result<Self, Self::DecodeError> {
        let message = WireUpdateMessage::decode_proto(proto)?;
        message.into_runtime(context.member_count())
    }
}

impl DecodeProtoViewWith<MemberCountContext> for UpdateMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::UpdateView<'a>;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        context: MemberCountContext,
    ) -> Result<Self, Self::Error> {
        let message = WireUpdateMessage::decode_proto_view(proto)?;
        message.into_runtime(context.member_count())
    }
}

/// Convert a stored update-log record into the catch-up wire payload.
///
/// Store-local metadata such as the recorded sender and `applied_locally` state
/// is intentionally dropped: receivers only need the update id, read versions,
/// and dataset operations to replay the missing update.
impl From<ReplicationUpdateRecord> for UpdateMessage {
    fn from(record: ReplicationUpdateRecord) -> Self {
        Self {
            group_id: record.group_id,
            update_id: record.update_id,
            read_versions: record.read_versions,
            dataset_updates: record
                .dataset_updates
                .into_iter()
                .map(DatasetUpdateMessage::from)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WireUpdateMessage {
    pub(crate) group_id: GroupId,
    pub(crate) update_id: UpdateId,
    /// Compact wire representation kept until the hosted group member count is known.
    read_versions: WireVersionVector,
    pub(crate) dataset_updates: Vec<DatasetUpdateMessage>,
}

impl DecodeProto for WireUpdateMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::Update;

    fn decode_proto(mut message: Self::Proto) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(&message.group_id, "update.group_id").context(
            InvalidWireValueSnafu {
                field: "update.group_id",
            },
        )?;
        let Some(update_id) = message.update_id.take() else {
            return MissingUpdateIdSnafu.fail();
        };
        let update_id = decode_update_id(update_id).context(InvalidUpdateIdSnafu {
            field: "update.update_id",
        })?;
        ensure_update_id_version_bound(update_id)?;
        let Some(read_versions) = message.read_versions.take() else {
            return MissingReadVersionsSnafu.fail();
        };
        let read_versions =
            WireVersionVector::decode_proto(read_versions).context(InvalidReadVersionsSnafu {
                field: "update.read_versions",
            })?;
        if message.dataset_updates.is_empty() {
            return EmptyUpdateSnafu.fail();
        }
        let dataset_updates = message
            .dataset_updates
            .into_iter()
            .map(DatasetUpdateMessage::decode_proto)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            group_id,
            update_id,
            read_versions,
            dataset_updates,
        })
    }
}

impl DecodeProtoView for WireUpdateMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::UpdateView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(message.group_id, "update.group_id").context(
            InvalidWireValueSnafu {
                field: "update.group_id",
            },
        )?;
        let Some(update_id) = message.update_id.as_option() else {
            return MissingUpdateIdSnafu.fail();
        };
        let update_id = UpdateId {
            version: update_id.version,
            node_index: update_id.node_index,
        };
        ensure_update_id_version_bound(update_id)?;
        let Some(read_versions) = message.read_versions.as_option() else {
            return MissingReadVersionsSnafu.fail();
        };
        let read_versions = WireVersionVector::decode_proto_view(read_versions).context(
            InvalidReadVersionsSnafu {
                field: "update.read_versions",
            },
        )?;
        if message.dataset_updates.is_empty() {
            return EmptyUpdateSnafu.fail();
        }
        let dataset_updates = message
            .dataset_updates
            .iter()
            .map(DatasetUpdateMessage::decode_proto_view)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            group_id,
            update_id,
            read_versions,
            dataset_updates,
        })
    }
}

impl WireUpdateMessage {
    pub(crate) fn into_runtime(
        self,
        num_members: NonZeroUsize,
    ) -> Result<UpdateMessage, RuntimeMessageError> {
        let read_versions =
            self.read_versions
                .to_runtime(num_members)
                .context(InvalidReadVersionsSnafu {
                    field: "update.read_versions",
                })?;
        Ok(UpdateMessage {
            group_id: self.group_id,
            update_id: self.update_id,
            read_versions,
            dataset_updates: self.dataset_updates,
        })
    }
}

impl From<UpdateMessage> for WireUpdateMessage {
    fn from(message: UpdateMessage) -> Self {
        Self {
            group_id: message.group_id,
            update_id: message.update_id,
            read_versions: WireVersionVector::from_runtime(&message.read_versions),
            dataset_updates: message.dataset_updates,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SummaryRequestMessage {
    pub(crate) group_id: GroupId,
    pub(crate) correlation_id: Uuid,
}

impl proto::ProtoCodec for SummaryRequestMessage {
    type DecodeError = RuntimeMessageError;
    type Proto = replication_proto::SummaryRequest;

    fn to_proto(&self) -> Self::Proto {
        replication_proto::SummaryRequest {
            group_id: self.group_id.0.as_bytes().to_vec(),
            correlation_id: self.correlation_id.as_bytes().to_vec(),
            ..replication_proto::SummaryRequest::default()
        }
    }

    fn from_proto(message: Self::Proto) -> Result<Self, Self::DecodeError> {
        let group_id = group_id_from_wire(&message.group_id, "summary_request.group_id").context(
            InvalidWireValueSnafu {
                field: "summary_request.group_id",
            },
        )?;
        let correlation_id =
            correlation_id_from_wire(&message.correlation_id, "summary_request.correlation_id")?;
        Ok(Self {
            group_id,
            correlation_id,
        })
    }
}

impl DecodeProtoView for SummaryRequestMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::SummaryRequestView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(message.group_id, "summary_request.group_id").context(
            InvalidWireValueSnafu {
                field: "summary_request.group_id",
            },
        )?;
        let correlation_id =
            correlation_id_from_wire(message.correlation_id, "summary_request.correlation_id")?;
        Ok(Self {
            group_id,
            correlation_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, View)]
pub(crate) struct SummaryVersionsMessage<V> {
    pub(crate) group_id: GroupId,
    pub(crate) correlation_id: Uuid,
    pub(crate) has_versions: V,
}

impl<V> SummaryVersionsMessage<V> {
    pub(crate) fn new(group_id: GroupId, correlation_id: Uuid, has_versions: V) -> Self {
        Self {
            group_id,
            correlation_id,
            has_versions,
        }
    }
}

pub(crate) type SummaryMessage = SummaryVersionsMessage<VersionVector>;
pub(crate) type WireSummaryMessage = SummaryVersionsMessage<WireVersionVector>;

impl EncodeProto for SummaryMessage {
    type Proto = replication_proto::Summary;

    fn encode_proto(&self) -> Self::Proto {
        self.view().encode_proto()
    }
}

impl proto::ProtoCodecWith<MemberCountContext> for SummaryMessage {
    type DecodeError = RuntimeMessageError;

    fn from_proto_with(
        proto: <Self as EncodeProto>::Proto,
        context: MemberCountContext,
    ) -> Result<Self, Self::DecodeError> {
        let message = WireSummaryMessage::decode_proto(proto)?;
        message.into_runtime(context.member_count())
    }
}

impl DecodeProtoViewWith<MemberCountContext> for SummaryMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::SummaryView<'a>;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        context: MemberCountContext,
    ) -> Result<Self, Self::Error> {
        let message = WireSummaryMessage::decode_proto_view(proto)?;
        message.into_runtime(context.member_count())
    }
}

impl DecodeProto for WireSummaryMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::Summary;

    fn decode_proto(mut message: Self::Proto) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(&message.group_id, "summary.group_id").context(
            InvalidWireValueSnafu {
                field: "summary.group_id",
            },
        )?;
        let correlation_id =
            correlation_id_from_wire(&message.correlation_id, "summary.correlation_id")?;
        let Some(has_versions) = message.has_versions.take() else {
            return MissingSummaryVersionsSnafu.fail();
        };
        let has_versions =
            WireVersionVector::decode_proto(has_versions).context(InvalidReadVersionsSnafu {
                field: "summary.has_versions",
            })?;
        Ok(Self::new(group_id, correlation_id, has_versions))
    }
}

impl DecodeProtoView for WireSummaryMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::SummaryView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(message.group_id, "summary.group_id").context(
            InvalidWireValueSnafu {
                field: "summary.group_id",
            },
        )?;
        let correlation_id =
            correlation_id_from_wire(message.correlation_id, "summary.correlation_id")?;
        let Some(has_versions) = message.has_versions.as_option() else {
            return MissingSummaryVersionsSnafu.fail();
        };
        let has_versions = WireVersionVector::decode_proto_view(has_versions).context(
            InvalidReadVersionsSnafu {
                field: "summary.has_versions",
            },
        )?;
        Ok(Self::new(group_id, correlation_id, has_versions))
    }
}

impl WireSummaryMessage {
    pub(crate) fn into_runtime(
        self,
        num_members: NonZeroUsize,
    ) -> Result<SummaryMessage, RuntimeMessageError> {
        let has_versions =
            self.has_versions
                .to_runtime(num_members)
                .context(InvalidReadVersionsSnafu {
                    field: "summary.has_versions",
                })?;
        Ok(SummaryMessage::new(
            self.group_id,
            self.correlation_id,
            has_versions,
        ))
    }

    pub(crate) fn into_summary(
        self,
        responder: MemberIdentity,
        member_count: NonZeroUsize,
    ) -> Result<Summary, RuntimeMessageError> {
        let summary_message = self.into_runtime(member_count)?;
        Ok(Summary {
            group_id: summary_message.group_id,
            responder,
            has_versions: summary_message.has_versions,
        })
    }
}

/// Inclusive producer-version range for one canonical group member.
///
/// Decoded wire values are normalised so `end_version >= start_version`, and
/// every bound is at most [`MAX_VERSION_VALUE`]. Runtime-created values must
/// preserve the same invariant before they reach catch-up tracking.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct UpdateRangeMessage {
    /// Canonical group member index of the producer whose versions are requested.
    pub(crate) producer_index: u32,
    /// Inclusive first producer version in this range.
    ///
    /// Runtime-created values must already respect [`MAX_VERSION_VALUE`].
    pub(crate) start_version: u64,
    /// Inclusive last producer version in this range.
    ///
    /// This must be greater than or equal to `start_version`. Runtime-created
    /// values must already respect [`MAX_VERSION_VALUE`].
    pub(crate) end_version: u64,
}

impl From<UpdateId> for UpdateRangeMessage {
    fn from(update_id: UpdateId) -> Self {
        Self {
            producer_index: update_id.node_index,
            start_version: update_id.version,
            end_version: update_id.version,
        }
    }
}

impl From<VersionVectorGap> for UpdateRangeMessage {
    fn from(gap: VersionVectorGap) -> Self {
        Self {
            producer_index: u32::try_from(gap.member_index)
                .expect("group member count must fit producer index"),
            start_version: gap.start_version,
            end_version: gap.end_version,
        }
    }
}

impl proto::ProtoCodec for UpdateRangeMessage {
    type DecodeError = RuntimeMessageError;
    type Proto = replication_proto::UpdateRange;

    fn to_proto(&self) -> Self::Proto {
        replication_proto::UpdateRange {
            producer_index: self.producer_index,
            start_version: self.start_version,
            end_version: option_when!(self.end_version != self.start_version, self.end_version),
            ..replication_proto::UpdateRange::default()
        }
    }

    fn from_proto(message: Self::Proto) -> Result<Self, Self::DecodeError> {
        let end_version = message.end_version.unwrap_or(message.start_version);
        ensure!(
            message.start_version <= end_version,
            InvalidNeedRangeSnafu {
                producer_index: message.producer_index,
                start_version: message.start_version,
                end_version,
            }
        );
        for version in [message.start_version, end_version] {
            ensure!(
                version <= MAX_VERSION_VALUE,
                NeedRangeBoundTooLargeSnafu {
                    producer_index: message.producer_index,
                    version,
                }
            );
        }
        Ok(Self {
            producer_index: message.producer_index,
            start_version: message.start_version,
            end_version,
        })
    }
}

impl DecodeProtoView for UpdateRangeMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::UpdateRangeView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let end_version = message.end_version.unwrap_or(message.start_version);
        ensure!(
            message.start_version <= end_version,
            InvalidNeedRangeSnafu {
                producer_index: message.producer_index,
                start_version: message.start_version,
                end_version,
            }
        );
        for version in [message.start_version, end_version] {
            ensure!(
                version <= MAX_VERSION_VALUE,
                NeedRangeBoundTooLargeSnafu {
                    producer_index: message.producer_index,
                    version,
                }
            );
        }
        Ok(Self {
            producer_index: message.producer_index,
            start_version: message.start_version,
            end_version,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NeedRangeMessage {
    pub(crate) group_id: GroupId,
    pub(crate) ranges: Vec<UpdateRangeMessage>,
}

impl proto::ProtoCodec for NeedRangeMessage {
    type DecodeError = RuntimeMessageError;
    type Proto = replication_proto::NeedRange;

    fn to_proto(&self) -> Self::Proto {
        replication_proto::NeedRange {
            group_id: self.group_id.0.as_bytes().to_vec(),
            ranges: self
                .ranges
                .iter()
                .map(UpdateRangeMessage::encode_proto)
                .collect(),
            ..replication_proto::NeedRange::default()
        }
    }

    fn from_proto(message: Self::Proto) -> Result<Self, Self::DecodeError> {
        let group_id = group_id_from_wire(&message.group_id, "need_range.group_id").context(
            InvalidWireValueSnafu {
                field: "need_range.group_id",
            },
        )?;
        if message.ranges.is_empty() {
            return EmptyNeedRangeSnafu.fail();
        }
        let ranges = message
            .ranges
            .into_iter()
            .map(UpdateRangeMessage::decode_proto)
            .collect::<Result<_, _>>()?;
        Ok(Self { group_id, ranges })
    }
}

impl DecodeProtoView for NeedRangeMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::NeedRangeView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(message.group_id, "need_range.group_id").context(
            InvalidWireValueSnafu {
                field: "need_range.group_id",
            },
        )?;
        if message.ranges.is_empty() {
            return EmptyNeedRangeSnafu.fail();
        }
        let ranges = message
            .ranges
            .iter()
            .map(UpdateRangeMessage::decode_proto_view)
            .collect::<Result<_, _>>()?;
        Ok(Self { group_id, ranges })
    }
}

#[derive(Clone, Debug, PartialEq, View)]
pub(crate) struct UpdateBatchMessage {
    pub(crate) group_id: GroupId,
    pub(crate) updates: Vec<UpdateMessage>,
}

impl EncodeProto for UpdateBatchMessage {
    type Proto = replication_proto::UpdateBatch;

    fn encode_proto(&self) -> Self::Proto {
        self.view().encode_proto()
    }
}

impl proto::ProtoCodecWith<MemberCountContext> for UpdateBatchMessage {
    type DecodeError = RuntimeMessageError;

    fn from_proto_with(
        proto: <Self as EncodeProto>::Proto,
        context: MemberCountContext,
    ) -> Result<Self, Self::DecodeError> {
        let message = WireUpdateBatchMessage::decode_proto(proto)?;
        message.into_runtime(context.member_count())
    }
}

impl DecodeProtoViewWith<MemberCountContext> for UpdateBatchMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::UpdateBatchView<'a>;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        context: MemberCountContext,
    ) -> Result<Self, Self::Error> {
        let message = WireUpdateBatchMessage::decode_proto_view(proto)?;
        message.into_runtime(context.member_count())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WireUpdateBatchMessage {
    pub(crate) group_id: GroupId,
    pub(crate) updates: Vec<WireUpdateMessage>,
}

impl From<UpdateBatchMessage> for WireUpdateBatchMessage {
    fn from(message: UpdateBatchMessage) -> Self {
        Self {
            group_id: message.group_id,
            updates: message
                .updates
                .into_iter()
                .map(WireUpdateMessage::from)
                .collect(),
        }
    }
}

impl DecodeProto for WireUpdateBatchMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::UpdateBatch;

    fn decode_proto(message: Self::Proto) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(&message.group_id, "update_batch.group_id").context(
            InvalidWireValueSnafu {
                field: "update_batch.group_id",
            },
        )?;
        if message.updates.is_empty() {
            return EmptyUpdateBatchSnafu.fail();
        }
        let updates: Vec<WireUpdateMessage> = message
            .updates
            .into_iter()
            .map(WireUpdateMessage::decode_proto)
            .collect::<Result<_, _>>()?;
        for update in &updates {
            ensure!(
                update.group_id == group_id,
                UpdateBatchGroupMismatchSnafu {
                    batch_group: group_id,
                    update_group: update.group_id,
                    update: update.update_id,
                }
            );
        }
        Ok(Self { group_id, updates })
    }
}

impl DecodeProtoView for WireUpdateBatchMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::UpdateBatchView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(message.group_id, "update_batch.group_id").context(
            InvalidWireValueSnafu {
                field: "update_batch.group_id",
            },
        )?;
        if message.updates.is_empty() {
            return EmptyUpdateBatchSnafu.fail();
        }
        let updates: Vec<WireUpdateMessage> = message
            .updates
            .iter()
            .map(WireUpdateMessage::decode_proto_view)
            .collect::<Result<_, _>>()?;
        for update in &updates {
            ensure!(
                update.group_id == group_id,
                UpdateBatchGroupMismatchSnafu {
                    batch_group: group_id,
                    update_group: update.group_id,
                    update: update.update_id,
                }
            );
        }
        Ok(Self { group_id, updates })
    }
}

impl WireUpdateBatchMessage {
    pub(crate) fn into_runtime(
        self,
        num_members: NonZeroUsize,
    ) -> Result<UpdateBatchMessage, RuntimeMessageError> {
        let updates = self
            .updates
            .into_iter()
            .map(|update| update.into_runtime(num_members))
            .collect::<Result<_, _>>()?;
        Ok(UpdateBatchMessage {
            group_id: self.group_id,
            updates,
        })
    }
}

/// Compact wire-only form for version vectors used inside inbound updates.
///
/// The runtime message model keeps a full `VersionVector`, while the protobuf
/// wire shape prefers more compact encodings such as "all members synced" or
/// "one member ahead" when possible. This stays separate from the generated
/// protobuf type so the runtime can validate and normalise wire values before
/// they become a domain `VersionVector`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WireVersionVector {
    Full(PureVersionVector),
    Override {
        group_version: u64,
        override_position: u32,
        override_version: u64,
    },
    Synced {
        group_version: u64,
    },
}

impl WireVersionVector {
    pub(crate) fn from_runtime(version_vector: &VersionVector) -> Self {
        match version_vector {
            VersionVector::Full(vector) => Self::Full(vector.clone()),
            VersionVector::Override { version, .. } => Self::Override {
                group_version: version.group_version(),
                override_position: u32::try_from(version.override_position)
                    .expect("wire version override position must fit into u32"),
                override_version: version.override_version(),
            },
            VersionVector::Synced { version, .. } => Self::Synced {
                group_version: *version,
            },
        }
    }

    pub(crate) fn to_runtime(
        &self,
        num_members: NonZeroUsize,
    ) -> Result<VersionVector, WireVersionVectorError> {
        match self {
            WireVersionVector::Full(vector) => {
                let actual_members = vector.len().get();
                let expected_members = num_members.get();
                ensure!(
                    actual_members == expected_members,
                    MemberCountMismatchSnafu {
                        expected_members,
                        actual_members,
                    }
                );
                Ok(VersionVector::Full(vector.clone()))
            }
            WireVersionVector::Override {
                group_version,
                override_position,
                override_version,
            } => {
                let override_position_index = usize::try_from(*override_position)
                    .expect("wire override position must fit in usize");
                ensure!(
                    override_position_index < num_members.get(),
                    InvalidOverridePositionSnafu {
                        num_members: num_members.get(),
                        override_position: *override_position,
                    }
                );
                let version = OverrideVersion::new_opt(
                    *group_version,
                    override_position_index,
                    *override_version,
                )
                .context(InvalidOverrideSnafu {
                    group_version: *group_version,
                    override_position: *override_position,
                    override_version: *override_version,
                })?;
                Ok(VersionVector::Override {
                    num_members,
                    version,
                })
            }
            WireVersionVector::Synced { group_version } => Ok(VersionVector::Synced {
                num_members,
                version: *group_version,
            }),
        }
    }
}

impl proto::ProtoCodec for WireVersionVector {
    type DecodeError = WireVersionVectorError;
    type Proto = versions_proto::VersionVector;

    fn to_proto(&self) -> Self::Proto {
        let versions = match self {
            WireVersionVector::Full(vector) => versions_proto::version_vector::Versions::Full(
                Box::new(versions_proto::FullVersionVector {
                    entries: vector.0.to_vec(),
                    ..versions_proto::FullVersionVector::default()
                }),
            ),
            WireVersionVector::Override {
                group_version,
                override_position,
                override_version,
            } => versions_proto::version_vector::Versions::Override(Box::new(
                versions_proto::OverrideVersionVector {
                    group_version: *group_version,
                    override_position: *override_position,
                    override_version: *override_version,
                    ..versions_proto::OverrideVersionVector::default()
                },
            )),
            WireVersionVector::Synced { group_version } => {
                versions_proto::version_vector::Versions::Synced(Box::new(
                    versions_proto::SyncedVersionVector {
                        group_version: *group_version,
                        ..versions_proto::SyncedVersionVector::default()
                    },
                ))
            }
        };
        versions_proto::VersionVector {
            versions: Some(versions),
            ..versions_proto::VersionVector::default()
        }
    }

    fn from_proto(version_vector: Self::Proto) -> Result<Self, Self::DecodeError> {
        let Some(versions) = version_vector.versions else {
            return MissingVersionsBodySnafu.fail();
        };
        match versions {
            versions_proto::version_vector::Versions::Full(full) => {
                if full.entries.is_empty() {
                    return EmptyFullVectorSnafu.fail();
                }
                for version in full.entries.iter().copied() {
                    ensure_version_vector_bound("full.entries", version)?;
                }
                Ok(WireVersionVector::Full(PureVersionVector::from(
                    full.entries,
                )))
            }
            versions_proto::version_vector::Versions::Override(override_vector) => {
                ensure_version_vector_bound(
                    "override.group_version",
                    override_vector.group_version,
                )?;
                ensure_version_vector_bound(
                    "override.override_version",
                    override_vector.override_version,
                )?;
                Ok(WireVersionVector::Override {
                    group_version: override_vector.group_version,
                    override_position: override_vector.override_position,
                    override_version: override_vector.override_version,
                })
            }
            versions_proto::version_vector::Versions::Synced(synced) => {
                ensure_version_vector_bound("synced.group_version", synced.group_version)?;
                Ok(WireVersionVector::Synced {
                    group_version: synced.group_version,
                })
            }
        }
    }
}

impl DecodeProtoView for WireVersionVector {
    type Error = WireVersionVectorError;
    type ProtoView<'a> = versions_proto::VersionVectorView<'a>;

    fn decode_proto_view(version_vector: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let Some(versions) = version_vector.versions.as_ref() else {
            return MissingVersionsBodySnafu.fail();
        };
        match versions {
            versions_proto::version_vector::VersionsView::Full(full) => {
                if full.entries.is_empty() {
                    return EmptyFullVectorSnafu.fail();
                }
                for version in full.entries.iter().copied() {
                    ensure_version_vector_bound("full.entries", version)?;
                }
                Ok(WireVersionVector::Full(PureVersionVector::from(
                    full.entries.to_vec(),
                )))
            }
            versions_proto::version_vector::VersionsView::Override(override_vector) => {
                ensure_version_vector_bound(
                    "override.group_version",
                    override_vector.group_version,
                )?;
                ensure_version_vector_bound(
                    "override.override_version",
                    override_vector.override_version,
                )?;
                Ok(WireVersionVector::Override {
                    group_version: override_vector.group_version,
                    override_position: override_vector.override_position,
                    override_version: override_vector.override_version,
                })
            }
            versions_proto::version_vector::VersionsView::Synced(synced) => {
                ensure_version_vector_bound("synced.group_version", synced.group_version)?;
                Ok(WireVersionVector::Synced {
                    group_version: synced.group_version,
                })
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, View)]
pub(crate) struct DatasetUpdateMessage {
    pub(crate) dataset_id: DatasetId,
    pub(crate) operations: Vec<datamodel_proto::SchemaOperation>,
}

impl proto::ProtoCodec for DatasetUpdateMessage {
    type DecodeError = RuntimeMessageError;
    type Proto = replication_proto::DatasetUpdate;

    fn to_proto(&self) -> Self::Proto {
        self.view().encode_proto()
    }

    fn from_proto(message: Self::Proto) -> Result<Self, Self::DecodeError> {
        if message.operations.is_empty() {
            return EmptyDatasetUpdateSnafu {
                dataset_id: message.dataset_id,
            }
            .fail();
        }

        let dataset_id =
            DatasetId::try_new(message.dataset_id.clone()).context(InvalidDatasetIdSnafu {
                value: message.dataset_id,
            })?;
        Ok(Self {
            dataset_id,
            operations: message.operations,
        })
    }
}

impl DecodeProtoView for DatasetUpdateMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::DatasetUpdateView<'a>;

    fn decode_proto_view(message: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        if message.operations.is_empty() {
            return EmptyDatasetUpdateSnafu {
                dataset_id: message.dataset_id.to_owned(),
            }
            .fail();
        }

        let dataset_id_value = message.dataset_id.to_owned();
        let dataset_id =
            DatasetId::try_new(dataset_id_value.clone()).context(InvalidDatasetIdSnafu {
                value: dataset_id_value,
            })?;
        let operations = message
            .operations
            .iter()
            .map(flotsync_messages::buffa::MessageView::to_owned_message)
            .collect::<Result<Vec<_>, _>>()
            .context(DecodeSnafu)?;
        Ok(Self {
            dataset_id,
            operations,
        })
    }
}

impl From<DatasetUpdateRecord> for DatasetUpdateMessage {
    fn from(record: DatasetUpdateRecord) -> Self {
        Self {
            dataset_id: record.dataset_id,
            operations: record.operations,
        }
    }
}

impl From<DatasetUpdateMessage> for DatasetUpdateRecord {
    fn from(message: DatasetUpdateMessage) -> Self {
        Self {
            dataset_id: message.dataset_id,
            operations: message.operations,
        }
    }
}

/// Borrowed source for encoding a runtime version vector without first
/// constructing the compact owned wire representation.
pub(crate) struct RuntimeVersionVectorProtoSource<'a> {
    /// Runtime version vector to encode into the generated protobuf shape.
    version_vector: &'a VersionVector,
}

impl<'a> From<&'a VersionVector> for RuntimeVersionVectorProtoSource<'a> {
    fn from(version_vector: &'a VersionVector) -> Self {
        Self { version_vector }
    }
}

impl EncodeProto for RuntimeVersionVectorProtoSource<'_> {
    type Proto = versions_proto::VersionVector;

    fn encode_proto(&self) -> Self::Proto {
        let versions = match self.version_vector {
            VersionVector::Full(vector) => versions_proto::version_vector::Versions::Full(
                Box::new(versions_proto::FullVersionVector {
                    entries: vector.0.to_vec(),
                    ..versions_proto::FullVersionVector::default()
                }),
            ),
            VersionVector::Override { version, .. } => {
                versions_proto::version_vector::Versions::Override(Box::new(
                    versions_proto::OverrideVersionVector {
                        group_version: version.group_version(),
                        override_position: u32::try_from(version.override_position)
                            .expect("wire version override position must fit into u32"),
                        override_version: version.override_version(),
                        ..versions_proto::OverrideVersionVector::default()
                    },
                ))
            }
            VersionVector::Synced { version, .. } => {
                versions_proto::version_vector::Versions::Synced(Box::new(
                    versions_proto::SyncedVersionVector {
                        group_version: *version,
                        ..versions_proto::SyncedVersionVector::default()
                    },
                ))
            }
        };
        versions_proto::VersionVector {
            versions: Some(versions),
            ..versions_proto::VersionVector::default()
        }
    }
}

impl<'a> From<&'a DatasetUpdateRecord> for DatasetUpdateMessageView<'a> {
    fn from(record: &'a DatasetUpdateRecord) -> Self {
        Self {
            dataset_id: &record.dataset_id,
            operations: &record.operations,
        }
    }
}

impl EncodeProto for DatasetUpdateMessageView<'_> {
    type Proto = replication_proto::DatasetUpdate;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::DatasetUpdate {
            dataset_id: self.dataset_id.as_str().to_owned(),
            operations: self.operations.to_vec(),
            ..replication_proto::DatasetUpdate::default()
        }
    }
}

impl EncodeProto for UpdateMessageView<'_> {
    type Proto = replication_proto::Update;

    fn encode_proto(&self) -> Self::Proto {
        let read_versions =
            RuntimeVersionVectorProtoSource::from(self.read_versions).encode_proto();
        replication_proto::Update {
            group_id: self.group_id.0.as_bytes().to_vec(),
            update_id: MessageField::some(encode_update_id(*self.update_id)),
            read_versions: MessageField::some(read_versions),
            dataset_updates: DatasetUpdateProtoSources::Messages(self.dataset_updates)
                .encode_proto(),
            ..replication_proto::Update::default()
        }
    }
}

/// Borrowed source for encoding a persisted store record without an
/// intermediate owned `UpdateMessage`.
pub(crate) struct UpdateMessageProtoSource<'a> {
    /// Group whose log contains the update.
    group_id: GroupId,
    /// Producer and version of the update.
    update_id: UpdateId,
    /// Read-version frontier carried by the update.
    read_versions: &'a VersionVector,
    /// Dataset updates borrowed from the original source shape.
    dataset_updates: DatasetUpdateProtoSources<'a>,
}

impl<'a> From<&'a ReplicationUpdateRecord> for UpdateMessageProtoSource<'a> {
    fn from(record: &'a ReplicationUpdateRecord) -> Self {
        Self {
            group_id: record.group_id,
            update_id: record.update_id,
            read_versions: &record.read_versions,
            dataset_updates: DatasetUpdateProtoSources::Records(&record.dataset_updates),
        }
    }
}

impl EncodeProto for UpdateMessageProtoSource<'_> {
    type Proto = replication_proto::Update;

    fn encode_proto(&self) -> Self::Proto {
        let read_versions =
            RuntimeVersionVectorProtoSource::from(self.read_versions).encode_proto();
        replication_proto::Update {
            group_id: self.group_id.0.as_bytes().to_vec(),
            update_id: MessageField::some(encode_update_id(self.update_id)),
            read_versions: MessageField::some(read_versions),
            dataset_updates: self.dataset_updates.encode_proto(),
            ..replication_proto::Update::default()
        }
    }
}

impl EncodeProto for SummaryVersionsMessageView<'_, VersionVector> {
    type Proto = replication_proto::Summary;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::Summary {
            group_id: self.group_id.0.as_bytes().to_vec(),
            correlation_id: self.correlation_id.as_bytes().to_vec(),
            has_versions: MessageField::some(
                RuntimeVersionVectorProtoSource::from(self.has_versions).encode_proto(),
            ),
            ..replication_proto::Summary::default()
        }
    }
}

impl EncodeProto for UpdateBatchMessageView<'_> {
    type Proto = replication_proto::UpdateBatch;

    fn encode_proto(&self) -> Self::Proto {
        replication_proto::UpdateBatch {
            group_id: self.group_id.0.as_bytes().to_vec(),
            updates: self
                .updates
                .iter()
                .map(|update| update.view().encode_proto())
                .collect(),
            ..replication_proto::UpdateBatch::default()
        }
    }
}

/// Borrowed dataset-update collection source inside an update message.
enum DatasetUpdateProtoSources<'a> {
    /// Dataset updates borrowed from a runtime update message.
    Messages(&'a [DatasetUpdateMessage]),
    /// Dataset updates borrowed from a persisted update record.
    Records(&'a [DatasetUpdateRecord]),
}

impl DatasetUpdateProtoSources<'_> {
    /// Encode every borrowed dataset update into generated protobuf entries.
    fn encode_proto(&self) -> Vec<replication_proto::DatasetUpdate> {
        match self {
            Self::Messages(dataset_updates) => dataset_updates
                .iter()
                .map(|message| message.view().encode_proto())
                .collect(),
            Self::Records(dataset_updates) => dataset_updates
                .iter()
                .map(|record| DatasetUpdateMessageView::from(record).encode_proto())
                .collect(),
        }
    }
}

fn ensure_update_id_version_bound(update_id: UpdateId) -> Result<(), RuntimeMessageError> {
    ensure!(
        update_id.version <= MAX_VERSION_VALUE,
        UpdateVersionBoundTooLargeSnafu {
            update_id,
            version: update_id.version,
        }
    );
    Ok(())
}

fn fixed_bytes_field<const N: usize>(
    field: &'static str,
    bytes: &[u8],
) -> Result<[u8; N], RuntimeMessageError> {
    bytes
        .try_into()
        .map_err(|_| RuntimeMessageError::InvalidByteLength {
            field,
            expected: N,
            actual: bytes.len(),
        })
}

/// Ensure bootstrap members and member-key entries are a one-to-one match.
fn validate_bootstrap_member_key_coverage(
    members: &[MemberIdentity],
    member_keys: &TrieMap<BootstrapMemberKeyMessage>,
) -> Result<(), RuntimeMessageError> {
    if members.is_empty() {
        return EmptyGroupSetupSnafu.fail();
    }
    if member_keys.is_empty() {
        return EmptyBootstrapMemberKeysSnafu.fail();
    }
    if member_keys.len() != members.len() {
        return BootstrapMemberKeyCountMismatchSnafu {
            member_count: members.len(),
            member_key_count: member_keys.len(),
        }
        .fail();
    }
    for member_id in members {
        let Some(member_key) = member_keys.get(member_id) else {
            return MissingBootstrapMemberKeySnafu {
                member_id: member_id.clone(),
            }
            .fail();
        };
        if let Some(public_keys) = member_key.public_keys()
            && public_keys.member_id() != member_id
        {
            return BootstrapMemberKeyBindingMismatchSnafu {
                member_id: member_id.clone(),
                public_key_member_id: public_keys.member_id().clone(),
            }
            .fail();
        }
    }
    Ok(())
}

fn ensure_version_vector_bound(
    field: &'static str,
    version: u64,
) -> Result<(), WireVersionVectorError> {
    ensure!(
        version <= MAX_VERSION_VALUE,
        VersionBoundTooLargeSnafu { field, version }
    );
    Ok(())
}

fn correlation_id_from_wire(
    bytes: &[u8],
    field: &'static str,
) -> Result<Uuid, RuntimeMessageError> {
    Uuid::from_slice(bytes).context(InvalidCorrelationIdSnafu { field })
}

#[cfg(test)]
mod tests {
    use super::{
        BootstrapMemberKeyMessage,
        DatasetUpdateMessage,
        DatasetUpdateMessageView,
        GroupInvitationMessage,
        GroupSetupKey,
        GroupSetupMessage,
        MemberCountContext,
        MigrationProposalMessage,
        NeedRangeMessage,
        RuntimeMessage,
        RuntimeMessageError,
        RuntimeVersionVectorProtoSource,
        SummaryMessage,
        SummaryRequestMessage,
        UpdateBatchMessage,
        UpdateMessage,
        UpdateMessageProtoSource,
        UpdateRangeMessage,
        WireRuntimeMessage,
        WireVersionVector,
        WireVersionVectorError,
    };
    use crate::{
        api::{
            DatasetId,
            DatasetUpdateRecord,
            GroupInvitation,
            InitialSnapshot,
            MigrationId,
            MigrationProposal,
            ReplicationUpdateRecord,
        },
        test_support::{docs_group_schema, test_public_member_keys},
    };
    use flotsync_core::{
        GroupId,
        MemberIdentity,
        member::TrieMap,
        versions::{OverrideVersion, PureVersionVector, UpdateId, VersionVector},
    };
    use flotsync_messages::{
        buffa::{Message as _, MessageView as _},
        datamodel as datamodel_proto,
        proto::{DecodeProto, DecodeProtoView, DecodeProtoViewWith, DecodeProtoWith, EncodeProto},
        replication as replication_proto,
        versions as versions_proto,
    };
    use flotsync_security::{GROUP_CIPHER_SUITE_CHACHA20_POLY1305, GROUP_KEY_LENGTH};
    use std::{num::NonZeroUsize, sync::Arc};
    use uuid::Uuid;

    fn test_update_message(
        group_id: GroupId,
        update_id: UpdateId,
        read_versions: VersionVector,
    ) -> UpdateMessage {
        UpdateMessage {
            group_id,
            update_id,
            read_versions,
            dataset_updates: vec![DatasetUpdateMessage {
                dataset_id: DatasetId::try_new("docs").expect("dataset id should build"),
                operations: vec![datamodel_proto::SchemaOperation::default()],
            }],
        }
    }

    fn test_group_setup(members: &[MemberIdentity]) -> Arc<GroupSetupMessage> {
        let mut member_keys = TrieMap::new();
        for member in members {
            member_keys.insert(
                member.clone(),
                BootstrapMemberKeyMessage::from_public_keys(&test_public_member_keys(member)),
            );
        }
        Arc::new(
            GroupSetupMessage::new(
                members.to_vec(),
                member_keys,
                GROUP_CIPHER_SUITE_CHACHA20_POLY1305,
                GroupSetupKey::from_bytes([5; GROUP_KEY_LENGTH]),
            )
            .expect("test group setup should build"),
        )
    }

    #[test]
    fn wire_version_vector_round_trips_full_override_and_synced() {
        let full = VersionVector::Full(PureVersionVector::from([2, 3, 4]));
        let override_vector = VersionVector::Override {
            num_members: NonZeroUsize::new(3).expect("three members"),
            version: OverrideVersion::new(7, 1, 8),
        };
        let synced = VersionVector::Synced {
            num_members: NonZeroUsize::new(3).expect("three members"),
            version: 11,
        };

        for vector in [full, override_vector, synced] {
            let wire = WireVersionVector::from_runtime(&vector);
            let proto = wire.encode_proto();
            let payload = proto.encode_to_bytes();
            let view = versions_proto::VersionVectorView::decode_view(&payload)
                .expect("view should decode");
            assert_eq!(
                WireVersionVector::decode_proto_view(&view).expect("wire view decode should work"),
                wire
            );
            let decoded_wire =
                WireVersionVector::decode_proto(proto).expect("wire decode should work");
            let decoded_vector = decoded_wire
                .to_runtime(vector.num_members())
                .expect("runtime decode should work");
            assert_eq!(decoded_vector, vector);
        }
    }

    #[test]
    fn summary_messages_round_trip_through_runtime_envelope() {
        let group_id = GroupId(Uuid::from_u128(101));
        let correlation_id = Uuid::from_u128(202);
        let summary_request = RuntimeMessage::SummaryRequest(SummaryRequestMessage {
            group_id,
            correlation_id,
        });
        let request_payload = summary_request.encode_proto().encode_to_bytes();

        assert_eq!(
            WireRuntimeMessage::decode_proto_view_from_slice(&request_payload)
                .expect("summary request should decode"),
            WireRuntimeMessage::SummaryRequest(SummaryRequestMessage {
                group_id,
                correlation_id,
            })
        );

        let has_versions = VersionVector::Full(PureVersionVector::from([2, 4]));
        let summary = RuntimeMessage::Summary(SummaryMessage::new(
            group_id,
            correlation_id,
            has_versions.clone(),
        ));
        let summary_payload = summary.encode_proto().encode_to_bytes();
        let decoded_summary = WireRuntimeMessage::decode_proto_view_from_slice(&summary_payload)
            .expect("summary should decode");

        let WireRuntimeMessage::Summary(decoded_summary) = decoded_summary else {
            panic!("summary payload should decode as a summary");
        };
        assert_eq!(
            decoded_summary
                .into_runtime(NonZeroUsize::new(2).expect("two members"))
                .expect("summary versions should normalise"),
            SummaryMessage::new(group_id, correlation_id, has_versions)
        );
    }

    #[test]
    fn updates_decode_with_member_count_context_from_owned_and_view() {
        let group_id = GroupId(Uuid::from_u128(211));
        let member_count = NonZeroUsize::new(2).expect("two members");
        let update = test_update_message(
            group_id,
            UpdateId {
                version: 3,
                node_index: 1,
            },
            VersionVector::Full(PureVersionVector::from([1, 2])),
        );

        assert_eq!(
            UpdateMessage::decode_proto_with(
                update.encode_proto(),
                MemberCountContext::new(member_count),
            )
            .expect("update should decode with member count"),
            update
        );
        let update_payload = update.encode_proto().encode_to_bytes();
        assert_eq!(
            UpdateMessage::decode_proto_from_slice_with(
                &update_payload,
                MemberCountContext::new(member_count),
            )
            .expect("update slice should decode with member count"),
            update
        );
        let mut update_payload_buf = update_payload.clone();
        assert_eq!(
            UpdateMessage::decode_proto_from_buf_with(
                &mut update_payload_buf,
                MemberCountContext::new(member_count),
            )
            .expect("update buffer should decode with member count"),
            update
        );
        assert_eq!(
            UpdateMessage::decode_proto_view_from_slice_with(
                &update_payload,
                MemberCountContext::new(member_count),
            )
            .expect("update view slice should decode with member count"),
            update
        );
        let update_view = replication_proto::UpdateView::decode_view(&update_payload)
            .expect("view should decode");
        assert_eq!(
            UpdateMessage::decode_proto_view_with(
                &update_view,
                MemberCountContext::new(member_count),
            )
            .expect("update view should decode with member count"),
            update
        );
    }

    #[test]
    fn update_batches_decode_with_member_count_context_from_owned_and_view() {
        let group_id = GroupId(Uuid::from_u128(211));
        let member_count = NonZeroUsize::new(2).expect("two members");
        let update = test_update_message(
            group_id,
            UpdateId {
                version: 3,
                node_index: 1,
            },
            VersionVector::Full(PureVersionVector::from([1, 2])),
        );
        let batch = UpdateBatchMessage {
            group_id,
            updates: vec![update],
        };

        assert_eq!(
            UpdateBatchMessage::decode_proto_with(
                batch.encode_proto(),
                MemberCountContext::new(member_count),
            )
            .expect("batch should decode with member count"),
            batch
        );
        let batch_payload = batch.encode_proto().encode_to_bytes();
        assert_eq!(
            UpdateBatchMessage::decode_proto_from_slice_with(
                &batch_payload,
                MemberCountContext::new(member_count),
            )
            .expect("batch slice should decode with member count"),
            batch
        );
        let mut batch_payload_buf = batch_payload.clone();
        assert_eq!(
            UpdateBatchMessage::decode_proto_from_buf_with(
                &mut batch_payload_buf,
                MemberCountContext::new(member_count),
            )
            .expect("batch buffer should decode with member count"),
            batch
        );
        assert_eq!(
            UpdateBatchMessage::decode_proto_view_from_slice_with(
                &batch_payload,
                MemberCountContext::new(member_count),
            )
            .expect("batch view slice should decode with member count"),
            batch
        );
        let batch_view = replication_proto::UpdateBatchView::decode_view(&batch_payload)
            .expect("view should decode");
        assert_eq!(
            UpdateBatchMessage::decode_proto_view_with(
                &batch_view,
                MemberCountContext::new(member_count),
            )
            .expect("batch view should decode with member count"),
            batch
        );
    }

    #[test]
    fn borrowed_proto_sources_match_owned_runtime_message_encoding() {
        let group_id = GroupId(Uuid::from_u128(212));
        let update = test_update_message(
            group_id,
            UpdateId {
                version: 4,
                node_index: 1,
            },
            VersionVector::Full(PureVersionVector::from([1, 3])),
        );
        let summary = SummaryMessage::new(
            group_id,
            Uuid::from_u128(213),
            VersionVector::Override {
                num_members: NonZeroUsize::new(2).expect("two members"),
                version: OverrideVersion::new(6, 1, 7),
            },
        );
        let batch = UpdateBatchMessage {
            group_id,
            updates: vec![update.clone()],
        };

        assert_eq!(
            RuntimeVersionVectorProtoSource::from(&update.read_versions)
                .encode_proto()
                .encode_to_bytes(),
            WireVersionVector::from_runtime(&update.read_versions)
                .encode_proto()
                .encode_to_bytes()
        );
        assert_eq!(
            update.dataset_updates[0]
                .view()
                .encode_proto()
                .encode_to_bytes(),
            update.dataset_updates[0].encode_proto().encode_to_bytes()
        );
        assert_eq!(
            update.view().encode_proto().encode_to_bytes(),
            update.encode_proto().encode_to_bytes()
        );
        assert_eq!(
            summary.view().encode_proto().encode_to_bytes(),
            summary.encode_proto().encode_to_bytes()
        );
        assert_eq!(
            batch.view().encode_proto().encode_to_bytes(),
            batch.encode_proto().encode_to_bytes()
        );
    }

    #[test]
    fn stored_update_proto_source_matches_owned_update_message_encoding() {
        let group_id = GroupId(Uuid::from_u128(214));
        let update = ReplicationUpdateRecord {
            group_id,
            update_id: UpdateId {
                version: 5,
                node_index: 0,
            },
            sender: MemberIdentity::from_array(["runtime-message", "sender"]),
            read_versions: VersionVector::Synced {
                num_members: NonZeroUsize::new(2).expect("two members"),
                version: 3,
            },
            dataset_updates: vec![DatasetUpdateRecord {
                dataset_id: DatasetId::try_new("docs").expect("dataset id should build"),
                operations: vec![datamodel_proto::SchemaOperation::default()],
            }],
            applied_locally: true,
        };
        let owned_message = UpdateMessage::from(update.clone());

        assert_eq!(
            DatasetUpdateMessageView::from(&update.dataset_updates[0])
                .encode_proto()
                .encode_to_bytes(),
            owned_message.dataset_updates[0]
                .encode_proto()
                .encode_to_bytes()
        );
        assert_eq!(
            UpdateMessageProtoSource::from(&update)
                .encode_proto()
                .encode_to_bytes(),
            owned_message.encode_proto().encode_to_bytes()
        );
    }

    #[test]
    fn pending_group_messages_round_trip_through_runtime_envelope() {
        let migration_id = MigrationId {
            old_group_id: GroupId(Uuid::from_u128(91_001)),
            new_group_id: GroupId(Uuid::from_u128(91_002)),
        };
        let members = vec![
            MemberIdentity::from_array(["runtime-message", "alice"]),
            MemberIdentity::from_array(["runtime-message", "bob"]),
        ];
        let group_schema = docs_group_schema();
        let group_setup = test_group_setup(&members);
        let invitation = GroupInvitation::new_migration(
            migration_id,
            members.clone(),
            group_schema.clone(),
            InitialSnapshot::Empty,
            Some("docs".to_owned()),
            Some("join migration".to_owned()),
        );
        let invitation_message =
            GroupInvitationMessage::try_new(invitation, Arc::clone(&group_setup))
                .expect("invitation members should match setup");
        assert_eq!(
            RuntimeMessage::GroupInvitation(invitation_message.clone()).group_id(),
            migration_id.new_group_id
        );
        let invitation_payload = RuntimeMessage::GroupInvitation(invitation_message.clone())
            .encode_proto()
            .encode_to_bytes();
        let decoded_invitation =
            WireRuntimeMessage::decode_proto_view_from_slice(&invitation_payload)
                .expect("invitation should decode");
        assert_eq!(
            decoded_invitation,
            WireRuntimeMessage::GroupInvitation(invitation_message)
        );

        let proposal = MigrationProposal {
            migration_id,
            final_versions: VersionVector::Full(PureVersionVector::from([3, 4])),
            proposed_members: members,
            group_schema,
            initial_snapshot: InitialSnapshot::Empty,
            group_name: Some("docs".to_owned()),
            message: Some("migrate".to_owned()),
        };
        let proposal_message = MigrationProposalMessage::try_new(proposal, group_setup)
            .expect("proposal members should match setup");
        assert_eq!(
            RuntimeMessage::MigrationProposal(proposal_message.clone()).group_id(),
            migration_id.old_group_id
        );
        let proposal_payload = RuntimeMessage::MigrationProposal(proposal_message.clone())
            .encode_proto()
            .encode_to_bytes();
        let decoded_proposal = WireRuntimeMessage::decode_proto_view_from_slice(&proposal_payload)
            .expect("proposal should decode");
        assert_eq!(
            decoded_proposal,
            WireRuntimeMessage::MigrationProposal(proposal_message)
        );
    }

    #[test]
    fn update_range_omits_end_version_for_singletons() {
        let singleton = UpdateRangeMessage {
            producer_index: 1,
            start_version: 7,
            end_version: 7,
        };
        let singleton_proto = singleton.encode_proto();
        assert_eq!(singleton_proto.end_version, None);
        assert_eq!(
            UpdateRangeMessage::decode_proto(singleton_proto).expect("singleton should decode"),
            singleton
        );

        let range = UpdateRangeMessage {
            producer_index: 1,
            start_version: 7,
            end_version: 9,
        };
        let range_proto = range.encode_proto();
        assert_eq!(range_proto.end_version, Some(9));
        assert_eq!(
            UpdateRangeMessage::decode_proto(range_proto).expect("range should decode"),
            range
        );
    }

    #[test]
    fn update_range_rejects_reserved_max_bound() {
        let group_id = GroupId(Uuid::from_u128(303));
        let payload = RuntimeMessage::NeedRange(NeedRangeMessage {
            group_id,
            ranges: vec![UpdateRangeMessage {
                producer_index: 1,
                start_version: u64::MAX - 1,
                end_version: u64::MAX,
            }],
        })
        .encode_proto()
        .encode_to_bytes();

        let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
            .expect_err("reserved max range bound should be rejected");
        assert!(matches!(
            error,
            RuntimeMessageError::NeedRangeBoundTooLarge {
                producer_index: 1,
                version: u64::MAX,
            }
        ));
    }

    #[test]
    fn update_rejects_reserved_update_id_version() {
        let group_id = GroupId(Uuid::from_u128(304));
        let update_id = UpdateId {
            version: u64::MAX,
            node_index: 0,
        };
        let payload = RuntimeMessage::Update(Box::new(test_update_message(
            group_id,
            update_id,
            VersionVector::initial(NonZeroUsize::new(2).expect("two members")),
        )))
        .encode_proto()
        .encode_to_bytes();

        let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
            .expect_err("reserved update id version should be rejected");
        assert!(matches!(
            error,
            RuntimeMessageError::UpdateVersionBoundTooLarge {
                update_id: actual_update_id,
                version: u64::MAX,
            } if actual_update_id == update_id
        ));
    }

    #[test]
    fn update_rejects_reserved_read_version_bound() {
        let group_id = GroupId(Uuid::from_u128(305));
        let payload = RuntimeMessage::Update(Box::new(test_update_message(
            group_id,
            UpdateId {
                version: 1,
                node_index: 0,
            },
            VersionVector::Full(PureVersionVector::from([u64::MAX, 0])),
        )))
        .encode_proto()
        .encode_to_bytes();

        let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
            .expect_err("reserved read version should be rejected");
        assert!(matches!(
            error,
            RuntimeMessageError::InvalidReadVersions {
                field: "update.read_versions",
                source: WireVersionVectorError::VersionBoundTooLarge {
                    field: "full.entries",
                    version: u64::MAX,
                },
            }
        ));
    }

    #[test]
    fn summary_rejects_reserved_version_bound() {
        let group_id = GroupId(Uuid::from_u128(306));
        let correlation_id = Uuid::from_u128(307);
        let payload = RuntimeMessage::Summary(SummaryMessage::new(
            group_id,
            correlation_id,
            VersionVector::Synced {
                num_members: NonZeroUsize::new(2).expect("two members"),
                version: u64::MAX,
            },
        ))
        .encode_proto()
        .encode_to_bytes();

        let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
            .expect_err("reserved summary version should be rejected");
        assert!(matches!(
            error,
            RuntimeMessageError::InvalidReadVersions {
                field: "summary.has_versions",
                source: WireVersionVectorError::VersionBoundTooLarge {
                    field: "synced.group_version",
                    version: u64::MAX,
                },
            }
        ));
    }

    #[test]
    fn update_batch_rejects_mismatched_inner_group() {
        let batch_group_id = GroupId(Uuid::from_u128(401));
        let update_group_id = GroupId(Uuid::from_u128(402));
        let update_id = UpdateId {
            version: 1,
            node_index: 0,
        };
        let payload = RuntimeMessage::UpdateBatch(UpdateBatchMessage {
            group_id: batch_group_id,
            updates: vec![test_update_message(
                update_group_id,
                update_id,
                VersionVector::initial(NonZeroUsize::new(2).expect("two members")),
            )],
        })
        .encode_proto()
        .encode_to_bytes();

        let error = WireRuntimeMessage::decode_proto_view_from_slice(&payload)
            .expect_err("mismatched batch group should be rejected");
        assert!(matches!(
            error,
            RuntimeMessageError::UpdateBatchGroupMismatch {
                batch_group: actual_batch_group,
                update_group: actual_update_group,
                update: actual_update,
            } if actual_batch_group == batch_group_id
                && actual_update_group == update_group_id
                && actual_update == update_id
        ));
    }
}

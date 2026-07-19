//! Runtime message envelope codecs.

use super::*;

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

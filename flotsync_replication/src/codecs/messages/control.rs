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

impl DecodeProtoWith<RuntimeMessageDecodeContext<'_>> for RuntimeMessage {
    type Error = RuntimeMessageError;
    type Proto = replication_proto::RuntimeMessage;

    fn decode_proto_with(
        message: Self::Proto,
        context: RuntimeMessageDecodeContext<'_>,
    ) -> Result<Self, Self::Error> {
        let Some(body) = message.body else {
            return MissingBodySnafu.fail();
        };
        match body {
            replication_proto::runtime_message::Body::Update(message) => {
                let member_count =
                    member_count_context(&message.group_id, "update.group_id", context)?;
                let message = UpdateMessage::decode_proto_with(*message, member_count)?;
                Ok(Self::Update(Box::new(message)))
            }
            replication_proto::runtime_message::Body::SummaryRequest(message) => {
                let message = SummaryRequestMessage::decode_proto(*message)?;
                Ok(Self::SummaryRequest(message))
            }
            replication_proto::runtime_message::Body::Summary(message) => {
                let member_count =
                    member_count_context(&message.group_id, "summary.group_id", context)?;
                let message = SummaryMessage::decode_proto_with(*message, member_count)?;
                Ok(Self::Summary(message))
            }
            replication_proto::runtime_message::Body::NeedRange(message) => {
                let message = NeedRangeMessage::decode_proto(*message)?;
                Ok(Self::NeedRange(message))
            }
            replication_proto::runtime_message::Body::UpdateBatch(message) => {
                let member_count =
                    member_count_context(&message.group_id, "update_batch.group_id", context)?;
                let message = UpdateBatchMessage::decode_proto_with(*message, member_count)?;
                Ok(Self::UpdateBatch(message))
            }
            replication_proto::runtime_message::Body::GroupInvitation(message) => {
                let message = GroupInvitationMessage::decode_proto(*message)?;
                Ok(Self::GroupInvitation(message))
            }
            replication_proto::runtime_message::Body::MigrationProposal(message) => {
                let message = MigrationProposalMessage::decode_proto(*message)?;
                Ok(Self::MigrationProposal(message))
            }
        }
    }
}

impl DecodeProtoViewWith<RuntimeMessageDecodeContext<'_>> for RuntimeMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::RuntimeMessageView<'a>;

    fn decode_proto_view_with(
        message: &Self::ProtoView<'_>,
        context: RuntimeMessageDecodeContext<'_>,
    ) -> Result<Self, Self::Error> {
        let Some(body) = message.body.as_ref() else {
            return MissingBodySnafu.fail();
        };
        match body {
            replication_proto::runtime_message::BodyView::Update(message) => {
                let member_count =
                    member_count_context(message.group_id, "update.group_id", context)?;
                let message = UpdateMessage::decode_proto_view_with(message, member_count)?;
                Ok(Self::Update(Box::new(message)))
            }
            replication_proto::runtime_message::BodyView::SummaryRequest(message) => {
                let message = SummaryRequestMessage::decode_proto_view(message)?;
                Ok(Self::SummaryRequest(message))
            }
            replication_proto::runtime_message::BodyView::Summary(message) => {
                let member_count =
                    member_count_context(message.group_id, "summary.group_id", context)?;
                let message = SummaryMessage::decode_proto_view_with(message, member_count)?;
                Ok(Self::Summary(message))
            }
            replication_proto::runtime_message::BodyView::NeedRange(message) => {
                let message = NeedRangeMessage::decode_proto_view(message)?;
                Ok(Self::NeedRange(message))
            }
            replication_proto::runtime_message::BodyView::UpdateBatch(message) => {
                let member_count =
                    member_count_context(message.group_id, "update_batch.group_id", context)?;
                let message = UpdateBatchMessage::decode_proto_view_with(message, member_count)?;
                Ok(Self::UpdateBatch(message))
            }
            replication_proto::runtime_message::BodyView::GroupInvitation(message) => {
                // TODO(flotsync-git-kmx): add borrowed contextual decoders for
                // pending-group payloads so view decoding does not first own
                // the generated payload.
                let message =
                    flotsync_messages::buffa::MessageView::to_owned_message(message.as_ref())
                        .context(DecodeSnafu)?;
                let message = GroupInvitationMessage::decode_proto(message)?;
                Ok(Self::GroupInvitation(message))
            }
            replication_proto::runtime_message::BodyView::MigrationProposal(message) => {
                // TODO(flotsync-git-kmx): add borrowed contextual decoders for
                // pending-group payloads so view decoding does not first own
                // the generated payload.
                let message =
                    flotsync_messages::buffa::MessageView::to_owned_message(message.as_ref())
                        .context(DecodeSnafu)?;
                let message = MigrationProposalMessage::decode_proto(message)?;
                Ok(Self::MigrationProposal(message))
            }
        }
    }
}

/// Resolve the compact-vector member count from one message group id.
fn member_count_context(
    group_id: &[u8],
    field: &'static str,
    context: RuntimeMessageDecodeContext<'_>,
) -> Result<MemberCountContext, RuntimeMessageError> {
    let group_id = group_id_from_wire(group_id, field).context(InvalidWireValueSnafu { field })?;
    context.member_count_for(group_id)
}

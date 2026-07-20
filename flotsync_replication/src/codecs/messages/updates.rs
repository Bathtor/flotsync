//! Update, summary, range, and dataset codecs.

use super::*;

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

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
        mut proto: <Self as EncodeProto>::Proto,
        context: MemberCountContext,
    ) -> Result<Self, Self::DecodeError> {
        let group_id = group_id_from_wire(&proto.group_id, "update.group_id").context(
            InvalidWireValueSnafu {
                field: "update.group_id",
            },
        )?;
        let Some(update_id) = proto.update_id.take() else {
            return MissingUpdateIdSnafu.fail();
        };
        let update_id = decode_update_id(update_id).context(InvalidUpdateIdSnafu {
            field: "update.update_id",
        })?;
        ensure_update_id_version_bound(update_id)?;
        let Some(read_versions) = proto.read_versions.take() else {
            return MissingReadVersionsSnafu.fail();
        };
        let read_versions =
            CompactVersionVectorProtoCodec::decode_proto_with(read_versions, context).context(
                InvalidReadVersionsSnafu {
                    field: "update.read_versions",
                },
            )?;
        let read_versions = read_versions.into_version_vector();
        if proto.dataset_updates.is_empty() {
            return EmptyUpdateSnafu.fail();
        }
        let dataset_updates = proto
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

impl DecodeProtoViewWith<MemberCountContext> for UpdateMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::UpdateView<'a>;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        context: MemberCountContext,
    ) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(proto.group_id, "update.group_id").context(
            InvalidWireValueSnafu {
                field: "update.group_id",
            },
        )?;
        let Some(update_id) = proto.update_id.as_option() else {
            return MissingUpdateIdSnafu.fail();
        };
        let update_id = UpdateId {
            version: update_id.version,
            node_index: update_id.node_index,
        };
        ensure_update_id_version_bound(update_id)?;
        let Some(read_versions) = proto.read_versions.as_option() else {
            return MissingReadVersionsSnafu.fail();
        };
        let read_versions =
            CompactVersionVectorProtoCodec::decode_proto_view_with(read_versions, context)
                .context(InvalidReadVersionsSnafu {
                    field: "update.read_versions",
                })?;
        let read_versions = read_versions.into_version_vector();
        if proto.dataset_updates.is_empty() {
            return EmptyUpdateSnafu.fail();
        }
        let dataset_updates = proto
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

impl EncodeProto for SummaryMessage {
    type Proto = replication_proto::Summary;

    fn encode_proto(&self) -> Self::Proto {
        self.view().encode_proto()
    }
}

impl proto::ProtoCodecWith<MemberCountContext> for SummaryMessage {
    type DecodeError = RuntimeMessageError;

    fn from_proto_with(
        mut proto: <Self as EncodeProto>::Proto,
        context: MemberCountContext,
    ) -> Result<Self, Self::DecodeError> {
        let group_id = group_id_from_wire(&proto.group_id, "summary.group_id").context(
            InvalidWireValueSnafu {
                field: "summary.group_id",
            },
        )?;
        let correlation_id =
            correlation_id_from_wire(&proto.correlation_id, "summary.correlation_id")?;
        let Some(has_versions) = proto.has_versions.take() else {
            return MissingSummaryVersionsSnafu.fail();
        };
        let has_versions = CompactVersionVectorProtoCodec::decode_proto_with(has_versions, context)
            .context(InvalidReadVersionsSnafu {
                field: "summary.has_versions",
            })?;
        let has_versions = has_versions.into_version_vector();
        Ok(Self::new(group_id, correlation_id, has_versions))
    }
}

impl DecodeProtoViewWith<MemberCountContext> for SummaryMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::SummaryView<'a>;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        context: MemberCountContext,
    ) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(proto.group_id, "summary.group_id").context(
            InvalidWireValueSnafu {
                field: "summary.group_id",
            },
        )?;
        let correlation_id =
            correlation_id_from_wire(proto.correlation_id, "summary.correlation_id")?;
        let Some(has_versions) = proto.has_versions.as_option() else {
            return MissingSummaryVersionsSnafu.fail();
        };
        let has_versions =
            CompactVersionVectorProtoCodec::decode_proto_view_with(has_versions, context).context(
                InvalidReadVersionsSnafu {
                    field: "summary.has_versions",
                },
            )?;
        let has_versions = has_versions.into_version_vector();
        Ok(Self::new(group_id, correlation_id, has_versions))
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
        let group_id = group_id_from_wire(&proto.group_id, "update_batch.group_id").context(
            InvalidWireValueSnafu {
                field: "update_batch.group_id",
            },
        )?;
        if proto.updates.is_empty() {
            return EmptyUpdateBatchSnafu.fail();
        }
        let updates = proto
            .updates
            .into_iter()
            .map(|update| UpdateMessage::decode_proto_with(update, context))
            .collect::<Result<Vec<_>, _>>()?;
        validate_update_batch_groups(group_id, &updates)?;
        Ok(Self { group_id, updates })
    }
}

impl DecodeProtoViewWith<MemberCountContext> for UpdateBatchMessage {
    type Error = RuntimeMessageError;
    type ProtoView<'a> = replication_proto::UpdateBatchView<'a>;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        context: MemberCountContext,
    ) -> Result<Self, Self::Error> {
        let group_id = group_id_from_wire(proto.group_id, "update_batch.group_id").context(
            InvalidWireValueSnafu {
                field: "update_batch.group_id",
            },
        )?;
        if proto.updates.is_empty() {
            return EmptyUpdateBatchSnafu.fail();
        }
        let updates = proto
            .updates
            .iter()
            .map(|update| UpdateMessage::decode_proto_view_with(update, context))
            .collect::<Result<Vec<_>, _>>()?;
        validate_update_batch_groups(group_id, &updates)?;
        Ok(Self { group_id, updates })
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

/// Ensure every update belongs to its containing batch group.
fn validate_update_batch_groups(
    group_id: GroupId,
    updates: &[UpdateMessage],
) -> Result<(), RuntimeMessageError> {
    for update in updates {
        ensure!(
            update.group_id == group_id,
            UpdateBatchGroupMismatchSnafu {
                batch_group: group_id,
                update_group: update.group_id,
                update: update.update_id,
            }
        );
    }
    Ok(())
}

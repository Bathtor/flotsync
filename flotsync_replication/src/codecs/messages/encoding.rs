//! Borrowed protobuf encoding adapters.

use super::*;

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
        let read_versions = CompactVersionVectorProtoCodec::from(self.read_versions).encode_proto();
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
        let read_versions = CompactVersionVectorProtoCodec::from(self.read_versions).encode_proto();
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
                CompactVersionVectorProtoCodec::from(self.has_versions).encode_proto(),
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
            updates: UpdateMessage::encode_proto_collection(self.updates.iter()),
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
            Self::Messages(dataset_updates) => {
                DatasetUpdateMessage::encode_proto_collection(dataset_updates.iter())
            }
            Self::Records(dataset_updates) => dataset_updates
                .iter()
                .map(|record| DatasetUpdateMessageView::from(record).encode_proto())
                .collect(),
        }
    }
}

pub(crate) fn ensure_update_id_version_bound(
    update_id: UpdateId,
) -> Result<(), RuntimeMessageError> {
    ensure!(
        update_id.version <= MAX_VERSION_VALUE,
        UpdateVersionBoundTooLargeSnafu {
            update_id,
            version: update_id.version,
        }
    );
    Ok(())
}

pub(crate) fn fixed_bytes_field<const N: usize>(
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
pub(crate) fn validate_bootstrap_member_key_coverage(
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

pub(crate) fn ensure_version_vector_bound(
    field: &'static str,
    version: u64,
) -> Result<(), VersionVectorCodecError> {
    ensure!(
        version <= MAX_VERSION_VALUE,
        VersionBoundTooLargeSnafu { field, version }
    );
    Ok(())
}

pub(crate) fn correlation_id_from_wire(
    bytes: &[u8],
    field: &'static str,
) -> Result<Uuid, RuntimeMessageError> {
    Uuid::from_slice(bytes).context(InvalidCorrelationIdSnafu { field })
}

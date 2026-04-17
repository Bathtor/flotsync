use crate::{
    api::{DatasetId, GroupId, MemberIdentity},
    delivery::wire::{
        WireValueDecodeError,
        group_id_from_wire,
        member_identity_from_wire,
        member_identity_to_wire_format,
    },
};
use flotsync_core::versions::{OverrideVersion, PureVersionVector, UpdateId, VersionVector};
use flotsync_messages::{
    buffa::{Message as _, MessageField},
    codecs::datamodel::{CodecError as DatamodelCodecError, decode_update_id, encode_update_id},
    datamodel as datamodel_proto,
    replication as replication_proto,
    versions as versions_proto,
};
use snafu::prelude::*;
use std::num::NonZeroUsize;

#[derive(Debug, Snafu)]
pub(crate) enum RuntimeMessageError {
    #[snafu(display("Failed to decode runtime message payload."))]
    Decode {
        source: flotsync_messages::buffa::DecodeError,
    },
    #[snafu(display("Runtime message did not contain a body."))]
    MissingBody,
    #[snafu(display("Bootstrap group message must include at least one member."))]
    EmptyBootstrapGroup,
    #[snafu(display("Update batch message must include at least one dataset update."))]
    EmptyUpdateBatch,
    #[snafu(display(
        "Update batch dataset entry for '{dataset_id}' must include at least one operation."
    ))]
    EmptyDatasetUpdate { dataset_id: String },
    #[snafu(display("Update batch message did not include an update id."))]
    MissingUpdateId,
    #[snafu(display("Update batch message did not include read versions."))]
    MissingReadVersions,
    #[snafu(display("Bootstrap group message field '{field}' was invalid: {source}"))]
    InvalidWireValue {
        field: &'static str,
        source: WireValueDecodeError,
    },
    #[snafu(display("Update batch field '{field}' was invalid: {source}"))]
    InvalidUpdateId {
        field: &'static str,
        source: DatamodelCodecError,
    },
    #[snafu(display("Update batch field '{field}' was invalid: {source}"))]
    InvalidReadVersions {
        field: &'static str,
        source: WireVersionVectorError,
    },
    #[snafu(display("Update batch dataset id '{value}' was invalid: {source}"))]
    InvalidDatasetId {
        value: String,
        source: crate::api::DatasetIdError,
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RuntimeMessage {
    BootstrapGroup(BootstrapGroupMessage),
    UpdateBatch(UpdateBatchMessage),
}

/// One decoded wire message before all runtime context is available.
///
/// Bootstrap messages can decode directly into their runtime form, but inbound
/// update batches may still carry compact read-version encodings that need the
/// hosted group member count before they can become a full `VersionVector`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum WireRuntimeMessage {
    /// Bootstrap messages already have their full runtime shape once decoded.
    BootstrapGroup(BootstrapGroupMessage),
    UpdateBatch(WireUpdateBatchMessage),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BootstrapGroupMessage {
    pub(crate) group_id: GroupId,
    pub(crate) members: Vec<MemberIdentity>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct UpdateBatchMessage {
    pub(crate) group_id: GroupId,
    pub(crate) update_id: UpdateId,
    pub(crate) read_versions: VersionVector,
    pub(crate) dataset_updates: Vec<DatasetUpdateMessage>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WireUpdateBatchMessage {
    pub(crate) group_id: GroupId,
    pub(crate) update_id: UpdateId,
    /// Compact wire representation kept until the hosted group member count is known.
    read_versions: WireVersionVector,
    pub(crate) dataset_updates: Vec<DatasetUpdateMessage>,
}

/// Compact wire-only form for version vectors used inside inbound update batches.
///
/// The runtime message model keeps a full `VersionVector`, while the protobuf
/// wire shape prefers more compact encodings such as "all members synced" or
/// "one member ahead" when possible. This stays separate from the generated
/// protobuf type so the runtime can validate and normalise wire values before
/// they become a domain `VersionVector`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum WireVersionVector {
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DatasetUpdateMessage {
    pub(crate) dataset_id: DatasetId,
    pub(crate) operations: Vec<datamodel_proto::SchemaOperation>,
}

impl DatasetUpdateMessage {
    fn to_proto(&self) -> replication_proto::DatasetUpdate {
        replication_proto::DatasetUpdate {
            dataset_id: self.dataset_id.as_str().to_owned(),
            operations: self.operations.clone(),
            ..replication_proto::DatasetUpdate::default()
        }
    }

    fn decode_proto_vec(
        dataset_updates: Vec<replication_proto::DatasetUpdate>,
    ) -> Result<Vec<Self>, RuntimeMessageError> {
        if dataset_updates.is_empty() {
            return EmptyUpdateBatchSnafu.fail();
        }

        let mut decoded_updates = Vec::with_capacity(dataset_updates.len());
        for dataset_update in dataset_updates {
            if dataset_update.operations.is_empty() {
                return EmptyDatasetUpdateSnafu {
                    dataset_id: dataset_update.dataset_id,
                }
                .fail();
            }

            let dataset_id = DatasetId::try_new(dataset_update.dataset_id.clone()).context(
                InvalidDatasetIdSnafu {
                    value: dataset_update.dataset_id,
                },
            )?;
            decoded_updates.push(Self {
                dataset_id,
                operations: dataset_update.operations,
            });
        }
        Ok(decoded_updates)
    }
}

impl RuntimeMessage {
    pub(crate) fn encode_to_proto(&self) -> replication_proto::RuntimeMessage {
        match self {
            RuntimeMessage::BootstrapGroup(message) => replication_proto::RuntimeMessage {
                body: Some(replication_proto::runtime_message::Body::BootstrapGroup(
                    Box::new(message.to_proto()),
                )),
                ..replication_proto::RuntimeMessage::default()
            },
            RuntimeMessage::UpdateBatch(message) => replication_proto::RuntimeMessage {
                body: Some(replication_proto::runtime_message::Body::UpdateBatch(
                    Box::new(message.to_proto()),
                )),
                ..replication_proto::RuntimeMessage::default()
            },
        }
    }
}

impl BootstrapGroupMessage {
    fn to_proto(&self) -> replication_proto::BootstrapGroup {
        let members = self
            .members
            .iter()
            .map(member_identity_to_wire_format)
            .collect();
        replication_proto::BootstrapGroup {
            group_id: self.group_id.0.as_bytes().to_vec(),
            members,
            ..replication_proto::BootstrapGroup::default()
        }
    }
}

impl UpdateBatchMessage {
    fn to_proto(&self) -> replication_proto::UpdateBatch {
        let read_versions = WireVersionVector::from_runtime(&self.read_versions).to_proto();
        let dataset_updates = self
            .dataset_updates
            .iter()
            .map(DatasetUpdateMessage::to_proto)
            .collect();
        replication_proto::UpdateBatch {
            group_id: self.group_id.0.as_bytes().to_vec(),
            update_id: MessageField::some(encode_update_id(self.update_id)),
            read_versions: MessageField::some(read_versions),
            dataset_updates,
            ..replication_proto::UpdateBatch::default()
        }
    }
}

impl WireRuntimeMessage {
    pub(crate) fn decode_from_slice(payload: &[u8]) -> Result<Self, RuntimeMessageError> {
        let message =
            replication_proto::RuntimeMessage::decode_from_slice(payload).context(DecodeSnafu)?;
        Self::from_proto(message)
    }

    fn from_proto(message: replication_proto::RuntimeMessage) -> Result<Self, RuntimeMessageError> {
        let Some(body) = message.body else {
            return MissingBodySnafu.fail();
        };
        match body {
            replication_proto::runtime_message::Body::BootstrapGroup(message) => {
                let group_id = group_id_from_wire(&message.group_id, "bootstrap_group.group_id")
                    .context(InvalidWireValueSnafu {
                        field: "bootstrap_group.group_id",
                    })?;
                if message.members.is_empty() {
                    return EmptyBootstrapGroupSnafu.fail();
                }
                let mut members = Vec::with_capacity(message.members.len());
                for member in message.members {
                    let member = member_identity_from_wire(member, "bootstrap_group.members")
                        .context(InvalidWireValueSnafu {
                            field: "bootstrap_group.members",
                        })?;
                    members.push(member);
                }
                Ok(WireRuntimeMessage::BootstrapGroup(BootstrapGroupMessage {
                    group_id,
                    members,
                }))
            }
            replication_proto::runtime_message::Body::UpdateBatch(message) => Ok(
                WireRuntimeMessage::UpdateBatch(WireUpdateBatchMessage::from_proto(*message)?),
            ),
        }
    }
}

impl WireUpdateBatchMessage {
    fn from_proto(
        mut message: replication_proto::UpdateBatch,
    ) -> Result<Self, RuntimeMessageError> {
        let group_id = group_id_from_wire(&message.group_id, "update_batch.group_id").context(
            InvalidWireValueSnafu {
                field: "update_batch.group_id",
            },
        )?;
        let Some(update_id) = message.update_id.take() else {
            return MissingUpdateIdSnafu.fail();
        };
        let update_id = decode_update_id(update_id).context(InvalidUpdateIdSnafu {
            field: "update_batch.update_id",
        })?;
        let Some(read_versions) = message.read_versions.take() else {
            return MissingReadVersionsSnafu.fail();
        };
        let read_versions =
            WireVersionVector::from_proto(read_versions).context(InvalidReadVersionsSnafu {
                field: "update_batch.read_versions",
            })?;
        let dataset_updates = DatasetUpdateMessage::decode_proto_vec(message.dataset_updates)?;
        Ok(Self {
            group_id,
            update_id,
            read_versions,
            dataset_updates,
        })
    }

    pub(crate) fn into_runtime(
        self,
        num_members: NonZeroUsize,
    ) -> Result<UpdateBatchMessage, RuntimeMessageError> {
        let read_versions =
            self.read_versions
                .to_runtime(num_members)
                .context(InvalidReadVersionsSnafu {
                    field: "update_batch.read_versions",
                })?;
        Ok(UpdateBatchMessage {
            group_id: self.group_id,
            update_id: self.update_id,
            read_versions,
            dataset_updates: self.dataset_updates,
        })
    }
}

impl From<UpdateBatchMessage> for WireUpdateBatchMessage {
    fn from(message: UpdateBatchMessage) -> Self {
        Self {
            group_id: message.group_id,
            update_id: message.update_id,
            read_versions: WireVersionVector::from_runtime(&message.read_versions),
            dataset_updates: message.dataset_updates,
        }
    }
}

impl WireVersionVector {
    fn from_runtime(version_vector: &VersionVector) -> Self {
        match version_vector {
            VersionVector::Full(vector) => Self::Full(vector.clone()),
            VersionVector::Override { version, .. } => Self::Override {
                group_version: version.group_version(),
                override_position: version.override_position as u32,
                override_version: version.override_version(),
            },
            VersionVector::Synced { version, .. } => Self::Synced {
                group_version: *version,
            },
        }
    }

    fn to_proto(&self) -> versions_proto::VersionVector {
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

    fn from_proto(
        version_vector: versions_proto::VersionVector,
    ) -> Result<Self, WireVersionVectorError> {
        let Some(versions) = version_vector.versions else {
            return MissingVersionsBodySnafu.fail();
        };
        match versions {
            versions_proto::version_vector::Versions::Full(full) => {
                if full.entries.is_empty() {
                    return EmptyFullVectorSnafu.fail();
                }
                Ok(WireVersionVector::Full(PureVersionVector::from(
                    full.entries,
                )))
            }
            versions_proto::version_vector::Versions::Override(override_vector) => {
                Ok(WireVersionVector::Override {
                    group_version: override_vector.group_version,
                    override_position: override_vector.override_position,
                    override_version: override_vector.override_version,
                })
            }
            versions_proto::version_vector::Versions::Synced(synced) => {
                Ok(WireVersionVector::Synced {
                    group_version: synced.group_version,
                })
            }
        }
    }

    fn to_runtime(
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

#[cfg(test)]
mod tests {
    use super::WireVersionVector;
    use flotsync_core::versions::{OverrideVersion, PureVersionVector, VersionVector};
    use std::num::NonZeroUsize;

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
            let proto = wire.to_proto();
            let decoded_wire =
                WireVersionVector::from_proto(proto).expect("wire decode should work");
            let decoded_vector = decoded_wire
                .to_runtime(vector.num_members())
                .expect("runtime decode should work");
            assert_eq!(decoded_vector, vector);
        }
    }
}

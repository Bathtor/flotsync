use crate::{
    api::{DatasetId, GroupId, MemberIdentity},
    delivery::wire::{
        group_id_from_wire,
        member_identity_from_wire,
        member_identity_to_wire_format,
    },
};
use flotsync_core::versions::{PureVersionVector, UpdateId, VersionVector};
use flotsync_messages::{
    buffa::{Message, MessageField},
    codecs::datamodel::{CodecError as DatamodelCodecError, decode_update_id, encode_update_id},
    datamodel as datamodel_proto,
    replication as replication_proto,
    versions as versions_proto,
};
use snafu::prelude::*;

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
    #[snafu(display("Update batch message did not include a read version vector."))]
    MissingReadVersionVector,
    #[snafu(display("Bootstrap group message field '{field}' was invalid: {source}"))]
    InvalidWireValue {
        field: &'static str,
        source: crate::delivery::wire::WireValueDecodeError,
    },
    #[snafu(display("Update batch field '{field}' was invalid: {source}"))]
    InvalidUpdateId {
        field: &'static str,
        source: DatamodelCodecError,
    },
    #[snafu(display("Update batch field '{field}' was invalid: {reason}"))]
    InvalidReadVersionVector { field: &'static str, reason: String },
    #[snafu(display("Update batch dataset id '{value}' was invalid: {source}"))]
    InvalidDatasetId {
        value: String,
        source: crate::api::DatasetIdError,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RuntimeMessage {
    BootstrapGroup(BootstrapGroupMessage),
    UpdateBatch(UpdateBatchMessage),
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
    pub(crate) read_vv: VersionVector,
    pub(crate) dataset_updates: Vec<DatasetUpdateMessage>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DatasetUpdateMessage {
    pub(crate) dataset_id: DatasetId,
    pub(crate) operations: Vec<datamodel_proto::SchemaOperation>,
}

impl RuntimeMessage {
    pub(crate) fn encode_to_bytes(&self) -> bytes::Bytes {
        match self {
            RuntimeMessage::BootstrapGroup(message) => replication_proto::RuntimeMessage {
                body: Some(replication_proto::runtime_message::Body::BootstrapGroup(
                    Box::new(replication_proto::BootstrapGroup {
                        group_id: message.group_id.0.as_bytes().to_vec(),
                        members: message
                            .members
                            .iter()
                            .map(member_identity_to_wire_format)
                            .collect(),
                        ..replication_proto::BootstrapGroup::default()
                    }),
                )),
                ..replication_proto::RuntimeMessage::default()
            }
            .encode_to_bytes(),
            RuntimeMessage::UpdateBatch(message) => replication_proto::RuntimeMessage {
                body: Some(replication_proto::runtime_message::Body::UpdateBatch(
                    Box::new(replication_proto::UpdateBatch {
                        group_id: message.group_id.0.as_bytes().to_vec(),
                        update_id: MessageField::some(encode_update_id(message.update_id)),
                        read_vv: MessageField::some(encode_version_vector(&message.read_vv)),
                        dataset_updates: message
                            .dataset_updates
                            .iter()
                            .map(|dataset| replication_proto::DatasetUpdate {
                                dataset_id: dataset.dataset_id.as_str().to_owned(),
                                operations: dataset.operations.clone(),
                                ..replication_proto::DatasetUpdate::default()
                            })
                            .collect(),
                        ..replication_proto::UpdateBatch::default()
                    }),
                )),
                ..replication_proto::RuntimeMessage::default()
            }
            .encode_to_bytes(),
        }
    }

    pub(crate) fn decode_from_slice(payload: &[u8]) -> Result<Self, RuntimeMessageError> {
        let message =
            replication_proto::RuntimeMessage::decode_from_slice(payload).context(DecodeSnafu)?;
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
                Ok(RuntimeMessage::BootstrapGroup(BootstrapGroupMessage {
                    group_id,
                    members,
                }))
            }
            replication_proto::runtime_message::Body::UpdateBatch(mut message) => {
                let group_id = group_id_from_wire(&message.group_id, "update_batch.group_id")
                    .context(InvalidWireValueSnafu {
                        field: "update_batch.group_id",
                    })?;
                let Some(update_id) = message.update_id.take() else {
                    return MissingUpdateIdSnafu.fail();
                };
                let update_id = decode_update_id(update_id).context(InvalidUpdateIdSnafu {
                    field: "update_batch.update_id",
                })?;
                let Some(read_vv) = message.read_vv.take() else {
                    return MissingReadVersionVectorSnafu.fail();
                };
                let read_vv = decode_version_vector(read_vv).map_err(|reason| {
                    RuntimeMessageError::InvalidReadVersionVector {
                        field: "update_batch.read_vv",
                        reason,
                    }
                })?;
                if message.dataset_updates.is_empty() {
                    return EmptyUpdateBatchSnafu.fail();
                }
                let mut dataset_updates = Vec::with_capacity(message.dataset_updates.len());
                for dataset_update in message.dataset_updates {
                    if dataset_update.operations.is_empty() {
                        return EmptyDatasetUpdateSnafu {
                            dataset_id: dataset_update.dataset_id,
                        }
                        .fail();
                    }
                    let dataset_id = DatasetId::try_new(dataset_update.dataset_id.clone())
                        .context(InvalidDatasetIdSnafu {
                            value: dataset_update.dataset_id,
                        })?;
                    dataset_updates.push(DatasetUpdateMessage {
                        dataset_id,
                        operations: dataset_update.operations,
                    });
                }
                Ok(RuntimeMessage::UpdateBatch(UpdateBatchMessage {
                    group_id,
                    update_id,
                    read_vv,
                    dataset_updates,
                }))
            }
        }
    }
}

fn encode_version_vector(version_vector: &VersionVector) -> versions_proto::VersionVector {
    // This first replication slice keeps the wire shape intentionally simple and
    // always serialises the full VV. That avoids having to reconstruct the
    // member count from compact representations on decode.
    versions_proto::VersionVector {
        versions: Some(versions_proto::version_vector::Versions::Full(Box::new(
            versions_proto::FullVersionVector {
                entries: version_vector.iter().collect(),
                ..versions_proto::FullVersionVector::default()
            },
        ))),
        ..versions_proto::VersionVector::default()
    }
}

fn decode_version_vector(
    version_vector: versions_proto::VersionVector,
) -> Result<VersionVector, String> {
    let Some(versions) = version_vector.versions else {
        return Err("missing version-vector body".to_owned());
    };
    match versions {
        versions_proto::version_vector::Versions::Full(full) => {
            if full.entries.is_empty() {
                return Err("full version vector must include at least one entry".to_owned());
            }
            Ok(VersionVector::Full(PureVersionVector::from(full.entries)))
        }
        versions_proto::version_vector::Versions::Override(_) => Err(
            "compact override version vectors are not supported on the runtime wire yet".to_owned(),
        ),
        versions_proto::version_vector::Versions::Synced(_) => Err(
            "compact synced version vectors are not supported on the runtime wire yet".to_owned(),
        ),
    }
}

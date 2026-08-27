//! Canonical protobuf conversion for opaque application read tokens.

use crate::codecs::messages::{VersionVectorCodecError, VersionVectorProtoCodec};
use flotsync_core::{GroupId, SortedArrayMap, versions::VersionVector};
use flotsync_messages::{
    buffa::MessageField,
    proto::{DecodeProto, EncodeProto, FromProtoDecodeError},
    versions as versions_proto,
    wire as message_wire,
};
use snafu::prelude::*;
use std::borrow::Cow;

/// Borrowed encoder and owned decoder for the persisted read-token format.
pub(crate) struct ReadTokenProtoCodec<'a> {
    /// Borrowed for encoding and owned after decoding.
    groups: Cow<'a, SortedArrayMap<GroupId, VersionVector>>,
}

impl<'a> From<&'a SortedArrayMap<GroupId, VersionVector>> for ReadTokenProtoCodec<'a> {
    fn from(groups: &'a SortedArrayMap<GroupId, VersionVector>) -> Self {
        Self {
            groups: Cow::Borrowed(groups),
        }
    }
}

impl ReadTokenProtoCodec<'_> {
    /// Consume a decoded adapter and return its group-scoped versions.
    pub(crate) fn into_groups(self) -> SortedArrayMap<GroupId, VersionVector> {
        self.groups.into_owned()
    }
}

impl EncodeProto for ReadTokenProtoCodec<'_> {
    type Proto = versions_proto::ReadToken;

    fn encode_proto(&self) -> Self::Proto {
        let groups = self
            .groups
            .iter()
            .map(|(group_id, versions)| versions_proto::ReadTokenGroup {
                group_id: message_wire::group_id_to_wire_bytes(*group_id),
                versions: MessageField::some(
                    VersionVectorProtoCodec::from(versions).encode_proto(),
                ),
                ..versions_proto::ReadTokenGroup::default()
            })
            .collect();
        versions_proto::ReadToken {
            groups,
            ..versions_proto::ReadToken::default()
        }
    }
}

impl DecodeProto for ReadTokenProtoCodec<'static> {
    type Error = ReadTokenCodecError;
    type Proto = versions_proto::ReadToken;

    fn decode_proto(mut proto: Self::Proto) -> Result<Self, Self::Error> {
        let mut groups = Vec::with_capacity(proto.groups.len());
        for (entry_index, mut entry) in proto.groups.drain(..).enumerate() {
            let group_id = message_wire::group_id_from_wire_bytes(
                &entry.group_id,
                "read_token.groups.group_id",
            )
            .with_context(|_| InvalidGroupIdSnafu { entry_index })?;
            let versions = entry.versions.take().context(MissingVersionVectorSnafu {
                entry_index,
                group_id,
            })?;
            let versions = VersionVectorProtoCodec::decode_proto(versions)
                .with_context(|_| InvalidVersionVectorSnafu {
                    entry_index,
                    group_id,
                })?
                .into_version_vector();
            groups.push((group_id, versions));
        }

        let groups = SortedArrayMap::try_from_entries(groups).map_err(|error| {
            DuplicateGroupSnafu {
                group_id: error.into_key(),
            }
            .build()
        })?;
        Ok(Self {
            groups: Cow::Owned(groups),
        })
    }
}

/// Structural failure while decoding persisted read-token bytes.
#[derive(Debug, Snafu)]
pub(crate) enum ReadTokenCodecError {
    /// The input was not a complete protobuf read-token message.
    #[snafu(display("Read-token protobuf was malformed: {source}"))]
    MalformedProtobuf {
        source: flotsync_messages::buffa::DecodeError,
    },
    /// One entry did not contain a canonical UUID byte sequence.
    #[snafu(display("Read-token group entry {entry_index} had an invalid group id: {source}"))]
    InvalidGroupId {
        entry_index: usize,
        source: flotsync_messages::wire::WireValueDecodeError,
    },
    /// One entry omitted its self-describing version vector.
    #[snafu(display(
        "Read-token group entry {entry_index} for group {group_id} omitted its version vector."
    ))]
    MissingVersionVector {
        entry_index: usize,
        group_id: GroupId,
    },
    /// One entry contained a structurally invalid version vector.
    #[snafu(display(
        "Read-token group entry {entry_index} for group {group_id} had an invalid version vector: {source}"
    ))]
    InvalidVersionVector {
        entry_index: usize,
        group_id: GroupId,
        source: VersionVectorCodecError,
    },
    /// Two entries referred to the same group.
    #[snafu(display("Read token contained group {group_id} more than once."))]
    DuplicateGroup { group_id: GroupId },
}

impl FromProtoDecodeError for ReadTokenCodecError {
    fn from_proto_decode_error(source: flotsync_messages::buffa::DecodeError) -> Self {
        Self::MalformedProtobuf { source }
    }
}

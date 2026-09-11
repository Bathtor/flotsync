//! Canonical protobuf conversion for opaque group and application read tokens.

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

/// Borrowed encoder and owned decoder for an application read token.
pub(crate) struct ApplicationReadTokenProtoCodec<'a> {
    /// Borrowed for encoding and owned after decoding.
    groups: Cow<'a, SortedArrayMap<GroupId, VersionVector>>,
}

impl<'a> From<&'a SortedArrayMap<GroupId, VersionVector>> for ApplicationReadTokenProtoCodec<'a> {
    fn from(groups: &'a SortedArrayMap<GroupId, VersionVector>) -> Self {
        Self {
            groups: Cow::Borrowed(groups),
        }
    }
}

impl ApplicationReadTokenProtoCodec<'_> {
    /// Consume a decoded adapter and return its group-scoped versions.
    pub(crate) fn into_groups(self) -> SortedArrayMap<GroupId, VersionVector> {
        self.groups.into_owned()
    }
}

impl EncodeProto for ApplicationReadTokenProtoCodec<'_> {
    type Proto = versions_proto::ReadToken;

    fn encode_proto(&self) -> Self::Proto {
        let groups = self
            .groups
            .iter()
            .map(|(group_id, versions)| encode_group_read_token(*group_id, versions))
            .collect();
        versions_proto::ReadToken {
            groups,
            ..versions_proto::ReadToken::default()
        }
    }
}

impl DecodeProto for ApplicationReadTokenProtoCodec<'static> {
    type Error = ReadTokenCodecError;
    type Proto = versions_proto::ReadToken;

    fn decode_proto(mut proto: Self::Proto) -> Result<Self, Self::Error> {
        let mut groups = Vec::with_capacity(proto.groups.len());
        for (entry_index, entry) in proto.groups.drain(..).enumerate() {
            groups.push(decode_group_read_token(entry, entry_index)?);
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

/// Borrowed encoder and owned decoder for one group read token.
pub(crate) struct GroupReadTokenProtoCodec<'a> {
    /// Group whose application position is represented.
    group_id: GroupId,
    /// Borrowed for encoding and owned after decoding.
    version: Cow<'a, VersionVector>,
}

impl<'a> GroupReadTokenProtoCodec<'a> {
    /// Build an encoder over one borrowed group position.
    pub(crate) fn new(group_id: GroupId, version: &'a VersionVector) -> Self {
        Self {
            group_id,
            version: Cow::Borrowed(version),
        }
    }

    /// Consume a decoded adapter and return its group position.
    pub(crate) fn into_group(self) -> (GroupId, VersionVector) {
        (self.group_id, self.version.into_owned())
    }
}

impl EncodeProto for GroupReadTokenProtoCodec<'_> {
    type Proto = versions_proto::ReadTokenGroup;

    fn encode_proto(&self) -> Self::Proto {
        encode_group_read_token(self.group_id, self.version.as_ref())
    }
}

impl DecodeProto for GroupReadTokenProtoCodec<'static> {
    type Error = ReadTokenCodecError;
    type Proto = versions_proto::ReadTokenGroup;

    fn decode_proto(proto: Self::Proto) -> Result<Self, Self::Error> {
        let (group_id, version) = decode_group_read_token(proto, 0)?;
        Ok(Self {
            group_id,
            version: Cow::Owned(version),
        })
    }
}

/// Encode one group entry for either token representation.
fn encode_group_read_token(
    group_id: GroupId,
    version: &VersionVector,
) -> versions_proto::ReadTokenGroup {
    versions_proto::ReadTokenGroup {
        group_id: message_wire::group_id_to_wire_bytes(group_id),
        versions: MessageField::some(VersionVectorProtoCodec::from(version).encode_proto()),
        ..versions_proto::ReadTokenGroup::default()
    }
}

/// Decode one group entry shared by both token representations.
fn decode_group_read_token(
    mut entry: versions_proto::ReadTokenGroup,
    entry_index: usize,
) -> Result<(GroupId, VersionVector), ReadTokenCodecError> {
    let group_id = message_wire::group_id_from_wire_bytes(&entry.group_id, "read_token.group_id")
        .with_context(|_| InvalidGroupIdSnafu { entry_index })?;
    let version = entry.versions.take().context(MissingVersionVectorSnafu {
        entry_index,
        group_id,
    })?;
    let version = VersionVectorProtoCodec::decode_proto(version)
        .with_context(|_| InvalidVersionVectorSnafu {
            entry_index,
            group_id,
        })?
        .into_version_vector();
    Ok((group_id, version))
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

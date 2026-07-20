//! Shared compact and self-describing version-vector protobuf codecs.

use super::*;

/// Trait-backed adapter for the context-dependent compact protobuf form.
///
/// Borrowed adapters encode without cloning the domain vector. Decoding always
/// creates an owned adapter and immediately exposes the domain value through
/// [`Self::into_version_vector`].
pub(crate) struct CompactVersionVectorProtoCodec<'a>(Cow<'a, VersionVector>);

impl<'a> From<&'a VersionVector> for CompactVersionVectorProtoCodec<'a> {
    fn from(version_vector: &'a VersionVector) -> Self {
        Self(Cow::Borrowed(version_vector))
    }
}

impl CompactVersionVectorProtoCodec<'_> {
    /// Consume a decoded adapter and return its domain vector.
    pub(crate) fn into_version_vector(self) -> VersionVector {
        self.0.into_owned()
    }
}

impl EncodeProto for CompactVersionVectorProtoCodec<'_> {
    type Proto = versions_proto::CompactVersionVector;

    fn encode_proto(&self) -> Self::Proto {
        encode_compact_version_vector(self.0.as_ref())
    }
}

impl proto::ProtoCodecWith<MemberCountContext> for CompactVersionVectorProtoCodec<'static> {
    type DecodeError = VersionVectorCodecError;

    fn from_proto_with(
        mut proto: Self::Proto,
        context: MemberCountContext,
    ) -> Result<Self, Self::DecodeError> {
        let Some(versions) = proto.versions.take() else {
            return MissingVersionsBodySnafu.fail();
        };
        let version_vector = decode_version_vector_body(versions, context.member_count())?;
        Ok(Self(Cow::Owned(version_vector)))
    }
}

impl DecodeProtoViewWith<MemberCountContext> for CompactVersionVectorProtoCodec<'static> {
    type Error = VersionVectorCodecError;
    type ProtoView<'a> = versions_proto::CompactVersionVectorView<'a>;

    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        context: MemberCountContext,
    ) -> Result<Self, Self::Error> {
        let Some(versions) = proto.versions.as_ref() else {
            return MissingVersionsBodySnafu.fail();
        };
        let version_vector = decode_version_vector_view_body(versions, context.member_count())?;
        Ok(Self(Cow::Owned(version_vector)))
    }
}

/// Trait-backed adapter for the self-describing protobuf form.
pub(crate) struct VersionVectorProtoCodec<'a>(Cow<'a, VersionVector>);

impl<'a> From<&'a VersionVector> for VersionVectorProtoCodec<'a> {
    fn from(version_vector: &'a VersionVector) -> Self {
        Self(Cow::Borrowed(version_vector))
    }
}

impl VersionVectorProtoCodec<'_> {
    /// Consume a decoded adapter and return its domain vector.
    pub(crate) fn into_version_vector(self) -> VersionVector {
        self.0.into_owned()
    }
}

impl EncodeProto for VersionVectorProtoCodec<'_> {
    type Proto = versions_proto::VersionVector;

    fn encode_proto(&self) -> Self::Proto {
        versions_proto::VersionVector {
            num_members: u32::try_from(self.0.num_members().get())
                .expect("version-vector member count must fit into u32"),
            compact: MessageField::some(
                CompactVersionVectorProtoCodec::from(self.0.as_ref()).encode_proto(),
            ),
            ..versions_proto::VersionVector::default()
        }
    }
}

impl DecodeProto for VersionVectorProtoCodec<'static> {
    type Error = VersionVectorCodecError;
    type Proto = versions_proto::VersionVector;

    fn decode_proto(mut proto: Self::Proto) -> Result<Self, Self::Error> {
        let num_members = usize::try_from(proto.num_members)
            .expect("u32 version-vector member count must fit into usize");
        let Some(num_members) = NonZeroUsize::new(num_members) else {
            return InvalidMemberCountSnafu.fail();
        };
        let Some(compact) = proto.compact.take() else {
            return MissingVersionsBodySnafu.fail();
        };
        let compact = CompactVersionVectorProtoCodec::decode_proto_with(
            compact,
            MemberCountContext::new(num_members),
        )?;
        Ok(Self(Cow::Owned(compact.into_version_vector())))
    }
}

impl DecodeProtoView for VersionVectorProtoCodec<'static> {
    type Error = VersionVectorCodecError;
    type ProtoView<'a> = versions_proto::VersionVectorView<'a>;

    fn decode_proto_view(proto: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
        let num_members = usize::try_from(proto.num_members)
            .expect("u32 version-vector member count must fit into usize");
        let Some(num_members) = NonZeroUsize::new(num_members) else {
            return InvalidMemberCountSnafu.fail();
        };
        let Some(compact) = proto.compact.as_option() else {
            return MissingVersionsBodySnafu.fail();
        };
        let compact = CompactVersionVectorProtoCodec::decode_proto_view_with(
            compact,
            MemberCountContext::new(num_members),
        )?;
        Ok(Self(Cow::Owned(compact.into_version_vector())))
    }
}

/// Encode the shared compact body used by both protobuf forms.
fn encode_compact_version_vector(
    version_vector: &VersionVector,
) -> versions_proto::CompactVersionVector {
    let versions = match version_vector {
        VersionVector::Full(vector) => versions_proto::compact_version_vector::Versions::Full(
            Box::new(versions_proto::FullVersionVector {
                entries: vector.0.to_vec(),
                ..versions_proto::FullVersionVector::default()
            }),
        ),
        VersionVector::Override { version, .. } => {
            versions_proto::compact_version_vector::Versions::Override(Box::new(
                versions_proto::OverrideVersionVector {
                    group_version: version.group_version(),
                    override_position: u32::try_from(version.override_position)
                        .expect("version-vector override position must fit into u32"),
                    override_version: version.override_version(),
                    ..versions_proto::OverrideVersionVector::default()
                },
            ))
        }
        VersionVector::Synced { version, .. } => {
            versions_proto::compact_version_vector::Versions::Synced(Box::new(
                versions_proto::SyncedVersionVector {
                    group_version: *version,
                    ..versions_proto::SyncedVersionVector::default()
                },
            ))
        }
    };
    versions_proto::CompactVersionVector {
        versions: Some(versions),
        ..versions_proto::CompactVersionVector::default()
    }
}

/// Decode one owned compact representation body.
fn decode_version_vector_body(
    versions: versions_proto::compact_version_vector::Versions,
    num_members: NonZeroUsize,
) -> Result<VersionVector, VersionVectorCodecError> {
    match versions {
        versions_proto::compact_version_vector::Versions::Full(full) => {
            decode_full_version_vector(full.entries, num_members)
        }
        versions_proto::compact_version_vector::Versions::Override(override_vector) => {
            decode_override_version_vector(
                override_vector.group_version,
                override_vector.override_position,
                override_vector.override_version,
                num_members,
            )
        }
        versions_proto::compact_version_vector::Versions::Synced(synced) => {
            decode_synced_version_vector(synced.group_version, num_members)
        }
    }
}

/// Decode one borrowed compact representation body.
fn decode_version_vector_view_body(
    versions: &versions_proto::compact_version_vector::VersionsView<'_>,
    num_members: NonZeroUsize,
) -> Result<VersionVector, VersionVectorCodecError> {
    match versions {
        versions_proto::compact_version_vector::VersionsView::Full(full) => {
            decode_full_version_vector(full.entries.to_vec(), num_members)
        }
        versions_proto::compact_version_vector::VersionsView::Override(override_vector) => {
            decode_override_version_vector(
                override_vector.group_version,
                override_vector.override_position,
                override_vector.override_version,
                num_members,
            )
        }
        versions_proto::compact_version_vector::VersionsView::Synced(synced) => {
            decode_synced_version_vector(synced.group_version, num_members)
        }
    }
}

/// Decode and validate the explicit full-vector representation.
fn decode_full_version_vector(
    entries: Vec<u64>,
    num_members: NonZeroUsize,
) -> Result<VersionVector, VersionVectorCodecError> {
    if entries.is_empty() {
        return EmptyFullVectorSnafu.fail();
    }
    for version in entries.iter().copied() {
        ensure_version_vector_bound("full.entries", version)?;
    }
    ensure!(
        entries.len() == num_members.get(),
        MemberCountMismatchSnafu {
            expected_members: num_members.get(),
            actual_members: entries.len(),
        }
    );
    Ok(VersionVector::Full(PureVersionVector::from(entries)))
}

/// Decode and validate the single-member override representation.
fn decode_override_version_vector(
    group_version: u64,
    override_position: u32,
    override_version: u64,
    num_members: NonZeroUsize,
) -> Result<VersionVector, VersionVectorCodecError> {
    ensure_version_vector_bound("override.group_version", group_version)?;
    ensure_version_vector_bound("override.override_version", override_version)?;
    let override_position_index =
        usize::try_from(override_position).expect("u32 override position must fit into usize");
    ensure!(
        override_position_index < num_members.get(),
        InvalidOverridePositionSnafu {
            num_members: num_members.get(),
            override_position,
        }
    );
    let version =
        OverrideVersion::new_opt(group_version, override_position_index, override_version)
            .context(InvalidOverrideSnafu {
                group_version,
                override_position,
                override_version,
            })?;
    Ok(VersionVector::Override {
        num_members,
        version,
    })
}

/// Decode and validate the fully synchronised representation.
fn decode_synced_version_vector(
    group_version: u64,
    num_members: NonZeroUsize,
) -> Result<VersionVector, VersionVectorCodecError> {
    ensure_version_vector_bound("synced.group_version", group_version)?;
    Ok(VersionVector::Synced {
        num_members,
        version: group_version,
    })
}

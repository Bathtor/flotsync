use crate::discovery;
use flotsync_core::{
    GroupId,
    MemberIdentity,
    member::{IdentifierBuf, IdentifierError, IdentifierLike, MAX_IDENTIFIER_SEGMENTS},
};
use snafu::prelude::*;
use uuid::Uuid;

/// Number of bytes in the canonical protobuf UUID representation.
pub const UUID_BYTE_LENGTH: usize = 16;

/// Decode failures for shared protobuf value helpers.
#[derive(Debug, Snafu)]
pub enum WireValueDecodeError {
    /// A protobuf byte field did not contain a valid UUID.
    #[snafu(transparent)]
    InvalidUuid { source: InvalidUuidWireValue },
    /// A protobuf byte field had the wrong protocol width.
    #[snafu(transparent)]
    InvalidByteLength { source: InvalidByteLengthWireValue },
    /// A protobuf identifier segment did not satisfy local identifier rules.
    #[snafu(transparent)]
    InvalidIdentifierSegment {
        source: InvalidIdentifierSegmentWireValue,
    },
    /// A protobuf identifier had too many segments for local protocol invariants.
    #[snafu(transparent)]
    InvalidIdentifierSegmentCount {
        source: InvalidIdentifierSegmentCountWireValue,
    },
}

/// Shared body for protobuf UUID decode failures.
#[derive(Debug, Snafu)]
#[snafu(display("Field '{field}' did not contain a valid UUID: {source}"))]
pub struct InvalidUuidWireValue {
    field: &'static str,
    source: uuid::Error,
}

/// Shared body for fixed-width protobuf byte field failures.
#[derive(Debug, Snafu)]
#[snafu(display("Field '{field}' had invalid byte length {actual}; expected {expected}."))]
pub struct InvalidByteLengthWireValue {
    field: &'static str,
    expected: usize,
    actual: usize,
}

/// Shared body for protobuf identifier segment validation failures.
#[derive(Debug, Snafu)]
#[snafu(display("Field '{field}' contains an invalid identifier segment '{segment}': {source}"))]
pub struct InvalidIdentifierSegmentWireValue {
    field: &'static str,
    segment: String,
    source: IdentifierError,
}

/// Shared body for protobuf identifier segment-count validation failures.
#[derive(Debug, Snafu)]
#[snafu(display(
    "Field '{field}' contains {actual} identifier segments; at most {maximum} are allowed."
))]
pub struct InvalidIdentifierSegmentCountWireValue {
    field: &'static str,
    maximum: usize,
    actual: usize,
}

/// Encode one UUID into the canonical protobuf byte representation.
#[must_use]
pub fn uuid_to_wire_bytes(uuid: Uuid) -> Vec<u8> {
    uuid.as_bytes().to_vec()
}

/// Decode one canonical protobuf UUID byte field.
///
/// # Errors
///
/// Returns [`WireValueDecodeError`] when the field is not exactly a valid UUID byte sequence.
pub fn uuid_from_wire_bytes(raw: &[u8], field: &'static str) -> Result<Uuid, WireValueDecodeError> {
    let uuid = Uuid::from_slice(raw).context(InvalidUuidWireValueSnafu { field })?;
    Ok(uuid)
}

/// Encode one group id into the canonical protobuf byte representation.
#[must_use]
pub fn group_id_to_wire_bytes(group_id: GroupId) -> Vec<u8> {
    uuid_to_wire_bytes(group_id.0)
}

/// Decode one group id from the canonical protobuf byte representation.
///
/// # Errors
///
/// Returns [`WireValueDecodeError`] when the field is not exactly a valid UUID byte sequence.
pub fn group_id_from_wire_bytes(
    raw: &[u8],
    field: &'static str,
) -> Result<GroupId, WireValueDecodeError> {
    uuid_from_wire_bytes(raw, field).map(GroupId)
}

/// Validate and copy a protobuf byte field into a fixed-width protocol array.
///
/// # Errors
///
/// Returns [`WireValueDecodeError`] when `bytes` does not have width `N`.
pub fn fixed_bytes_field<const N: usize>(
    field: &'static str,
    bytes: &[u8],
) -> Result<[u8; N], WireValueDecodeError> {
    bytes
        .try_into()
        .map_err(|_| InvalidByteLengthWireValue {
            field,
            expected: N,
            actual: bytes.len(),
        })
        .map_err(WireValueDecodeError::from)
}

/// Convert an internal member identity to the discovery protobuf shape.
#[must_use]
pub fn member_identity_to_wire_format(member: &MemberIdentity) -> discovery::Identifier {
    let segments = member
        .segments()
        .map(|segment| segment.as_ref().to_owned())
        .collect();
    discovery::Identifier {
        segments,
        ..discovery::Identifier::default()
    }
}

/// Decode one discovery protobuf identifier into the internal identity shape.
///
/// # Errors
///
/// Returns [`WireValueDecodeError`] when any identifier segment is invalid or
/// the identifier exceeds the segment-count limit.
pub fn member_identity_from_wire_format(
    identifier: discovery::Identifier,
    field: &'static str,
) -> Result<MemberIdentity, WireValueDecodeError> {
    member_identity_from_wire_segments(identifier.segments, field)
}

/// Decode one discovery protobuf identifier view into the internal identity shape.
///
/// # Errors
///
/// Returns [`WireValueDecodeError`] when any identifier segment is invalid or
/// the identifier exceeds the segment-count limit.
pub fn member_identity_from_wire_view(
    identifier: &discovery::IdentifierView<'_>,
    field: &'static str,
) -> Result<MemberIdentity, WireValueDecodeError> {
    ensure_identifier_segment_count(field, identifier.segments.len())?;
    let segments = identifier
        .segments
        .iter()
        .map(|segment| (*segment).to_owned());
    member_identity_from_wire_segments(segments, field)
}

fn member_identity_from_wire_segments(
    segments: impl IntoIterator<Item = String>,
    field: &'static str,
) -> Result<MemberIdentity, WireValueDecodeError> {
    let mut buffer = IdentifierBuf::new();
    let mut segment_count = 0usize;
    for segment in segments {
        segment_count += 1;
        ensure_identifier_segment_count(field, segment_count)?;
        buffer
            .push_checked(segment.clone())
            .context(InvalidIdentifierSegmentWireValueSnafu { field, segment })?;
    }
    Ok(buffer.into_identifier())
}

fn ensure_identifier_segment_count(
    field: &'static str,
    actual: usize,
) -> Result<(), WireValueDecodeError> {
    ensure!(
        actual <= MAX_IDENTIFIER_SEGMENTS,
        InvalidIdentifierSegmentCountWireValueSnafu {
            field,
            maximum: MAX_IDENTIFIER_SEGMENTS,
            actual,
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    const OVERLONG_IDENTIFIER_SEGMENTS: usize = MAX_IDENTIFIER_SEGMENTS + 1;

    fn proto_identifier(segment_count: usize) -> discovery::Identifier {
        let segments = (0..segment_count)
            .map(|index| format!("segment_{index}"))
            .collect();
        discovery::Identifier {
            segments,
            ..discovery::Identifier::default()
        }
    }

    #[test]
    fn member_identity_wire_format_accepts_maximum_segment_count() {
        let member = member_identity_from_wire_format(
            proto_identifier(MAX_IDENTIFIER_SEGMENTS),
            "member_id",
        )
        .unwrap();

        assert_eq!(member.len(), MAX_IDENTIFIER_SEGMENTS);
        let encoded = member_identity_to_wire_format(&member);
        assert_eq!(encoded.segments.len(), MAX_IDENTIFIER_SEGMENTS);
    }

    #[test]
    fn member_identity_wire_format_rejects_overlong_identifier() {
        let error = member_identity_from_wire_format(
            proto_identifier(OVERLONG_IDENTIFIER_SEGMENTS),
            "member_id",
        )
        .unwrap_err();

        assert_matches!(
            error,
            WireValueDecodeError::InvalidIdentifierSegmentCount {
                source: InvalidIdentifierSegmentCountWireValue {
                    maximum: MAX_IDENTIFIER_SEGMENTS,
                    actual: OVERLONG_IDENTIFIER_SEGMENTS,
                    ..
                }
            }
        );
    }

    #[test]
    fn member_identity_wire_view_rejects_overlong_identifier_before_cloning_segments() {
        use crate::buffa::Message as _;

        let identifier = proto_identifier(OVERLONG_IDENTIFIER_SEGMENTS);
        let view = discovery::IdentifierOwnedView::decode(identifier.encode_to_bytes()).unwrap();

        let error = member_identity_from_wire_view(view.view(), "member_id").unwrap_err();

        assert_matches!(
            error,
            WireValueDecodeError::InvalidIdentifierSegmentCount {
                source: InvalidIdentifierSegmentCountWireValue {
                    maximum: MAX_IDENTIFIER_SEGMENTS,
                    actual: OVERLONG_IDENTIFIER_SEGMENTS,
                    ..
                }
            }
        );
    }
}

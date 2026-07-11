use bytes::BufMut;
use flotsync_core::{MemberIdentity, member::IdentifierLike};
use sha2::Digest;

/// Append a length-prefixed byte slice to protocol input buffers.
///
/// The length prefix is always a fixed-width `u64` in big-endian byte order, so
/// protocol transcripts do not depend on the local platform's `usize` width.
///
/// # Panics
///
/// Panics if the platform permits slices longer than `u64::MAX` bytes.
pub(crate) fn append_len_prefixed<B>(output: &mut B, value: &[u8])
where
    B: BufMut,
{
    output.put_u64(len_u64(value.len()));
    output.put_slice(value);
}

/// Append a member identity as a segment count followed by length-prefixed segments.
///
/// The segment count uses the same fixed-width `u64` convention as other
/// protocol transcript lengths. It is not derived from protobuf's repeated-field
/// representation, which does not encode an explicit count-width invariant.
pub(crate) fn append_member_identity<B>(output: &mut B, member: &MemberIdentity)
where
    B: BufMut,
{
    output.put_u64(len_u64(member.len()));
    for segment in member.segments() {
        append_len_prefixed(output, segment.as_ref().as_bytes());
    }
}

/// Hash a length-prefixed byte slice into protocol digest state.
///
/// The length prefix is always a fixed-width `u64` in big-endian byte order, so
/// protocol transcripts do not depend on the local platform's `usize` width.
///
/// # Panics
///
/// Panics if the platform permits slices longer than `u64::MAX` bytes.
pub(crate) fn hash_len_prefixed<D>(hasher: &mut D, value: &[u8])
where
    D: Digest,
{
    hasher.update(len_u64(value.len()).to_be_bytes());
    hasher.update(value);
}

/// Convert a protocol length/count to `u64`.
///
/// # Panics
///
/// Panics if the platform permits lengths greater than `u64::MAX`.
pub(crate) fn len_u64(length: usize) -> u64 {
    u64::try_from(length).expect("protocol length must fit into u64")
}

/// Convert a slice with caller-guaranteed length into an array.
///
/// The helper is intentionally small and panics if the internal caller violates
/// the fixed-length precondition.
///
/// # Panics
///
/// Panics if `bytes.len() != N`.
pub(crate) fn fixed_array<const N: usize>(bytes: &[u8]) -> [u8; N] {
    bytes
        .try_into()
        .expect("source slice length is fixed by the caller")
}

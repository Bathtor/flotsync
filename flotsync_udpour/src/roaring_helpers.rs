use roaring::RoaringBitmap;
use snafu::prelude::*;
#[cfg(test)]
use std::io::Cursor;
use std::io::{Read, Write};

/// Serializes one roaring bitmap into its compact on-the-wire byte form.
#[cfg(test)]
pub(crate) fn serialize_bitmap(bitmap: &RoaringBitmap) -> Result<Vec<u8>, RoaringBitmapError> {
    let mut buffer = Vec::with_capacity(bitmap.serialized_size());
    serialize_bitmap_into(bitmap, &mut buffer)?;
    Ok(buffer)
}

/// Serializes one roaring bitmap directly into an existing writer.
pub(crate) fn serialize_bitmap_into<W: Write>(
    bitmap: &RoaringBitmap,
    writer: &mut W,
) -> Result<(), RoaringBitmapError> {
    bitmap.serialize_into(writer).context(SerializeSnafu)?;
    Ok(())
}

/// Deserializes one roaring bitmap from its compact on-the-wire byte form.
#[cfg(test)]
pub(crate) fn deserialize_bitmap(bytes: &[u8]) -> Result<RoaringBitmap, RoaringBitmapError> {
    deserialize_bitmap_from(Cursor::new(bytes))
}

/// Deserializes one roaring bitmap from any synchronous reader.
pub(crate) fn deserialize_bitmap_from<R: Read>(
    mut reader: R,
) -> Result<RoaringBitmap, RoaringBitmapError> {
    RoaringBitmap::deserialize_from(&mut reader).context(DeserializeSnafu)
}

/// Splits one bitmap into one chunk that fits within `max_serialized_size` and the remaining tail.
///
/// Contract:
///
/// - callers must pass a non-empty bitmap
/// - `max_serialized_size` refers only to the serialized bitmap body, not any outer protocol header
/// - the returned `chunk` preserves the original iteration order
/// - the optional `rest` contains every remaining set bit, again in original iteration order
///
/// This helper performs only one split step. Callers that need several chunks should keep feeding
/// the returned `rest` back into this function and can already process or send the first `chunk`
/// while that happens.
///
/// Ownership note:
///
/// - the function consumes the original bitmap
/// - when a split is needed, the returned `rest` keeps that original allocation and only the
///   fitted prefix `chunk` is rebuilt
///
/// Portability note:
///
/// - the splitting logic works in `u64` cardinalities and `u32` ranks, so it does not rely on
///   `usize` being able to represent the bitmap size
pub(crate) fn split_bitmap_to_serialized_bounds(
    bitmap: RoaringBitmap,
    max_serialized_size: usize,
) -> Result<SplittingResult, RoaringBitmapError> {
    ensure!(!bitmap.is_empty(), EmptyBitmapSnafu);
    ensure!(max_serialized_size > 0, ZeroMaxSerializedSizeSnafu);

    if bitmap.serialized_size() <= max_serialized_size {
        return Ok(SplittingResult {
            chunk: bitmap,
            rest: None,
        });
    }

    let total_cardinality = bitmap.len();
    let total_serialized_size = bitmap.serialized_size() as u64;
    let estimated = ((total_cardinality * max_serialized_size as u64) / total_serialized_size)
        .max(1)
        .min(total_cardinality);
    let fit =
        best_prefix_chunk_cardinality(&bitmap, total_cardinality, estimated, max_serialized_size)?;
    let chunk = prefix_chunk(&bitmap, fit)?;
    let rest = if fit == total_cardinality {
        None
    } else {
        Some(remove_prefix(bitmap, fit)?)
    };

    Ok(SplittingResult { chunk, rest })
}

/// Selects one largest-fitting chunk from `bitmap`, optionally restricted to values strictly
/// greater than `after_exclusive`.
///
/// This helper performs one chunk selection only. Callers that want to iterate over the whole set
/// should update their own cursor or watermark between calls.
///
/// Returns `Ok(None)` only when `bitmap` still has values overall but none remain strictly after
/// `after_exclusive`. It does not mean that a chunk failed to fit within the serialized budget.
pub(crate) fn select_bitmap_chunk(
    bitmap: &RoaringBitmap,
    after_exclusive: Option<u32>,
    max_serialized_size: usize,
) -> Result<Option<RoaringBitmap>, RoaringBitmapError> {
    ensure!(max_serialized_size > 0, ZeroMaxSerializedSizeSnafu);
    if bitmap.is_empty() {
        return Ok(None);
    }

    let Some(candidate) = suffix_after(bitmap, after_exclusive) else {
        return Ok(None);
    };
    let step = split_bitmap_to_serialized_bounds(candidate, max_serialized_size)?;
    Ok(Some(step.chunk))
}

/// One incremental split result for a roaring bitmap.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SplittingResult {
    pub(crate) chunk: RoaringBitmap,
    pub(crate) rest: Option<RoaringBitmap>,
}

/// Finds one large prefix chunk that still fits into `max_serialized_size`.
///
/// This uses the caller's rough estimate first, then shrinks and binary-searches from there.
fn best_prefix_chunk_cardinality(
    bitmap: &RoaringBitmap,
    total_cardinality: u64,
    estimated: u64,
    max_serialized_size: usize,
) -> Result<u64, RoaringBitmapError> {
    let mut low_fit;
    let mut high_fit = total_cardinality;
    let mut candidate = estimated.clamp(1, total_cardinality);
    let mut candidate_bitmap = prefix_chunk(bitmap, candidate)?;
    let mut candidate_size = candidate_bitmap.serialized_size();

    while candidate_size > max_serialized_size {
        high_fit = candidate.saturating_sub(1);
        ensure!(
            high_fit > 0,
            ChunkTooSmallSnafu {
                max_serialized_size
            }
        );
        candidate = (candidate / 2).max(1);
        candidate_bitmap = prefix_chunk(bitmap, candidate)?;
        candidate_size = candidate_bitmap.serialized_size();
    }
    low_fit = candidate;

    while low_fit < high_fit {
        let mid = (low_fit + high_fit).div_ceil(2);
        let mid_bitmap = prefix_chunk(bitmap, mid)?;
        if mid_bitmap.serialized_size() <= max_serialized_size {
            low_fit = mid;
        } else {
            high_fit = mid - 1;
        }
    }

    Ok(low_fit.max(1))
}

/// Builds one bitmap containing only the first `prefix_len` values from `bitmap`.
fn prefix_chunk(
    bitmap: &RoaringBitmap,
    prefix_len: u64,
) -> Result<RoaringBitmap, RoaringBitmapError> {
    ensure!(prefix_len > 0, ZeroChunkCountSnafu);
    let total_cardinality = bitmap.len();
    ensure!(
        prefix_len <= total_cardinality,
        PrefixLongerThanBitmapSnafu {
            prefix_len,
            cardinality: total_cardinality,
        }
    );

    if prefix_len == total_cardinality {
        return Ok(bitmap.clone());
    }

    let end_value = select_rank(bitmap, prefix_len - 1)?;
    let upper_bound = end_value
        .checked_add(1)
        .context(PrefixUpperBoundOverflowSnafu)?;
    let mut chunk = bitmap.clone();
    let removed = chunk.remove_range(upper_bound..);
    debug_assert_eq!(
        removed,
        total_cardinality - prefix_len,
        "prefix chunk should remove exactly the suffix cardinality"
    );
    Ok(chunk)
}

/// Removes the first `prefix_len` values from `bitmap` and returns the remainder.
fn remove_prefix(
    mut bitmap: RoaringBitmap,
    prefix_len: u64,
) -> Result<RoaringBitmap, RoaringBitmapError> {
    ensure!(prefix_len > 0, ZeroChunkCountSnafu);
    let total_cardinality = bitmap.len();
    ensure!(
        prefix_len <= total_cardinality,
        PrefixLongerThanBitmapSnafu {
            prefix_len,
            cardinality: total_cardinality,
        }
    );

    if prefix_len == total_cardinality {
        return Ok(RoaringBitmap::new());
    }

    let first_value = bitmap.min().context(EmptyBitmapSnafu)?;
    let end_value = select_rank(&bitmap, prefix_len - 1)?;
    let upper_bound = end_value
        .checked_add(1)
        .context(PrefixUpperBoundOverflowSnafu)?;
    let removed = bitmap.remove_range(first_value..upper_bound);
    debug_assert_eq!(
        removed, prefix_len,
        "rest bitmap should remove exactly the selected prefix cardinality"
    );
    Ok(bitmap)
}

/// Returns all bitmap values strictly greater than `after_exclusive`.
fn suffix_after(bitmap: &RoaringBitmap, after_exclusive: Option<u32>) -> Option<RoaringBitmap> {
    match after_exclusive {
        None => Some(bitmap.clone()),
        Some(value) => {
            let upper_bound = value.checked_add(1)?;
            let mut suffix = bitmap.clone();
            suffix.remove_range(0..upper_bound);
            if suffix.is_empty() { None } else { Some(suffix) }
        }
    }
}

/// Converts one logical rank into the `u32` API used by `roaring::select`.
fn select_rank(bitmap: &RoaringBitmap, rank: u64) -> Result<u32, RoaringBitmapError> {
    let rank = u32::try_from(rank).context(RankTooLargeSnafu { rank })?;
    bitmap
        .select(rank)
        .context(SelectOutOfRangeSnafu { rank: rank as u64 })
}

/// Roaring-bitmap helper errors.
#[derive(Debug, Snafu)]
pub enum RoaringBitmapError {
    #[snafu(display("roaring bitmap body must not be empty"))]
    EmptyBitmap,
    #[snafu(display("max serialized size must be greater than zero"))]
    ZeroMaxSerializedSize,
    #[snafu(display("chunk count must be greater than zero"))]
    ZeroChunkCount,
    #[snafu(display(
        "requested prefix length {prefix_len} exceeds bitmap cardinality {cardinality}"
    ))]
    PrefixLongerThanBitmap { prefix_len: u64, cardinality: u64 },
    #[snafu(display("rank {rank} does not fit into the u32 API used by roaring::select"))]
    RankTooLarge {
        rank: u64,
        source: std::num::TryFromIntError,
    },
    #[snafu(display("rank {rank} is outside the bitmap cardinality"))]
    SelectOutOfRange { rank: u64 },
    #[snafu(display("bitmap chunk cannot fit into max serialized size {max_serialized_size}"))]
    ChunkTooSmall { max_serialized_size: usize },
    #[snafu(display("cannot split a prefix that ends at u32::MAX but is not the whole bitmap"))]
    PrefixUpperBoundOverflow,
    #[snafu(display("failed to serialize roaring bitmap"))]
    Serialize { source: std::io::Error },
    #[snafu(display("failed to deserialize roaring bitmap"))]
    Deserialize { source: std::io::Error },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_bitmap_preserves_all_values() {
        let mut bitmap = RoaringBitmap::new();
        for value in 0..512u32 {
            bitmap.insert(value * 3);
        }

        let mut current = Some(bitmap.clone());
        let mut recombined = RoaringBitmap::new();
        let mut chunk_count = 0usize;
        while let Some(remaining) = current.take() {
            let step = split_bitmap_to_serialized_bounds(remaining, 128).unwrap();
            assert!(step.chunk.serialized_size() <= 128);
            recombined |= &step.chunk;
            current = step.rest;
            chunk_count += 1;
        }

        assert!(chunk_count > 1);
        assert_eq!(recombined, bitmap);
    }

    #[test]
    fn serialize_round_trip() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(33);
        bitmap.insert(4096);

        let encoded = serialize_bitmap(&bitmap).unwrap();
        let decoded = deserialize_bitmap(&encoded).unwrap();
        assert_eq!(decoded, bitmap);
    }
}

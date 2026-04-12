use crate::wire::{DecodeFromBuf, EncodeToBufMut};
use bytes::{Buf, BufMut};
use roaring::RoaringBitmap;
use snafu::prelude::*;

/// Smallest known encoded size of one non-empty roaring bitmap body.
///
/// `NeedParts` must always carry at least one missing-part number, so callers that
/// want to fit one serialized bitmap into a frame budget need at least this many
/// bytes available for the bitmap body itself.
pub(crate) const MIN_ENCODED_NON_EMPTY_BITMAP_LEN: usize = 16;

/// Roaring-bitmap helper errors.
#[derive(Debug, Snafu)]
pub enum RoaringBitmapError {
    #[snafu(display("roaring bitmap body must not be empty"))]
    EmptyBitmap,
    #[snafu(display(
        "max serialized size {max_serialized_size} is too small; need at least {min_serialized_size} bytes for one non-empty roaring bitmap"
    ))]
    MaxSerializedSizeTooSmall {
        max_serialized_size: usize,
        min_serialized_size: usize,
    },
    #[snafu(display("chunk count must be greater than zero"))]
    ZeroChunkCount,
    #[snafu(display(
        "requested prefix length {prefix_len} exceeds bitmap cardinality {cardinality}"
    ))]
    PrefixLongerThanBitmap { prefix_len: u64, cardinality: u64 },
    #[snafu(display("bitmap chunk cannot fit into max serialized size {max_serialized_size}"))]
    ChunkTooSmall { max_serialized_size: usize },
    #[snafu(display("failed to serialize roaring bitmap"))]
    Serialize { source: std::io::Error },
    #[snafu(display("failed to deserialize roaring bitmap"))]
    Deserialize { source: std::io::Error },
}

impl EncodeToBufMut for RoaringBitmap {
    type Error = RoaringBitmapError;

    fn encoded_len(&self) -> usize {
        self.serialized_size()
    }

    fn encode_into_buf<B>(&self, out: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        let mut writer = out.writer();
        self.serialize_into(&mut writer).context(SerializeSnafu)?;
        Ok(())
    }
}

impl DecodeFromBuf for RoaringBitmap {
    type Error = RoaringBitmapError;

    fn decode_from_buf<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        let mut reader = buf.reader();
        RoaringBitmap::deserialize_from(&mut reader).context(DeserializeSnafu)
    }
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
    ensure!(
        max_serialized_size >= MIN_ENCODED_NON_EMPTY_BITMAP_LEN,
        MaxSerializedSizeTooSmallSnafu {
            max_serialized_size,
            min_serialized_size: MIN_ENCODED_NON_EMPTY_BITMAP_LEN,
        }
    );
    if bitmap.is_empty() {
        return Ok(None);
    }

    let Some(suffix) = suffix_after(bitmap, after_exclusive) else {
        return Ok(None);
    };
    let step = split_bitmap_to_serialized_bounds(suffix, max_serialized_size)?;
    Ok(Some(step.chunk))
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
    ensure!(
        max_serialized_size >= MIN_ENCODED_NON_EMPTY_BITMAP_LEN,
        MaxSerializedSizeTooSmallSnafu {
            max_serialized_size,
            min_serialized_size: MIN_ENCODED_NON_EMPTY_BITMAP_LEN,
        }
    );

    if bitmap.serialized_size() <= max_serialized_size {
        return Ok(SplittingResult {
            chunk: bitmap,
            rest: None,
        });
    }

    let total_cardinality = bitmap.len();
    let total_serialized_size = bitmap.serialized_size() as u64;
    // This is unlikely to overflow in u64 math. RoaringBitmaps cannot be that large.
    let estimated_cutoff_in_rank_space =
        (total_cardinality * max_serialized_size as u64) / total_serialized_size;
    let fitting_prefix_len = best_prefix_chunk_cardinality(
        &bitmap,
        total_cardinality,
        estimated_cutoff_in_rank_space,
        max_serialized_size,
    )?;
    // This should never trigger, because we already checked above that we don't fully fit.
    debug_assert!(fitting_prefix_len < total_cardinality);
    let chunk = prefix_chunk(&bitmap, fitting_prefix_len)?;
    let rest = {
        let mut rest = bitmap;
        rest.remove_smallest(fitting_prefix_len);
        debug_assert_eq!(
            rest.len(),
            total_cardinality - fitting_prefix_len,
            "rest bitmap should keep exactly the suffix cardinality"
        );
        Some(rest)
    };

    Ok(SplittingResult { chunk, rest })
}

/// One incremental split result for a roaring bitmap.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SplittingResult {
    pub(crate) chunk: RoaringBitmap,
    pub(crate) rest: Option<RoaringBitmap>,
}

/// Returns the largest prefix cardinality whose serialized bitmap still fits within
/// `max_serialized_size`.
///
/// The search starts from the caller's rough estimate, shrinks until that prefix fits,
/// and then binary-searches upward to recover the largest fitting prefix length.
fn best_prefix_chunk_cardinality(
    bitmap: &RoaringBitmap,
    total_cardinality: u64,
    initial_candidate: u64,
    max_serialized_size: usize,
) -> Result<u64, RoaringBitmapError> {
    let mut low_fit;
    let mut high_fit = total_cardinality;
    let mut candidate = initial_candidate.clamp(1, total_cardinality);
    let mut candidate_bitmap = prefix_chunk(bitmap, candidate)?;

    while candidate_bitmap.serialized_size() > max_serialized_size {
        high_fit = candidate.saturating_sub(1);
        ensure!(
            high_fit > 0,
            ChunkTooSmallSnafu {
                max_serialized_size
            }
        );
        candidate = (candidate / 2).max(1);
        candidate_bitmap = prefix_chunk(bitmap, candidate)?;
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

    let suffix_len = total_cardinality - prefix_len;
    let mut chunk = bitmap.clone();
    chunk.remove_biggest(suffix_len);
    Ok(chunk)
}

/// Returns all bitmap values strictly greater than `after_exclusive`.
fn suffix_after(bitmap: &RoaringBitmap, after_exclusive: Option<u32>) -> Option<RoaringBitmap> {
    match after_exclusive {
        None => Some(bitmap.clone()),
        Some(value) => {
            let mut suffix = bitmap.clone();
            suffix.remove_range(..=value);
            if suffix.is_empty() {
                None
            } else {
                Some(suffix)
            }
        }
    }
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

        let encoded = bitmap.encode_to_bytes().unwrap();
        let decoded = RoaringBitmap::decode_from_slice(encoded.as_ref()).unwrap();
        assert_eq!(decoded, bitmap);
    }
}

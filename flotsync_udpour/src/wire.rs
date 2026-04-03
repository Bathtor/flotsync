#[cfg(test)]
use bytes::BytesMut;
use bytes::{Buf, BufMut};

/// Encodes one wire-level value directly into any synchronous `BufMut`.
///
/// This stays crate-private for now because it exists only to keep low-level wire
/// serialization logic attached to the values being serialized instead of spread
/// across ad hoc free functions.
///
/// The main API is [`EncodeToBufMut::encode_into_buf`], which writes into caller-owned
/// storage and therefore composes naturally with pooled payload writers.
///
/// [`EncodeToBufMut::encode_to_bytes`] is only a convenience helper for tests and other
/// call sites that explicitly want one owned contiguous byte buffer.
pub(crate) trait EncodeToBufMut {
    type Error;

    /// Returns the exact number of bytes this value will encode to on the wire.
    fn encoded_len(&self) -> usize;

    /// Encodes this value directly into one caller-provided `BufMut`.
    fn encode_into_buf<B>(&self, out: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut;

    #[cfg(test)]
    /// Encodes this value into one newly allocated contiguous byte buffer.
    fn encode_to_bytes(&self) -> Result<BytesMut, Self::Error> {
        let mut buffer = BytesMut::with_capacity(self.encoded_len());
        self.encode_into_buf(&mut buffer)?;
        Ok(buffer)
    }
}

/// Decodes one wire-level value directly from any synchronous `Buf`.
///
/// The main API is [`DecodeFromBuf::decode_from_buf`], which consumes bytes from an
/// existing buffer cursor. [`DecodeFromBuf::decode_from_slice`] is a test-friendly
/// convenience for callers that already have one complete contiguous byte slice.
pub(crate) trait DecodeFromBuf: Sized {
    type Error;

    /// Decodes one value from the current position of `buf`.
    fn decode_from_buf<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf;

    #[cfg(test)]
    /// Decodes one value from one complete contiguous byte slice.
    fn decode_from_slice(bytes: &[u8]) -> Result<Self, Self::Error> {
        let mut bytes = bytes;
        Self::decode_from_buf(&mut bytes)
    }
}

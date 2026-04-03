use bytes::{Buf, BufMut};

/// Encodes one wire-level value directly into any synchronous `BufMut`.
///
/// This stays crate-private for now because it exists only to keep low-level wire
/// serialization logic attached to the values being serialized instead of spread
/// across ad hoc free functions.
pub(crate) trait EncodeToBufMut {
    type Error;

    fn encode_into<B>(&self, out: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut;
}

/// Decodes one wire-level value directly from any synchronous `Buf`.
pub(crate) trait DecodeFromBuf: Sized {
    type Error;

    fn decode_from<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf;
}

use crate::{
    errors::{Errors, ErrorsExt},
    uuid_encodings::{UuidEncoding, UuidEncodingError, UuidEncodingExt},
};
use flotsync_utils::IString;
use itertools::Itertools;
use regex::Regex;
use snafu::prelude::*;
use std::{fmt, sync::LazyLock};
use uuid::Uuid;

pub type IdentifierSegment = IString;

const SEGMENT_SEPARATOR: &str = ".";

#[derive(Debug, Snafu)]
pub enum IdentifierError {
    #[snafu(display("The segment '{segment}' contained an illegal character."))]
    IllegalCharactersError { segment: String },
}

#[derive(Debug, Snafu)]
pub enum IdentifierUuidDecodeError {
    #[snafu(display("Could not infer UUID encoding from segment '{segment}'."))]
    UnknownEncodingShapeError { segment: String },
    #[snafu(display(
        "Found mixed UUID segment encodings. First segment implies {expected:?}, but segment {index} '{segment}' looks like {found:?}."
    ))]
    MixedEncodingError {
        expected: UuidEncoding,
        found: UuidEncoding,
        index: usize,
        segment: String,
    },
    #[snafu(display(
        "Failed to decode UUID segment {index} '{segment}' as {encoding:?}: {source}"
    ))]
    DecodeSegmentError {
        index: usize,
        segment: String,
        encoding: UuidEncoding,
        source: UuidEncodingError,
    },
    #[snafu(display("The identifier text '{input}' contains an empty segment."))]
    EmptySegmentError { input: String },
}

/// Identifier are immutable by default.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identifier {
    segments: Box<[IdentifierSegment]>,
}

impl Identifier {
    pub fn segments(self) -> impl Iterator<Item = IdentifierSegment> {
        self.segments.into_iter()
    }

    pub fn segments_iter(&self) -> impl Iterator<Item = &IdentifierSegment> {
        self.segments.iter()
    }

    pub(crate) fn from_segments_unchecked(segments: Box<[IdentifierSegment]>) -> Self {
        Self { segments }
    }

    /// An identifier with the given `segments` as components, if they are all legal segments.
    ///
    /// Otherwise the errors indicate all the segments with illegal characters.
    pub fn try_from_array<I, const N: usize>(
        segments: [I; N],
    ) -> Result<Self, Errors<IdentifierError>>
    where
        I: Into<IdentifierSegment>,
    {
        let id_segments = Box::new(segments.map(|i| i.into()));
        id_segments
            .iter()
            .ensure_for_all(check_that_contains_only_legal_chars)?;
        Ok(Self {
            segments: id_segments,
        })
    }

    /// An identifier with the given `segments` as components.
    ///
    /// Panics if it encounters illegal characters in any segment.
    pub fn from_array<I, const N: usize>(segments: [I; N]) -> Self
    where
        I: Into<IdentifierSegment>,
    {
        let id_segments = segments.map(|i| {
            let s = i.into();
            assert!(
                contains_only_legal_chars(s.as_ref()),
                "'{s}' contains illegal characters for an identifier segment"
            );
            s
        });
        Self {
            segments: Box::new(id_segments),
        }
    }

    /// An identifier with the given `segments` as components.
    ///
    /// Uses the string representation of the [[Uuid]], which is always legal, so this never panics.
    pub fn from_uuid_array<const N: usize>(segments: [Uuid; N]) -> Self {
        Self::from_uuid_array_with_encoding(segments, UuidEncoding::Hyphenated)
    }

    /// An identifier with the given `segments` as components encoded using `encoding`.
    pub fn from_uuid_array_with_encoding<const N: usize>(
        segments: [Uuid; N],
        encoding: UuidEncoding,
    ) -> Self {
        let id_segments = segments.map(|u| encode_uuid_segment(u, encoding));
        Self {
            segments: Box::new(id_segments),
        }
    }

    /// Decode all identifier segments as UUIDs, inferring a single shared encoding from the first
    /// segment's shape.
    pub fn decode_uuid_segments(&self) -> Result<Box<[Uuid]>, IdentifierUuidDecodeError> {
        decode_uuid_segments(self.segments_iter().map(|segment| segment.as_ref()))
    }

    /// Decode all identifier segments as UUIDs using the provided encoding.
    pub fn decode_uuid_segments_with_encoding(
        &self,
        encoding: UuidEncoding,
    ) -> Result<Box<[Uuid]>, IdentifierUuidDecodeError> {
        decode_uuid_segments_with_encoding(
            self.segments_iter().map(|segment| segment.as_ref()),
            encoding,
        )
    }

    /// Parse a textual identifier and decode all segments as UUIDs, inferring one shared encoding.
    pub fn parse_uuid_segments(input: &str) -> Result<Box<[Uuid]>, IdentifierUuidDecodeError> {
        let segments = split_identifier_text(input)?;
        decode_uuid_segments(segments)
    }

    /// Parse a textual identifier and decode all segments as UUIDs using `encoding`.
    pub fn parse_uuid_segments_with_encoding(
        input: &str,
        encoding: UuidEncoding,
    ) -> Result<Box<[Uuid]>, IdentifierUuidDecodeError> {
        let segments = split_identifier_text(input)?;
        decode_uuid_segments_with_encoding(segments, encoding)
    }

    /// Clone this immutable identifier into a mutable buffer.
    pub fn to_buf(&self) -> IdentifierBuf {
        IdentifierBuf {
            segments: self.segments.to_vec(),
        }
    }

    /// Convert this immutable identifier into a mutable buffer.
    pub fn into_buf(self) -> IdentifierBuf {
        IdentifierBuf {
            segments: self.segments.into_vec(),
        }
    }
}

impl From<IdentifierBuf> for Identifier {
    fn from(value: IdentifierBuf) -> Self {
        value.into_identifier()
    }
}

impl fmt::Debug for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Identifier({})",
            self.segments.iter().map(|s| format!("{s:?}")).join(", ")
        )
    }
}
impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.segments.iter().join(SEGMENT_SEPARATOR))
    }
}

/// Owned and mutable identifier buffer.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdentifierBuf {
    segments: Vec<IdentifierSegment>,
}

impl IdentifierBuf {
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// An empty identifier with no segments.
    pub const EMPTY: Self = Self {
        segments: Vec::new(),
    };

    pub fn into_identifier(self) -> Identifier {
        Identifier::from_segments_unchecked(self.segments.into_boxed_slice())
    }

    pub fn segments(self) -> impl Iterator<Item = IdentifierSegment> {
        self.segments.into_iter()
    }

    pub fn segments_iter(&self) -> impl Iterator<Item = &IdentifierSegment> {
        self.segments.iter()
    }

    /// An identifier buffer with the given `segments` as components, if they are all legal
    /// segments.
    ///
    /// Otherwise the errors indicate all the segments with illegal characters.
    pub fn try_from_array<I, const N: usize>(
        segments: [I; N],
    ) -> Result<Self, Errors<IdentifierError>>
    where
        I: Into<IdentifierSegment>,
    {
        let id_segments = segments.into_iter().map(|i| i.into()).collect_vec();
        id_segments
            .iter()
            .ensure_for_all(check_that_contains_only_legal_chars)?;
        Ok(Self {
            segments: id_segments,
        })
    }

    /// An identifier buffer with the given `segments` as components.
    ///
    /// Panics if it encounters illegal characters in any segment.
    pub fn from_array<I, const N: usize>(segments: [I; N]) -> Self
    where
        I: Into<IdentifierSegment>,
    {
        let id_segments = segments
            .into_iter()
            .map(|i| {
                let s = i.into();
                assert!(
                    contains_only_legal_chars(s.as_ref()),
                    "'{s}' contains illegal characters for an identifier segment"
                );
                s
            })
            .collect_vec();
        Self {
            segments: id_segments,
        }
    }

    /// An identifier buffer with the given `segments` as components.
    ///
    /// Uses the string representation of the [[Uuid]], which is always legal, so this never
    /// panics.
    pub fn from_uuid_array<const N: usize>(segments: [Uuid; N]) -> Self {
        Self::from_uuid_array_with_encoding(segments, UuidEncoding::Hyphenated)
    }

    /// An identifier buffer with the given `segments` as components encoded using `encoding`.
    pub fn from_uuid_array_with_encoding<const N: usize>(
        segments: [Uuid; N],
        encoding: UuidEncoding,
    ) -> Self {
        let id_segments = segments
            .into_iter()
            .map(|u| encode_uuid_segment(u, encoding))
            .collect_vec();
        Self {
            segments: id_segments,
        }
    }

    /// Extend this identifier buffer by adding a new segment to the end.
    ///
    /// Panics if `segment` contains illegal characters.
    pub fn push<I>(&mut self, segment: I)
    where
        I: Into<IdentifierSegment>,
    {
        let s = segment.into();
        assert!(
            contains_only_legal_chars(s.as_ref()),
            "'{s}' contains illegal characters for an identifier segment"
        );
        self.segments.push(s);
    }

    /// Extend this identifier buffer by adding a new `segment` to the end, if it is legal as a
    /// segment.
    ///
    /// Otherwise returns the input in the error.
    pub fn push_checked<I>(&mut self, segment: I) -> Result<(), IdentifierError>
    where
        I: Into<IdentifierSegment>,
    {
        let s = segment.into();
        check_that_contains_only_legal_chars(s.as_ref())?;
        self.segments.push(s);
        Ok(())
    }

    /// Extend this identifier buffer by adding a new segment to the end.
    pub fn push_uuid(&mut self, segment: Uuid) {
        self.push_uuid_with_encoding(segment, UuidEncoding::Hyphenated);
    }

    /// Extend this identifier buffer by adding a new UUID segment using `encoding`.
    pub fn push_uuid_with_encoding(&mut self, segment: Uuid, encoding: UuidEncoding) {
        self.segments.push(encode_uuid_segment(segment, encoding));
    }
}

impl From<Identifier> for IdentifierBuf {
    fn from(value: Identifier) -> Self {
        value.into_buf()
    }
}

impl<I> Extend<I> for IdentifierBuf
where
    I: Into<IdentifierSegment>,
{
    fn extend<T: IntoIterator<Item = I>>(&mut self, iter: T) {
        let id_segments = iter.into_iter().map(|i| {
            let s = i.into();
            assert!(
                contains_only_legal_chars(s.as_ref()),
                "'{s}' contains illegal characters for an identifier segment"
            );
            s
        });
        self.segments.extend(id_segments)
    }
}

impl fmt::Debug for IdentifierBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IdentifierBuf({})",
            self.segments.iter().map(|s| format!("{s:?}")).join(", ")
        )
    }
}

impl fmt::Display for IdentifierBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.segments.iter().join(SEGMENT_SEPARATOR))
    }
}

fn encode_uuid_segment(uuid: Uuid, encoding: UuidEncoding) -> IdentifierSegment {
    let raw = match encoding {
        UuidEncoding::Hyphenated => uuid.hyphenated().to_string(),
        UuidEncoding::Words => uuid
            .encode_words()
            .expect("encoding a UUID into words should not fail"),
        UuidEncoding::Base64 => uuid.encode_base64(),
    };
    let segment: IdentifierSegment = raw.into();
    debug_assert!(
        contains_only_legal_chars(segment.as_ref()),
        "'{segment}' contains illegal characters for an identifier segment"
    );
    segment
}

fn decode_uuid_segments<'a, I>(segments: I) -> Result<Box<[Uuid]>, IdentifierUuidDecodeError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut iter = segments.into_iter();
    let Some(first) = iter.next() else {
        return Ok(Box::new([]));
    };

    let encoding = infer_uuid_encoding_from_shape(first).context(UnknownEncodingShapeSnafu {
        segment: first.to_owned(),
    })?;

    let mut out = Vec::new();
    out.push(decode_uuid_segment_with_encoding(first, encoding, 0)?);

    for (offset, segment) in iter.enumerate() {
        let index = offset + 1;
        if let Some(found) = infer_uuid_encoding_from_shape(segment)
            && found != encoding
        {
            return MixedEncodingSnafu {
                expected: encoding,
                found,
                index,
                segment: segment.to_owned(),
            }
            .fail();
        }

        out.push(decode_uuid_segment_with_encoding(segment, encoding, index)?);
    }

    Ok(out.into_boxed_slice())
}

fn decode_uuid_segments_with_encoding<'a, I>(
    segments: I,
    encoding: UuidEncoding,
) -> Result<Box<[Uuid]>, IdentifierUuidDecodeError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut out = Vec::new();
    for (index, segment) in segments.into_iter().enumerate() {
        out.push(decode_uuid_segment_with_encoding(segment, encoding, index)?);
    }
    Ok(out.into_boxed_slice())
}

fn decode_uuid_segment_with_encoding(
    segment: &str,
    encoding: UuidEncoding,
    index: usize,
) -> Result<Uuid, IdentifierUuidDecodeError> {
    let decoded = match encoding {
        UuidEncoding::Hyphenated => {
            Uuid::try_parse(segment).map_err(|source| UuidEncodingError::DecodeHyphenatedError {
                input: segment.to_owned(),
                source,
            })
        }
        UuidEncoding::Words => <Uuid as UuidEncodingExt>::decode_words(segment),
        UuidEncoding::Base64 => <Uuid as UuidEncodingExt>::decode_base64(segment),
    };
    decoded.context(DecodeSegmentSnafu {
        index,
        segment: segment.to_owned(),
        encoding,
    })
}

fn infer_uuid_encoding_from_shape(segment: &str) -> Option<UuidEncoding> {
    if looks_like_hyphenated_uuid(segment) {
        Some(UuidEncoding::Hyphenated)
    } else if looks_like_base64_url_no_pad_uuid(segment) {
        Some(UuidEncoding::Base64)
    } else if looks_like_words_uuid(segment) {
        Some(UuidEncoding::Words)
    } else {
        None
    }
}

fn looks_like_hyphenated_uuid(segment: &str) -> bool {
    if segment.len() != 36 {
        return false;
    }
    for (index, b) in segment.bytes().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if b != b'-' {
                return false;
            }
        } else if !b.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

fn looks_like_base64_url_no_pad_uuid(segment: &str) -> bool {
    segment.len() == 22
        && segment
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn looks_like_words_uuid(segment: &str) -> bool {
    let mut count = 0usize;
    for word in segment.split(':') {
        count += 1;
        if word.is_empty() || !word.bytes().all(|b| b.is_ascii_lowercase()) {
            return false;
        }
    }
    count == 8
}

fn split_identifier_text(input: &str) -> Result<Vec<&str>, IdentifierUuidDecodeError> {
    if input.is_empty() {
        return Ok(vec![]);
    }

    let segments: Vec<_> = input.split(SEGMENT_SEPARATOR).collect();
    if segments.iter().any(|segment| segment.is_empty()) {
        return EmptySegmentSnafu {
            input: input.to_owned(),
        }
        .fail();
    }
    Ok(segments)
}

static LEGAL_CHARS_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9:_-]+$").unwrap());

fn check_that_contains_only_legal_chars<S>(s: S) -> Result<(), IdentifierError>
where
    S: AsRef<str>,
{
    let s_ref = s.as_ref();
    if contains_only_legal_chars(s_ref) {
        Ok(())
    } else {
        IllegalCharactersSnafu {
            segment: s_ref.to_owned(),
        }
        .fail()
    }
}

fn contains_only_legal_chars<S>(s: S) -> bool
where
    S: AsRef<str>,
{
    LEGAL_CHARS_PATTERN.is_match(s.as_ref())
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;
    use proptest::prelude::*;
    use uuid::Uuid;

    #[test]
    fn segments() {
        assert_matches!(check_that_contains_only_legal_chars("asimpleid"), Ok(_));
        assert_matches!(check_that_contains_only_legal_chars("a-simple-id"), Ok(_));
        assert_matches!(check_that_contains_only_legal_chars("a:simple:id"), Ok(_));

        assert_matches!(check_that_contains_only_legal_chars("a0simple1id2"), Ok(_));
        assert_matches!(
            check_that_contains_only_legal_chars("a-0-simple-1-id-2"),
            Ok(_)
        );
        assert_matches!(
            check_that_contains_only_legal_chars("a:0:simple:1:id:2"),
            Ok(_)
        );

        assert_matches!(check_that_contains_only_legal_chars(""), Err(_));
        assert_matches!(check_that_contains_only_legal_chars("a.b"), Err(_));
        assert_matches!(check_that_contains_only_legal_chars("a b"), Err(_));
    }

    proptest! {
        #[test]
        fn legal_segments(segment in "[a-zA-Z0-9:_-]+") {
            assert_matches!(check_that_contains_only_legal_chars(segment), Ok(_));
        }
    }

    proptest! {
        #[test]
        fn illegal_segments(segment in ".*") {
            // Just check that we don't panic for any input.
            let _res = check_that_contains_only_legal_chars(segment);
        }
    }

    #[test]
    fn identifiers() {
        assert_matches!(Identifier::try_from_array(["some", "legal", "id"]), Ok(_));
        assert_eq!(
            Identifier::from_array(["some", "legal", "id"]).to_string(),
            "some.legal.id"
        );
        {
            let mut id = IdentifierBuf::EMPTY;
            id.push("some");
            assert_eq!(id.to_string(), "some");
            id.push("legal");
            assert_eq!(id.to_string(), "some.legal");
            id.push("id");
            assert_eq!(
                Identifier::from_array(["some", "legal", "id"]).to_string(),
                "some.legal.id"
            );
            assert_matches!(id.push_checked("not.ok"), Err(_));
        }
        assert_matches!(Identifier::try_from_array(["", "not.legal", "also not legal"]), Err(e) if e.len() == 3);

        {
            let uuid1 = Uuid::new_v4();
            let uuid2 = Uuid::new_v4();
            let uuid3 = Uuid::new_v4();
            let mut id = IdentifierBuf::from_uuid_array([uuid1, uuid2, uuid3]);
            assert_eq!(
                id.to_string(),
                format!(
                    "{}.{}.{}",
                    uuid1.as_hyphenated(),
                    uuid2.as_hyphenated(),
                    uuid3.as_hyphenated()
                )
            );
            let uuid4 = Uuid::new_v4();
            id.push_uuid(uuid4);
            assert_eq!(
                id.to_string(),
                format!(
                    "{}.{}.{}.{}",
                    uuid1.as_hyphenated(),
                    uuid2.as_hyphenated(),
                    uuid3.as_hyphenated(),
                    uuid4.as_hyphenated()
                )
            );
        }
    }

    #[test]
    fn uuid_segment_encoding_roundtrip() {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();
        let expected = vec![uuid1, uuid2, uuid3];

        for encoding in [
            UuidEncoding::Hyphenated,
            UuidEncoding::Words,
            UuidEncoding::Base64,
        ] {
            let id = Identifier::from_uuid_array_with_encoding([uuid1, uuid2, uuid3], encoding);
            assert_eq!(
                id.decode_uuid_segments().unwrap().as_ref(),
                expected.as_slice()
            );
            assert_eq!(
                id.decode_uuid_segments_with_encoding(encoding)
                    .unwrap()
                    .as_ref(),
                expected.as_slice()
            );

            let text = id.to_string();
            assert_eq!(
                Identifier::parse_uuid_segments(&text).unwrap().as_ref(),
                expected.as_slice()
            );
            assert_eq!(
                Identifier::parse_uuid_segments_with_encoding(&text, encoding)
                    .unwrap()
                    .as_ref(),
                expected.as_slice()
            );
        }
    }

    #[test]
    fn uuid_segment_encoding_literals() {
        let uuid_zero = Uuid::from_bytes([0u8; 16]);
        let uuid_ff = Uuid::from_bytes([255u8; 16]);
        let expected = vec![uuid_zero, uuid_ff];

        let cases = [
            (
                UuidEncoding::Hyphenated,
                "00000000-0000-0000-0000-000000000000.ffffffff-ffff-ffff-ffff-ffffffffffff",
            ),
            (
                UuidEncoding::Base64,
                "AAAAAAAAAAAAAAAAAAAAAA._____________________w",
            ),
            (
                UuidEncoding::Words,
                "a:a:a:a:a:a:a:a.zyzzyva:zyzzyva:zyzzyva:zyzzyva:zyzzyva:zyzzyva:zyzzyva:zyzzyva",
            ),
        ];

        for (encoding, text) in cases {
            let id = Identifier::from_uuid_array_with_encoding([uuid_zero, uuid_ff], encoding);
            assert_eq!(id.to_string(), text);
            assert_eq!(
                id.decode_uuid_segments().unwrap().as_ref(),
                expected.as_slice()
            );
            assert_eq!(
                id.decode_uuid_segments_with_encoding(encoding)
                    .unwrap()
                    .as_ref(),
                expected.as_slice()
            );
            assert_eq!(
                Identifier::parse_uuid_segments(text).unwrap().as_ref(),
                expected.as_slice()
            );
            assert_eq!(
                Identifier::parse_uuid_segments_with_encoding(text, encoding)
                    .unwrap()
                    .as_ref(),
                expected.as_slice()
            );
        }
    }

    #[test]
    fn identifier_buf_uuid_encoding_literals() {
        let uuid_zero = Uuid::from_bytes([0u8; 16]);
        let uuid_ff = Uuid::from_bytes([255u8; 16]);

        let mut id =
            IdentifierBuf::from_uuid_array_with_encoding([uuid_zero], UuidEncoding::Base64);
        assert_eq!(id.to_string(), "AAAAAAAAAAAAAAAAAAAAAA");
        id.push_uuid_with_encoding(uuid_ff, UuidEncoding::Base64);
        assert_eq!(
            id.to_string(),
            "AAAAAAAAAAAAAAAAAAAAAA._____________________w"
        );
    }

    #[test]
    fn parse_uuid_segments_empty() {
        assert!(Identifier::parse_uuid_segments("").unwrap().is_empty());
        assert!(
            Identifier::parse_uuid_segments_with_encoding("", UuidEncoding::Words)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn parse_uuid_segments_mixed_encoding() {
        let first = Uuid::new_v4().hyphenated().to_string();
        let second = Uuid::new_v4().encode_base64();
        let id = Identifier::from_array([first, second]);
        assert_matches!(
            id.decode_uuid_segments(),
            Err(IdentifierUuidDecodeError::MixedEncodingError { .. })
        );
    }

    #[test]
    fn parse_uuid_segments_with_empty_identifier_segment_fails() {
        assert_matches!(
            Identifier::parse_uuid_segments("abc..def"),
            Err(IdentifierUuidDecodeError::EmptySegmentError { .. })
        );
    }

    proptest! {
        #[test]
        fn uuid_identifier_proptest_roundtrip(
            a in any::<[u8; 16]>(),
            b in any::<[u8; 16]>(),
            c in any::<[u8; 16]>(),
        ) {
            let values = [Uuid::from_bytes(a), Uuid::from_bytes(b), Uuid::from_bytes(c)];
            for encoding in [
                UuidEncoding::Hyphenated,
                UuidEncoding::Words,
                UuidEncoding::Base64,
            ] {
                let id = Identifier::from_uuid_array_with_encoding(values, encoding);
                let decoded = id.decode_uuid_segments().expect("inferred decoding should succeed");
                prop_assert_eq!(decoded.as_ref(), values.as_slice());

                let decoded_with_encoding = id.decode_uuid_segments_with_encoding(encoding).expect("decoding with known encoding should succeed");
                prop_assert_eq!(decoded_with_encoding.as_ref(), values.as_slice());

                let parsed = Identifier::parse_uuid_segments(&id.to_string()).expect("parsing should succeed");
                prop_assert_eq!(parsed.as_ref(), values.as_slice());
            }
        }
    }
}

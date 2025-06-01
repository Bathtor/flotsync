use crate::{
    errors::{Errors, ErrorsExt},
    utils::IString,
};
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identifier {
    segments: Vec<IdentifierSegment>,
}

impl Identifier {
    /// An empty identifier with no segments.
    pub const EMPTY: Self = Self {
        segments: Vec::new(),
    };

    /// An identifier with the given `segments` as components, if they are all legal segments.
    ///
    /// Otherwise the errors indicate all the segments with illegal characters.
    pub fn from_array_checked<I, const N: usize>(
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

    /// An identifier with the given `segments` as components.
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

    /// An identifier with the given `segments` as components.
    ///
    /// Uses the string representation of the [[Uuid]], which is always legal, so this never panics.
    pub fn from_uuid_array<const N: usize>(segments: [Uuid; N]) -> Self {
        let id_segments = segments
            .into_iter()
            .map(|u| {
                let i = u.hyphenated().to_string();
                let s: IString = i.into();
                // These don't really need to be checked, since their hyphenated format is always allowed.
                debug_assert!(
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

    /// Extend this identifier by adding a new segment to the end.
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

    /// Extend this identifier by adding a new `segment` to the end, if it is legal as a segment.
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

    /// Extend this identifier by adding a new segment to the end.
    pub fn push_uuid(&mut self, segment: Uuid) {
        let i = segment.hyphenated().to_string();
        let s: IString = i.into();
        debug_assert!(
            contains_only_legal_chars(s.as_ref()),
            "'{s}' contains illegal characters for an identifier segment"
        );
        self.segments.push(s);
    }
}

impl<I> Extend<I> for Identifier
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

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.segments.iter().join(SEGMENT_SEPARATOR))
    }
}

static LEGAL_CHARS_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9:-]+$").unwrap());

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
    use std::assert_matches::assert_matches;

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
        fn legal_segments(segment in "[a-zA-Z0-9:-]+") {
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
        assert_matches!(
            Identifier::from_array_checked(["some", "legal", "id"]),
            Ok(_)
        );
        assert_eq!(
            Identifier::from_array(["some", "legal", "id"]).to_string(),
            "some.legal.id"
        );
        {
            let mut id = Identifier::EMPTY;
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
        assert_matches!(Identifier::from_array_checked(["", "not.legal", "also not legal"]), Err(e) if e.len() == 3);

        {
            let uuid1 = Uuid::new_v4();
            let uuid2 = Uuid::new_v4();
            let uuid3 = Uuid::new_v4();
            let mut id = Identifier::from_uuid_array([uuid1, uuid2, uuid3]);
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
}

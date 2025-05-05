use crate::utils::IString;
use itertools::Itertools;
use std::fmt;

pub type IdentifierSegment = IString;

const SEGMENT_SEPARATOR: &str = ".";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Identifier {
    segments: Vec<IdentifierSegment>,
}

impl Identifier {
    pub fn from_array<I, const N: usize>(segments: [I; N]) -> Self
    where
        I: Into<IdentifierSegment>,
    {
        let id_segments = segments.into_iter().map(|i| i.into()).collect_vec();
        Self {
            segments: id_segments,
        }
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.segments.iter().join(SEGMENT_SEPARATOR))
    }
}

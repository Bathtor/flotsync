use super::{HappenedBeforeOrd, HappenedBeforeOrdering};
use crate::option_when;
use itertools::Itertools;
use std::{cmp, fmt, num::NonZeroUsize};

/// The most general version vector that abstracts over the exact representation.
#[derive(Clone, Debug, Eq)]
pub enum VersionVector {
    /// This is the full reference representation, which every participant's version listed explicitly.
    Full(PureVersionVector),
    /// The system is mostly synced up, but a single participant is posting a new version.
    Override {
        num_members: NonZeroUsize,
        version: OverrideVersion,
    },
    /// The system is fully synced up and all participants have exactly the same version.
    Synced {
        num_members: NonZeroUsize,
        version: u64,
    },
}
impl VersionVector {
    pub const fn num_members(&self) -> NonZeroUsize {
        match self {
            VersionVector::Full(v) => v.len(),
            VersionVector::Override { num_members, .. } => *num_members,
            VersionVector::Synced { num_members, .. } => *num_members,
        }
    }

    pub const fn max_version(&self) -> u64 {
        match self {
            VersionVector::Full(v) => v.max_version(),
            VersionVector::Override { version, .. } => version.override_version,
            VersionVector::Synced { version, .. } => *version,
        }
    }

    pub fn succ_at(&self, position: usize) -> Self {
        let mut next = self.clone();
        next.increment_at(position);
        next
    }

    pub fn increment_at(&mut self, position: usize) {
        assert!(
            position < self.num_members().get(),
            "Position {position} is outside of group range (0-{})",
            self.num_members()
        );
        match self {
            VersionVector::Full(v) => v.increment_at(position),
            VersionVector::Override {
                num_members,
                version,
            } => {
                if position == version.override_position {
                    version.override_version = version
                        .override_version
                        .checked_add(1)
                        .expect("Max version reached");
                } else {
                    let mut full = version.to_vector(*num_members);
                    full.increment_at(position);
                    *self = Self::Full(full);
                }
            }
            VersionVector::Synced {
                num_members,
                version,
            } => {
                if num_members.get() == 1 {
                    *version += 1;
                } else {
                    *self = Self::Override {
                        num_members: *num_members,
                        version: OverrideVersion::with_next_version(*version, position),
                    }
                }
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> {
        self.into_iter()
    }
}
impl fmt::Display for VersionVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionVector::Full(v) => write!(f, "{v}"),
            VersionVector::Override {
                num_members,
                version,
            } => {
                let last_position = num_members.get() - 1;
                if version.override_position == 0 {
                    write!(
                        f,
                        "〈{}, 1-{}:{}〉",
                        version.override_version, last_position, version.group_version
                    )
                } else if version.override_position == last_position {
                    write!(
                        f,
                        "〈0-{}:{}, {}〉",
                        last_position - 1,
                        version.group_version,
                        version.override_version
                    )
                } else {
                    let pre_override = version.override_position - 1;
                    let post_override = version.override_position + 1;
                    write!(
                        f,
                        "〈0-{}:{}, {}:{}, {}-{}:{}〉",
                        pre_override,
                        version.group_version,
                        version.override_position,
                        version.override_version,
                        post_override,
                        last_position,
                        version.group_version
                    )
                }
            }
            VersionVector::Synced {
                num_members,
                version,
            } => write!(f, "〈0-{}:{}〉", num_members.get() - 1, version),
        }
    }
}
impl HappenedBeforeOrd for VersionVector {
    fn hb_cmp(&self, other: &Self) -> HappenedBeforeOrdering {
        if self.num_members() != other.num_members() {
            return HappenedBeforeOrdering::Incomparable;
        }
        match (self, other) {
            (VersionVector::Full(v1), VersionVector::Full(v2)) => v1.hb_cmp(v2),
            (
                VersionVector::Full(v1),
                VersionVector::Override {
                    num_members,
                    version: v2,
                },
            ) => {
                assert!(
                    num_members.get() > 1,
                    "Override with a single member is not supported"
                );
                hb_compare_full_override(v1, v2)
            }
            (VersionVector::Full(v1), VersionVector::Synced { version: v2, .. }) => {
                hb_compare_full_synced(v1, *v2)
            }
            (
                VersionVector::Override {
                    num_members,
                    version: v1,
                },
                VersionVector::Full(v2),
            ) => {
                assert!(
                    num_members.get() > 1,
                    "Override with a single member is not supported"
                );
                hb_compare_full_override(v2, v1).reverse()
            }
            (
                VersionVector::Override {
                    version: v1,
                    num_members: n1,
                },
                VersionVector::Override {
                    version: v2,
                    num_members: n2,
                },
            ) => {
                assert!(
                    n1.get() > 1,
                    "Override with a single member is not supported"
                );
                assert!(
                    n2.get() > 1,
                    "Override with a single member is not supported"
                );
                v1.hb_cmp(v2)
            }
            (
                VersionVector::Override {
                    version: v1,
                    num_members,
                },
                VersionVector::Synced { version: v2, .. },
            ) => {
                assert!(
                    num_members.get() > 1,
                    "Override with a single member is not supported"
                );
                hb_compare_override_synced(v1, *v2)
            }
            (VersionVector::Synced { version: v1, .. }, VersionVector::Full(v2)) => {
                hb_compare_full_synced(v2, *v1).reverse()
            }
            (
                VersionVector::Synced { version: v1, .. },
                VersionVector::Override {
                    version: v2,
                    num_members,
                },
            ) => {
                assert!(
                    num_members.get() > 1,
                    "Override with a single member is not supported"
                );
                hb_compare_override_synced(v2, *v1).reverse()
            }
            (
                VersionVector::Synced { version: v1, .. },
                VersionVector::Synced { version: v2, .. },
            ) => v1.cmp(v2).into(),
        }
    }
}
impl PartialEq for VersionVector {
    fn eq(&self, other: &Self) -> bool {
        self.hb_cmp(other) == HappenedBeforeOrdering::Equal
    }
}
impl PartialOrd for VersionVector {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.hb_cmp(other).into()
    }
}
impl<'a> IntoIterator for &'a VersionVector {
    type Item = u64;

    type IntoIter = VersionVectorIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let internal = match self {
            VersionVector::Full(v) => VersionVectorIterInternal::Full(v.0.iter().cloned()),
            VersionVector::Override {
                num_members,
                version,
            } => VersionVectorIterInternal::Override(OverrideIter::new(
                *num_members,
                version.clone(),
            )),
            VersionVector::Synced {
                num_members,
                version,
            } => {
                VersionVectorIterInternal::Synced(std::iter::repeat_n(*version, num_members.get()))
            }
        };
        VersionVectorIter(internal)
    }
}

/*
This is to hide the internals of the iterator implementation from the public API.
*/

/// Use [[VersionVector::iter()]].
pub struct VersionVectorIter<'a>(VersionVectorIterInternal<'a>);
impl<'a> Iterator for VersionVectorIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

enum VersionVectorIterInternal<'a> {
    Full(std::iter::Cloned<std::slice::Iter<'a, u64>>),
    Override(OverrideIter),
    Synced(std::iter::RepeatN<u64>),
}
impl<'a> Iterator for VersionVectorIterInternal<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Full(iter) => iter.next(),
            Self::Override(iter) => iter.next(),
            Self::Synced(iter) => iter.next(),
        }
    }
}

/// This is somewhat equivalent to a Set<Ordering> just much more compact.
struct EncounteredOrderings {
    has_equal: bool,
    has_less: bool,
    has_greater: bool,
}
impl EncounteredOrderings {
    const fn none() -> Self {
        Self {
            has_equal: false,
            has_less: false,
            has_greater: false,
        }
    }

    fn update(&mut self, ord: cmp::Ordering) {
        match ord {
            cmp::Ordering::Less => {
                self.has_less = true;
            }
            cmp::Ordering::Equal => {
                self.has_equal = true;
            }
            cmp::Ordering::Greater => {
                self.has_greater = true;
            }
        }
    }

    fn has_less_and_greater(&self) -> bool {
        self.has_less && self.has_greater
    }

    fn to_hb_assume_loop_check(&self) -> HappenedBeforeOrdering {
        debug_assert!(self.has_equal || self.has_less || self.has_greater);
        if self.has_equal && !(self.has_less || self.has_greater) {
            HappenedBeforeOrdering::Equal
        } else if self.has_less && !self.has_greater {
            HappenedBeforeOrdering::Before
        } else if self.has_greater && !self.has_less {
            HappenedBeforeOrdering::After
        } else {
            unreachable!("This should be covered by the check at the end of the for loop already.");
        }
    }
}

// These functions are not public, because they elide the member set compatibility check that is done in VersionVector::hb_cmp.
fn hb_compare_full_override(f: &PureVersionVector, o: &OverrideVersion) -> HappenedBeforeOrdering {
    f.assert_valid();
    o.assert_valid();

    let mut orderings = EncounteredOrderings::none();
    for (pos, value) in f.0.iter().enumerate() {
        let result = if pos == o.override_position {
            value.cmp(&o.override_version)
        } else {
            value.cmp(&o.group_version)
        };
        orderings.update(result);
        if orderings.has_less_and_greater() {
            // We can stop checking early in this case.
            return HappenedBeforeOrdering::Concurrent;
        }
    }
    orderings.to_hb_assume_loop_check()
}
fn hb_compare_full_synced(f: &PureVersionVector, synced_version: u64) -> HappenedBeforeOrdering {
    f.assert_valid();

    let mut orderings = EncounteredOrderings::none();
    for value in f.0.iter() {
        orderings.update(value.cmp(&synced_version));
        if orderings.has_less_and_greater() {
            // We can stop checking early in this case.
            return HappenedBeforeOrdering::Concurrent;
        }
    }
    orderings.to_hb_assume_loop_check()
}
fn hb_compare_override_synced(o: &OverrideVersion, synced_version: u64) -> HappenedBeforeOrdering {
    o.assert_valid();

    match o.group_version.cmp(&synced_version) {
        cmp::Ordering::Less => match o.override_version.cmp(&synced_version) {
            cmp::Ordering::Less => HappenedBeforeOrdering::Before, //  (5, 6) vs. 7
            cmp::Ordering::Equal => HappenedBeforeOrdering::Before, //  (5, 6) vs. 6
            cmp::Ordering::Greater => HappenedBeforeOrdering::Concurrent, // (5, 7) vs. 6
        },
        cmp::Ordering::Equal => HappenedBeforeOrdering::After, // (5, 6) vs. 5
        cmp::Ordering::Greater => HappenedBeforeOrdering::After, // (5, 6) vs. 4
    }
}

/// The traditiononal array representation of a vector with every position corresponding to that member.
///
/// Note that empty vectors are not supported.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PureVersionVector(pub Box<[u64]>);
impl PureVersionVector {
    pub const fn len(&self) -> NonZeroUsize {
        self.assert_valid();
        NonZeroUsize::new(self.0.len())
            .expect("We just checked that the vector's length is non-zero")
    }

    pub const fn max_version(&self) -> u64 {
        self.assert_valid();

        // This isn't const.
        // self.0.iter().max().cloned().unwrap()
        // A very manual, but const, max ;)
        let mut max = self.0[0];
        let mut i = 1;
        while i < self.0.len() {
            let v = self.0[i];
            if max < v {
                max = v;
            }
            i += 1;
        }
        max
    }

    pub fn increment_at(&mut self, position: usize) {
        // No need to check position, indexed access is anyway checked.
        self.0[position] = self.0[position]
            .checked_add(1)
            .expect("Max version reached");
    }

    const fn is_valid(&self) -> bool {
        !self.0.is_empty()
    }

    #[inline(always)]
    const fn assert_valid(&self) {
        debug_assert!(self.is_valid());
    }
}
impl<const N: usize> From<[u64; N]> for PureVersionVector {
    fn from(entries: [u64; N]) -> Self {
        assert!(N > 0, "N must be greater than 0");
        Self(Box::from(entries))
    }
}
impl From<Vec<u64>> for PureVersionVector {
    fn from(entries: Vec<u64>) -> Self {
        assert!(!entries.is_empty(), "Must have at least 1 entry.");
        Self(entries.into_boxed_slice())
    }
}
impl fmt::Display for PureVersionVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "〈{}〉", self.0.iter().join(", "))
    }
}
impl HappenedBeforeOrd for PureVersionVector {
    fn hb_cmp(&self, other: &Self) -> HappenedBeforeOrdering {
        self.assert_valid();
        other.assert_valid();

        if self.0.len() == other.0.len() {
            if self.0.is_empty() {
                debug_assert!(other.0.is_empty()); // How could it be otherwise?
                HappenedBeforeOrdering::Equal
            } else {
                let mut orderings = EncounteredOrderings::none();
                for (s, o) in self.0.iter().zip(other.0.iter()) {
                    orderings.update(s.cmp(o));
                    if orderings.has_less_and_greater() {
                        // We can stop checking early in this case.
                        return HappenedBeforeOrdering::Concurrent;
                    }
                }
                orderings.to_hb_assume_loop_check()
            }
        } else {
            // Vectors of different length cannot be sensibly compared.
            HappenedBeforeOrdering::Incomparable
        }
    }
}
impl PartialOrd for PureVersionVector {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.hb_cmp(other).into()
    }
}

/// A representation of a [[VersionVector]] for when the system is mostly synced up,
/// but a single member is posting a new version.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OverrideVersion {
    /// Everyone has this version, except the member with [[override_position]].
    group_version: u64,
    /// The position of the member with the newer version in the full vector of members.
    pub override_position: usize,
    /// The new version at this member.
    ///
    /// This must be > [[group_version]]!
    override_version: u64,
}
impl OverrideVersion {
    /// Panics if the combination of `group_version` and `override_version` is not legal.
    pub const fn new(group_version: u64, override_position: usize, override_version: u64) -> Self {
        assert!(group_version < override_version);
        Self {
            group_version,
            override_position,
            override_version,
        }
    }

    /// Returns `None` if the combination of `group_version` and `override_version` is not legal.
    pub const fn new_opt(
        group_version: u64,
        override_position: usize,
        override_version: u64,
    ) -> Option<Self> {
        option_when!(
            group_version < override_version,
            Self {
                group_version,
                override_position,
                override_version,
            }
        )
    }

    /// Creates a new instance where `override_version = group_version + 1`.
    pub const fn with_next_version(group_version: u64, override_position: usize) -> Self {
        Self {
            group_version,
            override_position,
            override_version: group_version.checked_add(1).expect("Max version reached"),
        }
    }

    /// Everyone has this version, except the member with [[override_position]].
    pub const fn group_version(&self) -> u64 {
        self.group_version
    }

    /// The new version at the member with [[override_position]].
    pub const fn override_version(&self) -> u64 {
        self.override_version
    }

    pub fn to_vector(&self, num_members: NonZeroUsize) -> PureVersionVector {
        let mut entries = vec![self.group_version; num_members.get()];
        entries[self.override_position] = self.override_version;
        PureVersionVector::from(entries)
    }

    const fn is_valid(&self) -> bool {
        self.group_version < self.override_version
    }

    /// Panic if not [[is_valid]].
    #[inline(always)]
    fn assert_valid(&self) {
        // Can be debug assert now, because we are enforcing this during construction.
        debug_assert!(self.is_valid(), "Invalid override version: {self:?}");
    }
}
impl HappenedBeforeOrd for OverrideVersion {
    fn hb_cmp(&self, other: &Self) -> HappenedBeforeOrdering {
        self.assert_valid();
        other.assert_valid();
        if self.override_position == other.override_position {
            match self.group_version.cmp(&other.group_version) {
                cmp::Ordering::Less => {
                    match self.override_version.cmp(&other.override_version) {
                        cmp::Ordering::Less => HappenedBeforeOrdering::Before, // (5, 6) vs. (6, 7)
                        cmp::Ordering::Equal => HappenedBeforeOrdering::Before, // (5, 6) vs (6, 6)
                        cmp::Ordering::Greater => HappenedBeforeOrdering::Concurrent, // Concurrent, e.g. (5, 8) vs. (6, 7)
                    }
                }
                cmp::Ordering::Equal => self.override_version.cmp(&other.override_version).into(),
                cmp::Ordering::Greater => {
                    match self.override_version.cmp(&other.override_version) {
                        cmp::Ordering::Less => HappenedBeforeOrdering::Concurrent, // Concurrent, e.g. (6, 7) vs (5, 8)
                        cmp::Ordering::Equal => HappenedBeforeOrdering::After, // (6, 7) vs (5, 7)
                        cmp::Ordering::Greater => HappenedBeforeOrdering::After, // (6, 7) vs. (5, 6)
                    }
                }
            }
        } else {
            // When we have different ids, then the override_version of one is part of the other's group_version.
            match self.override_version.cmp(&other.group_version) {
                cmp::Ordering::Less => HappenedBeforeOrdering::Before, // (5, 6) vs (7, 8)
                cmp::Ordering::Equal => HappenedBeforeOrdering::Before, // (5, 6) vs (6, 7)
                cmp::Ordering::Greater => match other.override_version.cmp(&self.group_version) {
                    cmp::Ordering::Less => HappenedBeforeOrdering::After, // (7, 8) vs (5, 6)
                    cmp::Ordering::Equal => HappenedBeforeOrdering::After, // (6, 7) vs (5, 6)
                    cmp::Ordering::Greater => HappenedBeforeOrdering::Concurrent, // Concurrent (7, 8) vs. (5, 8)
                },
            }
        }
    }
}
impl PartialOrd for OverrideVersion {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.hb_cmp(other).into()
    }
}
impl fmt::Display for OverrideVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "〈{}..., {}:{}, {}...〉",
            self.group_version, self.override_position, self.override_version, self.group_version,
        )
    }
}

struct OverrideIter {
    num_members: NonZeroUsize,
    underlying: OverrideVersion,
    next_position: usize,
}
impl OverrideIter {
    fn new(num_members: NonZeroUsize, underlying: OverrideVersion) -> Self {
        Self {
            underlying,
            next_position: 0usize,
            num_members,
        }
    }
}
impl Iterator for OverrideIter {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_position < self.num_members.get() {
            let res = if self.next_position == self.underlying.override_position {
                Some(self.underlying.override_version)
            } else {
                Some(self.underlying.group_version)
            };
            self.next_position += 1;
            res
        } else {
            None
        }
    }
}

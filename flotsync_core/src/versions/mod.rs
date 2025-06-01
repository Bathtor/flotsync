//! The [[VersionVector]] is a hierarchal tree variant of a [Version Vector](https://en.wikipedia.org/wiki/Version_vector).
//! It describe has entries for the local version at each group member, which may however be collapsed to save space when they are all the same.

use crate::option_when;
use itertools::Itertools;
use std::{cmp, fmt, num::NonZeroUsize};

/// Establishes the "happened-before" order.
///
/// This is a form of partial order, but an additional variant of incomparable is "concurrent".
///
/// Otherwise it just has better naming to clarify the implication.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum HappenedBeforeOrdering {
    /// `a` happened strictly before `b`.
    Before,
    /// `a == b` (exactly at the same time).
    Equal,
    /// `a` happened strictly after `b`.
    After,
    /// `a` and `b` are concurrent.
    Concurrent,
    /// `a` and `b` are incomparable (e.g. due to different vector sizes).
    Incomparable,
}
impl HappenedBeforeOrdering {
    /// Reverses the ordering.
    ///
    /// - `Before` becomes `After`.
    /// - `After` becomes `Before`.
    /// - Everything else stays the same.
    pub const fn reverse(self) -> HappenedBeforeOrdering {
        match self {
            HappenedBeforeOrdering::Before => HappenedBeforeOrdering::After,
            HappenedBeforeOrdering::After => HappenedBeforeOrdering::Before,
            _ => self,
        }
    }
}

impl From<cmp::Ordering> for HappenedBeforeOrdering {
    fn from(value: cmp::Ordering) -> Self {
        match value {
            cmp::Ordering::Less => HappenedBeforeOrdering::Before,
            cmp::Ordering::Equal => HappenedBeforeOrdering::Equal,
            cmp::Ordering::Greater => HappenedBeforeOrdering::After,
        }
    }
}

impl From<HappenedBeforeOrdering> for Option<cmp::Ordering> {
    fn from(val: HappenedBeforeOrdering) -> Self {
        match val {
            HappenedBeforeOrdering::Before => Some(cmp::Ordering::Less),
            HappenedBeforeOrdering::Equal => Some(cmp::Ordering::Equal),
            HappenedBeforeOrdering::After => Some(cmp::Ordering::Greater),
            HappenedBeforeOrdering::Concurrent => None,
            HappenedBeforeOrdering::Incomparable => None,
        }
    }
}

/// Trait for types that can establish a [happened-before order](HappenedBeforeOrdering).
///
/// This is a form of partial order, so the same rules as [[PartialOrd]] apply, but an additional variants of incomparable is "concurrent".
pub trait HappenedBeforeOrd<Rhs = Self>: PartialEq<Rhs>
where
    Rhs: ?Sized,
{
    fn hb_cmp(&self, other: &Rhs) -> HappenedBeforeOrdering;

    /// Get something that can be used to compare using [[PartialOrd]] for this instance.
    fn ord(&self) -> HappenedBeforePartialOrdWrapper<Self> {
        HappenedBeforePartialOrdWrapper(self)
    }
}

/// A wrapper that allows [[HappenedBeforeOrd]] types to be treated as [[PartialOrd]].
///
/// This is just a workaround for the orphan rules.
pub struct HappenedBeforePartialOrdWrapper<'a, T: ?Sized>(&'a T);

impl<T> PartialEq for HappenedBeforePartialOrdWrapper<'_, T>
where
    T: HappenedBeforeOrd + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> PartialOrd for HappenedBeforePartialOrdWrapper<'_, T>
where
    T: HappenedBeforeOrd + ?Sized,
{
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        self.0.hb_cmp(other.0).into()
    }
}

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
pub struct PureVersionVector(Box<[u64]>);
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
    /// Everyone has this version, except the member with [[override_id]].
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

    pub const fn group_version(&self) -> u64 {
        self.group_version
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{prelude::*, strategy::Union};

    const LARGE_VERSION: u64 = (u32::MAX as u64) + 1;

    #[test]
    fn string_representations() {
        const FOUR_MEMBERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

        let synced_v = VersionVector::Synced {
            num_members: FOUR_MEMBERS,
            version: 12,
        };
        assert_eq!(synced_v.to_string(), "〈0-3:12〉".to_string());

        let override_v_inner = OverrideVersion::with_next_version(12, 2);
        assert_eq!(
            override_v_inner.to_string(),
            "〈12..., 2:13, 12...〉".to_string()
        );
        let override_v = VersionVector::Override {
            num_members: FOUR_MEMBERS,
            version: override_v_inner,
        };
        assert_eq!(
            override_v.to_string(),
            "〈0-1:12, 2:13, 3-3:12〉".to_string()
        );

        let full_v_inner = PureVersionVector::from([12, 13, 12, 11]);
        assert_eq!(full_v_inner.to_string(), "〈12, 13, 12, 11〉".to_string());
        let full_v = VersionVector::Full(full_v_inner);
        assert_eq!(full_v.to_string(), "〈12, 13, 12, 11〉".to_string());
    }

    /// Just some shorthands, to make the tests easier to read.
    mod helpers {
        use super::*;

        pub const BEFORE: HappenedBeforeOrdering = HappenedBeforeOrdering::Before;
        pub const AFTER: HappenedBeforeOrdering = HappenedBeforeOrdering::After;
        pub const EQUAL: HappenedBeforeOrdering = HappenedBeforeOrdering::Equal;
        pub const CONCURRENT: HappenedBeforeOrdering = HappenedBeforeOrdering::Concurrent;
        pub fn pure(a: [u64; 3]) -> VersionVector {
            let full = PureVersionVector::from(a);
            VersionVector::Full(full)
        }
        pub fn over(gv: u64, ov: (usize, u64)) -> VersionVector {
            let version = OverrideVersion::new(gv, ov.0, ov.1);
            VersionVector::Override {
                num_members: NonZeroUsize::new(3).unwrap(),
                version,
            }
        }

        pub fn sync(version: u64) -> VersionVector {
            VersionVector::Synced {
                num_members: NonZeroUsize::new(3).unwrap(),
                version,
            }
        }
    }

    #[test]
    fn special_equivalents() {
        use helpers::*;
        // These values are the same in different representations
        assert_eq!(sync(1), pure([1, 1, 1]));
        assert_eq!(sync(1).hb_cmp(&pure([1, 1, 1])), EQUAL);

        assert_eq!(over(1, (0, 2)), pure([2, 1, 1]));
        assert_eq!(over(1, (0, 2)).hb_cmp(&pure([2, 1, 1])), EQUAL);

        assert_eq!(over(1, (1, 3)), pure([1, 3, 1]));
        assert_eq!(over(1, (1, 3)).hb_cmp(&pure([1, 3, 1])), EQUAL);
    }

    #[test]
    fn basic_relationships() {
        use helpers::*;

        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 2, 3])), EQUAL);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 3, 3])), BEFORE);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 1, 3])), AFTER);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([4, 5, 6])), BEFORE);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 2, 18])), BEFORE);
        assert_eq!(pure([1, 2, 3]).hb_cmp(&pure([1, 3, 1])), CONCURRENT);

        assert_eq!(over(1, (1, 2)).hb_cmp(&over(1, (1, 2))), EQUAL);
        assert_eq!(over(1, (1, 2)).hb_cmp(&pure([1, 2, 1])), EQUAL);
        assert_eq!(over(1, (1, 2)).hb_cmp(&over(1, (1, 3))), BEFORE);
        assert_eq!(over(0, (1, 2)).hb_cmp(&over(1, (1, 2))), BEFORE);
        assert_eq!(over(0, (1, 3)).hb_cmp(&over(1, (1, 2))), CONCURRENT);
        assert_eq!(over(1, (1, 2)).hb_cmp(&over(1, (0, 2))), CONCURRENT);
        assert_eq!(over(0, (1, 1)).hb_cmp(&over(1, (0, 2))), BEFORE);
        assert_eq!(over(0, (1, 2)).hb_cmp(&over(0, (1, 1))), AFTER);

        assert_eq!(sync(1).hb_cmp(&sync(1)), EQUAL);
        assert_eq!(sync(1).hb_cmp(&pure([1, 1, 1])), EQUAL);
        assert_eq!(sync(1).hb_cmp(&sync(2)), BEFORE);
        assert_eq!(sync(3).hb_cmp(&sync(2)), AFTER);
        assert_eq!(sync(1).hb_cmp(&pure([1, 2, 1])), BEFORE);
        assert_eq!(sync(1).hb_cmp(&pure([0, 2, 1])), CONCURRENT);
    }

    #[test]
    fn successors() {
        use helpers::*;

        assert_eq!(pure([1, 2, 3]).succ_at(0), pure([2, 2, 3]));
        assert_eq!(pure([1, 2, 3]).succ_at(1), pure([1, 3, 3]));
        assert_eq!(pure([1, 2, 3]).succ_at(2), pure([1, 2, 4]));
        assert_eq!(pure([1, 2, 3]).succ_at(2).succ_at(2), pure([1, 2, 5]));
        assert_eq!(pure([1, 2, 3]).succ_at(1).succ_at(2), pure([1, 3, 4]));

        assert_eq!(over(1, (0, 2)).succ_at(0), over(1, (0, 3)));
        assert_eq!(over(1, (0, 2)).succ_at(0).succ_at(0), over(1, (0, 4)));
        assert_eq!(over(1, (0, 2)).succ_at(1), pure([2, 2, 1]));
        assert_eq!(over(1, (0, 2)).succ_at(0).succ_at(1), pure([3, 2, 1]));

        assert_eq!(sync(1).succ_at(0), over(1, (0, 2)));
        assert_eq!(sync(1).succ_at(1), over(1, (1, 2)));
        assert_eq!(sync(1).succ_at(0).succ_at(1), pure([2, 2, 1]));
    }

    fn full_vector_strategy() -> impl Strategy<Value = VersionVector> {
        // Don't make ridiculously large vectors, since they take a lot of memory.
        prop::collection::vec(any::<u64>(), 1..100)
            .prop_map(|entries| VersionVector::Full(PureVersionVector::from(entries)))
    }

    fn override_vector_strategy() -> impl Strategy<Value = VersionVector> {
        (any::<NonZeroUsize>(), 0..LARGE_VERSION).prop_flat_map(|(num_members, group_version)| {
            (0..num_members.get(), (group_version + 1)..u64::MAX).prop_map(
                move |(override_position, override_version)| VersionVector::Override {
                    num_members,
                    version: OverrideVersion::new(
                        group_version,
                        override_position,
                        override_version,
                    ),
                },
            )
        })
    }

    fn version_vector_strategy() -> impl Strategy<Value = VersionVector> {
        prop_oneof![
            full_vector_strategy(),
            override_vector_strategy(),
            (any::<NonZeroUsize>(), any::<u64>()).prop_map(|(num_members, version)| {
                VersionVector::Synced {
                    num_members,
                    version,
                }
            }),
        ]
    }

    fn fixed_size_full_vector_strategy(
        num_members: NonZeroUsize,
    ) -> impl Strategy<Value = VersionVector> {
        prop::collection::vec(any::<u64>(), num_members.get())
            .prop_map(|entries| VersionVector::Full(PureVersionVector(entries.into_boxed_slice())))
    }

    fn fixed_size_override_vector_strategy(
        num_members: NonZeroUsize,
    ) -> impl Strategy<Value = VersionVector> {
        (0..LARGE_VERSION).prop_flat_map(move |group_version| {
            (0..num_members.get(), (group_version + 1)..u64::MAX).prop_map(
                move |(override_position, override_version)| VersionVector::Override {
                    num_members,
                    version: OverrideVersion::new(
                        group_version,
                        override_position,
                        override_version,
                    ),
                },
            )
        })
    }

    fn fixed_size_synced_strategy(
        num_members: NonZeroUsize,
    ) -> impl Strategy<Value = VersionVector> {
        any::<u64>().prop_map(move |version| VersionVector::Synced {
            num_members,
            version,
        })
    }

    fn fixed_size_version_vector_strategy(
        num_members: NonZeroUsize,
    ) -> BoxedStrategy<VersionVector> {
        let mut strategies = vec![
            fixed_size_full_vector_strategy(num_members).boxed(),
            fixed_size_synced_strategy(num_members).boxed(),
        ];
        if num_members.get() > 1 {
            strategies.push(fixed_size_override_vector_strategy(num_members).boxed());
        }
        Union::new(strategies).boxed()
    }

    fn version_vector_size_strategy() -> impl Strategy<Value = NonZeroUsize> {
        (1usize..100usize).prop_map(|u| NonZeroUsize::new(u).unwrap())
    }

    prop_compose! {
        fn equal_size_version_vector_strategy()(l in version_vector_size_strategy())(v1 in fixed_size_version_vector_strategy(l), v2 in fixed_size_version_vector_strategy(l), v3 in fixed_size_version_vector_strategy(l)) -> (VersionVector, VersionVector, VersionVector) {
            (v1, v2, v3)
        }
    }

    proptest! {
        #[test]
        fn version_vector_invariants(v1 in version_vector_strategy(), v2 in version_vector_strategy(), v3 in version_vector_strategy()) {
            version_vector_invariants_impl(v1, v2, v3)
        }

        #[test]
        fn version_vector_equal_size_invariants((v1, v2, v3) in equal_size_version_vector_strategy()) {
            version_vector_invariants_impl(v1, v2, v3)
        }
    }
    fn version_vector_invariants_impl(v1: VersionVector, v2: VersionVector, v3: VersionVector) {
        single_version_vector_invariants_impl(v1.clone());
        single_version_vector_invariants_impl(v2.clone());
        single_version_vector_invariants_impl(v3.clone());
        two_version_vector_invariants_impl(v1.clone(), v2.clone());
        two_version_vector_invariants_impl(v2.clone(), v3.clone());
        two_version_vector_invariants_impl(v1.clone(), v3.clone());

        // Transitive
        if v1 <= v2 && v2 <= v3 {
            assert!(v1 <= v3);
        }
        if v1.hb_cmp(&v2) == HappenedBeforeOrdering::Before
            && v2.hb_cmp(&v3) == HappenedBeforeOrdering::Before
        {
            assert_eq!(v1.hb_cmp(&v3), HappenedBeforeOrdering::Before);
        }
    }
    fn single_version_vector_invariants_impl(v: VersionVector) {
        // Reflexive
        #[allow(clippy::eq_op)]
        {
            assert_eq!(v, v);
        }
        assert_eq!(v.hb_cmp(&v), HappenedBeforeOrdering::Equal);

        // Ensure that we don't overflow, and also we don't run out of memory if we need to expand to a full vector.
        if v.max_version() < u64::MAX - 2 && v.num_members().get() < 100 {
            // Increments
            for pos in 0..v.num_members().get() {
                let next = v.succ_at(pos);
                assert!(v < next);
                assert_eq!(v.hb_cmp(&next), HappenedBeforeOrdering::Before);
                assert_eq!(next.hb_cmp(&v), HappenedBeforeOrdering::After);

                let next_again = next.succ_at(pos);
                assert!(v < next_again);
                assert!(next < next_again);
                assert_eq!(v.hb_cmp(&next_again), HappenedBeforeOrdering::Before);
                assert_eq!(next.hb_cmp(&next_again), HappenedBeforeOrdering::Before);
            }
        }
    }
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    fn two_version_vector_invariants_impl(v1: VersionVector, v2: VersionVector) {
        // v1 == v2 iff v1 =hb= v2
        if v1 == v2 {
            assert_eq!(v1.hb_cmp(&v2), HappenedBeforeOrdering::Equal);
        } else {
            assert_ne!(v1.hb_cmp(&v2), HappenedBeforeOrdering::Equal);
        }
        if v1.hb_cmp(&v2) == HappenedBeforeOrdering::Equal {
            assert_eq!(v1, v2);
        } else {
            assert_ne!(v1, v2);
        }

        // v1 < v2 iff v1 -> v2
        if v1 < v2 {
            assert_eq!(v1.hb_cmp(&v2), HappenedBeforeOrdering::Before);
        } else {
            assert_ne!(v1.hb_cmp(&v2), HappenedBeforeOrdering::Before);
        }
        if v1.hb_cmp(&v2) == HappenedBeforeOrdering::Before {
            assert!(v1 < v2);
        } else {
            assert!(!(v1 < v2));
        }
        // v1 > v2 iff v2 -> v1
        if v1 > v2 {
            assert_eq!(v1.hb_cmp(&v2), HappenedBeforeOrdering::After);
        } else {
            assert_ne!(v1.hb_cmp(&v2), HappenedBeforeOrdering::After);
        }
        if v1.hb_cmp(&v2) == HappenedBeforeOrdering::After {
            assert!(v1 > v2);
        } else {
            assert!(!(v1 > v2));
        }

        // Antisymmetry.
        assert_eq!(v1.hb_cmp(&v2), v2.hb_cmp(&v1).reverse());
    }

    proptest! {
        #[test]
        fn single_override_version_compare(v in 0u64..LARGE_VERSION) {
            single_override_version_compare_impl(v)
        }
    }
    fn single_override_version_compare_impl(v: u64) {
        let ov = OverrideVersion::with_next_version(v, 2usize);
        #[allow(clippy::eq_op)]
        {
            assert_eq!(ov, ov);
        }
        assert_eq!(ov.hb_cmp(&ov), HappenedBeforeOrdering::Equal);
    }

    proptest! {
        #[test]
        fn two_override_version_compare(v1 in 0u64..u64::MAX, v2 in 0u64..LARGE_VERSION) {
            two_override_version_compare_impl(v1, v2)
        }
    }
    fn two_override_version_compare_impl(v1: u64, v2: u64) {
        let v1_id1 = OverrideVersion::with_next_version(v1, 2usize);
        let v2_id1 = OverrideVersion::with_next_version(v2, 2usize);

        let number_compare: HappenedBeforeOrdering = v1.cmp(&v2).into();
        assert_eq!(v1_id1.hb_cmp(&v2_id1), number_compare);

        let v2_id2 = OverrideVersion::with_next_version(v2, 5usize);

        let compare_result = v1_id1.hb_cmp(&v2_id2);
        if v1 < v2 - 1 {
            assert_eq!(compare_result, HappenedBeforeOrdering::Before);
        } else if v2 < v1 - 1 {
            assert_eq!(compare_result, HappenedBeforeOrdering::After);
        } else {
            assert_eq!(compare_result, HappenedBeforeOrdering::Concurrent);
        }
        // println!("v1={v1}, v2={v2}, v1_id1={v1_id1}, v2_id1={v2_id1}, v2_id2={v2_id2}");
    }
}

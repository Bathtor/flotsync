use core::fmt;
use std::{collections::BTreeMap, num::NonZeroUsize};

use itertools::Itertools;

use super::VersionVector;
use crate::{
    member::{GroupMembership, Identifier},
    versions::{HappenedBeforeOrd, HappenedBeforeOrdering},
};

/// A [[versionVector]] together with the corresponding group-membership information.
///
/// Aligned such that the version at position `p` belongs to the group member at index `p`.
#[derive(Clone, Debug)]
pub struct GroupVersionVector<G> {
    // Fields are read-only so the sizes stay in sync.
    group_members: G,
    versions: VersionVector,
}

impl<G> GroupVersionVector<G>
where
    G: GroupMembership,
{
    /// Create a new instance of [[GroupVersionVector]] if the arguments match in length.
    ///
    /// Otherwise return the arguments unchanged in the `Err` variant.
    pub fn new_checked(
        group_members: G,
        versions: VersionVector,
    ) -> Result<Self, (G, VersionVector)> {
        if group_members.len() == versions.num_members().get() {
            Ok(Self {
                group_members,
                versions,
            })
        } else {
            // Just return the arguments.
            Err((group_members, versions))
        }
    }

    /// Create a new instance of [[GroupVersionVector]].
    ///
    /// Panics if the arguments do not match in length.
    pub fn new(group_members: G, versions: VersionVector) -> Self {
        Self::new_checked(group_members, versions).unwrap_or_else(|e| {
            panic!("Require matching group size, but there were {} group members compared to a length {} version vector.",
            e.0.len(),
            e.1.num_members());
        })
    }

    pub fn group_members(&self) -> &G {
        &self.group_members
    }

    pub fn versions(&self) -> &VersionVector {
        &self.versions
    }

    /// Returns the size of the group and vector.
    pub fn len(&self) -> NonZeroUsize {
        self.versions.num_members()
    }

    pub fn format_line_by_line(&self) -> GroupVersionVectorLineByLineDisplay<'_, G> {
        GroupVersionVectorLineByLineDisplay(self)
    }

    /// Iterate over group members and their associated versions.
    pub fn iter(&self) -> impl Iterator<Item = (&Identifier, u64)> {
        self.group_members.iter().zip(self.versions.iter())
    }

    /// Returns for each group member in `other` that is ahead of the view in `self`,
    /// which versions are missing to bring `self` up to at least `other`.
    ///
    /// # Panics
    ///
    /// - If `self` and `other` do not have the same length.
    ///
    /// Note that this is *not* the same as the difference between the two vectors!
    /// Versions where `self` is already up-to-date or ahead are ignored here.
    ///
    /// # Example
    ///
    /// - self = <a -> 5, b -> 3, c -> 1>
    /// - other = <a -> 5, b -> 1, c -> 4>
    /// - result = <c -> [2, 3, 4]>
    pub fn missing_to<'a, O>(
        &self,
        other: &'a GroupVersionVector<O>,
    ) -> BTreeMap<&'a Identifier, Vec<u64>>
    where
        O: GroupMembership,
    {
        assert!(
            self.len() == other.len(),
            "Cannot compare version vectors of different lengths."
        );
        debug_assert!(
            self.versions.hb_cmp(&other.versions) != HappenedBeforeOrdering::After,
            "It's probably not intended to invoke a.missing_to(b) when b < a. a={self}, b={other}"
        );

        // Basically (current, updated] as a Vec.
        fn missing_versions_between(current: u64, updated: u64) -> Vec<u64> {
            (current..=updated).skip(1).collect_vec()
        }

        let mut result = BTreeMap::new();
        match (&self.versions, &other.versions) {
            (VersionVector::Full(_), _) | (_, VersionVector::Full(_)) => {
                for (self_version, (other_id, other_version)) in
                    self.versions.iter().zip(other.iter())
                {
                    if self_version < other_version {
                        result.insert(
                            other_id,
                            missing_versions_between(self_version, other_version),
                        );
                    }
                }
            }
            (
                VersionVector::Override {
                    version: self_version,
                    ..
                },
                VersionVector::Override {
                    version: other_version,
                    ..
                },
            ) => {
                // Lots of possible cases...just do the group level, and then apply the overrides.
                if self_version.group_version() < other_version.group_version() {
                    let missing: Vec<u64> = missing_versions_between(
                        self_version.group_version(),
                        other_version.group_version(),
                    );
                    result = other
                        .group_members
                        .iter()
                        .map(|id| (id, missing.clone()))
                        .collect();
                }
                if self_version.override_position == other_version.override_position {
                    if self_version.override_version() < other_version.override_version() {
                        // Update the override member with the shorter version sequence.
                        let missing: Vec<u64> = missing_versions_between(
                            self_version.override_version(),
                            other_version.override_version(),
                        );
                        result.insert(
                            &other.group_members[other_version.override_position],
                            missing,
                        );
                    } else {
                        // self_override >= other_override, so just remove it.
                        result.remove(&other.group_members[other_version.override_position]);
                    }
                } else {
                    // Different override positions.

                    // Check self_override
                    if self_version.override_version() < other_version.group_version() {
                        // Update the override member with the shorter version sequence.
                        let missing: Vec<u64> = missing_versions_between(
                            self_version.override_version(),
                            other_version.group_version(),
                        );
                        result.insert(
                            &other.group_members[self_version.override_position],
                            missing,
                        );
                    } else {
                        // self_override >= other_group, so just remove it.
                        result.remove(&other.group_members[self_version.override_position]);
                    }

                    // Check other_override
                    if self_version.group_version() < other_version.override_version() {
                        // Update the override member with the longer version sequence.
                        let missing: Vec<u64> = missing_versions_between(
                            self_version.group_version(),
                            other_version.override_version(),
                        );
                        result.insert(
                            &other.group_members[other_version.override_position],
                            missing,
                        );
                    } else {
                        // self_group >= other_override, so result should be empty.
                        debug_assert!(
                            result.is_empty(),
                            "Nothing should have been added when a's group is greater or equal to b's override. a={self}, b={other}, result={result:#?}"
                        );
                    }
                }
            }
            (
                VersionVector::Override {
                    version: self_version,
                    ..
                },
                VersionVector::Synced {
                    version: other_version,
                    ..
                },
            ) => {
                // Possible cases:
                // - group < override < other_version
                // - group < other_version <= override
                if self_version.group_version() < *other_version {
                    let missing: Vec<u64> =
                        missing_versions_between(self_version.group_version(), *other_version);
                    result = other
                        .group_members
                        .iter()
                        .map(|id| (id, missing.clone()))
                        .collect();
                    if self_version.override_version() < *other_version {
                        // Update the override member with the shorter version sequence.
                        let missing: Vec<u64> = missing_versions_between(
                            self_version.override_version(),
                            *other_version,
                        );
                        result.insert(
                            &other.group_members[self_version.override_position],
                            missing,
                        );
                    } else {
                        // other_version <= override
                        // Just remove the override member from the result completely.
                        result.remove(&other.group_members[self_version.override_position]);
                    }
                }
            }
            (
                VersionVector::Synced {
                    version: self_version,
                    ..
                },
                VersionVector::Override {
                    version: other_version,
                    ..
                },
            ) => {
                if *self_version < other_version.group_version() {
                    let missing: Vec<u64> =
                        missing_versions_between(*self_version, other_version.group_version());
                    result = other
                        .group_members
                        .iter()
                        .map(|id| (id, missing.clone()))
                        .collect();
                }
                if *self_version < other_version.override_version() {
                    let missing =
                        missing_versions_between(*self_version, other_version.override_version());
                    // We may or may not have already filled this position above, either way, we can just override it.
                    result.insert(
                        &other.group_members[other_version.override_position],
                        missing,
                    );
                }
            }
            (
                VersionVector::Synced {
                    version: self_version,
                    ..
                },
                VersionVector::Synced {
                    version: other_version,
                    ..
                },
            ) => {
                if self_version < other_version {
                    let missing: Vec<u64> = missing_versions_between(*self_version, *other_version);
                    result = other
                        .group_members
                        .iter()
                        .map(|id| (id, missing.clone()))
                        .collect();
                }
            }
        }
        result
    }
}

impl<G> fmt::Display for GroupVersionVector<G>
where
    G: GroupMembership,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries = self
            .group_members
            .iter()
            .zip(self.versions.iter())
            .map(|(id, version)| format!("{id} -> {version}"))
            .join(", ");
        write!(f, "〈{entries}〉")
    }
}

/// A printer wrapper for [[GroupVersionVector]] that displays entries one-per line.
///
/// Use with [[GroupVersionVector::format_line_by_line()]].
pub struct GroupVersionVectorLineByLineDisplay<'a, G>(&'a GroupVersionVector<G>);

impl<'a, G> fmt::Display for GroupVersionVectorLineByLineDisplay<'a, G>
where
    G: GroupMembership,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let entries = self
            .0
            .group_members
            .iter()
            .zip(self.0.versions.iter())
            .map(|(id, version)| format!(" {id} -> {version}"))
            .join(",\n");
        write!(f, "〈\n{entries}\n〉")
    }
}

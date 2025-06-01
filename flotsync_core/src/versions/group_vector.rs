use core::fmt;

use itertools::Itertools;

use super::VersionVector;
use crate::member::GroupMembership;

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

    pub fn format_line_by_line(&self) -> GroupVersionVectorLineByLineDisplay<G> {
        GroupVersionVectorLineByLineDisplay(self)
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

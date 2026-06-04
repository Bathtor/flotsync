use crate::{GroupId, MemberIdentity, MemberIndex, member::TrieMap};
use arc_swap::ArcSwap;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

/// Immutable snapshot of the groups currently hosted locally and their
/// currently known members.
#[derive(Clone, Debug, Default)]
pub struct GroupMemberships {
    /// One immutable member snapshot per locally hosted group.
    groups: HashMap<GroupId, GroupMembers>,
}

impl GroupMemberships {
    /// Create one empty membership snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build one snapshot from the provided full group-to-members mapping.
    pub fn from_groups(groups: impl IntoIterator<Item = (GroupId, GroupMembers)>) -> Self {
        Self {
            groups: groups.into_iter().collect(),
        }
    }

    /// Returns `true` when the given group currently exists in the local
    /// delivery view.
    #[must_use]
    pub fn contains_group(&self, group_id: &GroupId) -> bool {
        self.groups.contains_key(group_id)
    }

    /// Return the currently known members for the given group when present.
    #[must_use]
    pub fn members(&self, group_id: &GroupId) -> Option<&GroupMembers> {
        self.groups.get(group_id)
    }

    /// Return the ids of all locally hosted groups in this snapshot.
    pub fn group_ids(&self) -> impl Iterator<Item = &GroupId> {
        self.groups.keys()
    }

    /// Replace the membership set for one group inside this snapshot.
    pub fn insert(&mut self, group_id: GroupId, members: GroupMembers) -> Option<GroupMembers> {
        self.groups.insert(group_id, members)
    }
}

/// Shared handle to the current local group-membership view.
///
/// Each call to [`snapshot`](Self::snapshot) returns the immutable view that was current at that
/// instant. Later updates may replace the shared view, so callers should treat the returned
/// snapshot as a short-lived consistency boundary and reload it when they need current membership
/// information.
#[derive(Clone, Debug)]
pub struct SharedGroupMemberships {
    /// ArcSwap-backed pointer to the current immutable membership snapshot.
    inner: Arc<ArcSwap<GroupMemberships>>,
}

impl SharedGroupMemberships {
    /// Create one new shared snapshot handle around the provided initial view.
    #[must_use]
    pub fn new(initial: GroupMemberships) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Load the current immutable snapshot.
    ///
    /// The returned snapshot remains valid for as long as the [`Arc`] is held, but it may become
    /// stale as soon as another task replaces the shared membership view.
    #[must_use]
    pub fn snapshot(&self) -> Arc<GroupMemberships> {
        self.inner.load_full()
    }

    /// Replace the full shared snapshot atomically.
    pub fn replace(&self, memberships: GroupMemberships) {
        self.inner.store(Arc::new(memberships));
    }
}

impl Default for SharedGroupMemberships {
    fn default() -> Self {
        Self::new(GroupMemberships::new())
    }
}

/// Construction failures for indexed group member sets.
#[derive(Debug, Snafu)]
pub enum GroupMembersError {
    #[snafu(display(
        "Group member set contains duplicate member {member} in its canonical order."
    ))]
    DuplicateMember { member: MemberIdentity },
    #[snafu(display(
        "Group member set has {member_count} members, which exceeds UpdateId node index capacity."
    ))]
    TooManyMembers { member_count: usize },
}

/// Indexed members for one replication group.
///
/// The trie remains the authoritative membership representation. The value part
/// stores the fixed canonical member index from the group's bootstrap order so
/// delivery and replication can both query membership and producer positions
/// from one shared snapshot.
#[derive(Clone, Debug)]
pub struct GroupMembers {
    member_indices: TrieMap<MemberIndex>,
}

impl GroupMembers {
    /// Build one single-member group with that member at canonical index `0`.
    ///
    /// # Errors
    ///
    /// See `GroupMembersError` for failure conditions.
    pub fn singleton(member: MemberIdentity) -> Result<Self, GroupMembersError> {
        Self::from_ordered_members([member])
    }

    /// Build one indexed member set from the canonical group member order.
    ///
    /// # Errors
    ///
    /// See `GroupMembersError` for failure conditions.
    ///
    /// # Panics
    ///
    /// Panics if a member index cannot be represented after the group size was already checked
    /// against [`u32::MAX`].
    pub fn from_ordered_members(
        ordered_members: impl IntoIterator<Item = MemberIdentity>,
    ) -> Result<Self, GroupMembersError> {
        let ordered_members: Vec<_> = ordered_members.into_iter().collect();
        if ordered_members.len() > (u32::MAX as usize) {
            return TooManyMembersSnafu {
                member_count: ordered_members.len(),
            }
            .fail();
        }

        let mut member_indices = TrieMap::new();
        for (index, member) in ordered_members.iter().cloned().enumerate() {
            let index = MemberIndex::try_from(index).expect("checked group size above");
            if member_indices.insert(member.clone(), index).is_some() {
                return DuplicateMemberSnafu { member }.fail();
            }
        }
        Ok(Self { member_indices })
    }

    /// Return whether this group currently includes `member`.
    #[must_use]
    pub fn contains(&self, member: &MemberIdentity) -> bool {
        self.member_indices.get(member).is_some()
    }

    /// Return the fixed producer index assigned to `member`, if present.
    #[must_use]
    pub fn member_index(&self, member: &MemberIdentity) -> Option<MemberIndex> {
        self.member_indices.get(member).copied()
    }

    /// Return the member assigned to one canonical group index.
    #[must_use]
    pub fn member_at_index(&self, index: MemberIndex) -> Option<MemberIdentity> {
        for (member, member_index) in &self.member_indices {
            if *member_index == index {
                return Some(member.clone());
            }
        }
        None
    }

    /// Iterate all members currently in this group.
    pub fn iter(&self) -> impl Iterator<Item = MemberIdentity> + '_ {
        self.member_indices.iter_keys()
    }

    /// Return the canonical bootstrap order for this group.
    #[must_use]
    pub fn ordered_members(&self) -> Vec<MemberIdentity> {
        let mut ordered_members: Vec<_> = self
            .member_indices
            .iter()
            .map(|(member, index)| (*index, member.clone()))
            .collect();
        ordered_members.sort_by_key(|(index, _)| *index);
        ordered_members
            .into_iter()
            .map(|(_, member)| member)
            .collect()
    }

    /// Return whether this member set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.member_indices.is_empty()
    }

    /// Return the number of members in this group.
    #[must_use]
    pub fn len(&self) -> usize {
        self.member_indices.len()
    }
}

impl PartialEq for GroupMembers {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.member_indices
            .iter()
            .all(|(member, index)| other.member_index(&member) == Some(*index))
    }
}

impl Eq for GroupMembers {}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;
    use crate::member::Identifier;
    use uuid::Uuid;

    fn member<const N: usize>(segments: [&str; N]) -> MemberIdentity {
        Identifier::from_array(segments)
    }

    #[test]
    fn group_members_preserve_canonical_indices() {
        let alice = member(["alice"]);
        let bob = member(["bob"]);
        let charlie = member(["charlie"]);

        let members =
            GroupMembers::from_ordered_members(vec![bob.clone(), alice.clone(), charlie.clone()])
                .expect("members should build");

        assert_eq!(members.member_index(&bob), Some(MemberIndex::new(0)));
        assert_eq!(members.member_index(&alice), Some(MemberIndex::new(1)));
        assert_eq!(members.member_index(&charlie), Some(MemberIndex::new(2)));
        assert_eq!(members.member_at_index(MemberIndex::new(1)), Some(alice));
    }

    #[test]
    fn group_members_reject_duplicates() {
        let alice = member(["alice"]);

        let error = GroupMembers::from_ordered_members(vec![alice.clone(), alice])
            .expect_err("duplicate member should be rejected");

        assert_matches!(error, GroupMembersError::DuplicateMember { .. });
    }

    #[test]
    fn group_members_distinguish_nested_identities() {
        let bob_phone = member(["bob", "phone"]);
        let bob_tablet = member(["bob", "tablet"]);
        let bob = member(["bob"]);

        let members = GroupMembers::from_ordered_members(vec![
            bob_phone.clone(),
            bob_tablet.clone(),
            bob.clone(),
        ])
        .expect("members should build");

        assert_eq!(members.member_index(&bob_phone), Some(MemberIndex::new(0)));
        assert_eq!(members.member_index(&bob_tablet), Some(MemberIndex::new(1)));
        assert_eq!(members.member_index(&bob), Some(MemberIndex::new(2)));
    }

    #[test]
    fn shared_group_memberships_replace_snapshot_atomically() {
        let alice = member(["alice"]);
        let bob = member(["bob"]);
        let group_id = GroupId(Uuid::from_u128(7));
        let snapshot = GroupMemberships::from_groups([(
            group_id,
            GroupMembers::from_ordered_members(vec![alice.clone(), bob.clone()])
                .expect("members should build"),
        )]);
        let memberships = SharedGroupMemberships::new(snapshot);

        let snapshot = memberships.snapshot();
        let members = snapshot.members(&group_id).expect("group should exist");

        assert_eq!(members.member_index(&alice), Some(MemberIndex::new(0)));
        assert_eq!(members.member_index(&bob), Some(MemberIndex::new(1)));
    }
}

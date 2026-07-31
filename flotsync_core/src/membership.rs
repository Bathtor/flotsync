use crate::{GroupId, MemberIdentity, MemberIndex, member::TrieMap};
use snafu::prelude::*;
use std::sync::Arc;

/// Read-only membership capabilities exposed by one coherent group-state snapshot.
pub trait GroupMemberships: Send + Sync {
    /// Return the currently known members for `group_id` when the group is hosted locally.
    fn members(&self, group_id: &GroupId) -> Option<&GroupMembers>;

    /// Iterate locally hosted groups in an unspecified order.
    fn groups(&self) -> Box<dyn Iterator<Item = (GroupId, &GroupMembers)> + '_>;

    /// Return whether `group_id` currently exists in this membership snapshot.
    fn contains_group(&self, group_id: &GroupId) -> bool {
        self.members(group_id).is_some()
    }
}

/// Shared source of atomically replaceable immutable membership snapshots.
pub trait SharedGroupMemberships: Send + Sync {
    /// Load the snapshot current at this instant.
    ///
    /// The returned snapshot remains valid while its [`Arc`] is held, but may become stale as soon
    /// as the shared source publishes a replacement.
    fn snapshot(&self) -> Arc<dyn GroupMemberships>;
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
/// The trie is the authoritative representation. Its values retain the canonical bootstrap
/// indices so callers can reconstruct that order when they explicitly require it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMembers {
    /// Direct identity lookup whose values form a permutation of `0..member_indices.len()`.
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
    pub fn from_ordered_members(
        ordered_members: impl IntoIterator<Item = MemberIdentity>,
    ) -> Result<Self, GroupMembersError> {
        let mut member_indices = TrieMap::new();
        for (index, member) in ordered_members.into_iter().enumerate() {
            let Ok(index) = MemberIndex::try_from(index) else {
                return TooManyMembersSnafu {
                    member_count: index + 1,
                }
                .fail();
            };
            if member_indices
                .insert(member.clone().into_identifier(), index)
                .is_some()
            {
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
        let mut entries = self.member_indices.entries();
        while let Some((member, member_index)) = entries.next() {
            if *member_index == index {
                return Some(MemberIdentity::from(member.to_owned()));
            }
        }
        None
    }

    /// Iterate all members currently in this group in an unspecified order.
    pub fn iter(&self) -> impl Iterator<Item = MemberIdentity> + '_ {
        self.member_indices.owned_keys().map(MemberIdentity::from)
    }

    /// Return the canonical bootstrap order for this group.
    // Panics here only indicate a broken private `GroupMembers` representation invariant.
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn ordered_members(&self) -> Vec<MemberIdentity> {
        let num_members = self.member_indices.len();
        let mut ordered_members = Vec::with_capacity(num_members);

        let raw_storage = &mut ordered_members.spare_capacity_mut()[..num_members];
        let mut members_added = 0usize;
        for (member, index) in self.member_indices.owned_entries() {
            let slot = raw_storage
                .get_mut(index.as_usize())
                .expect("Member indices should not exceed number of members");
            slot.write(MemberIdentity::from(member));
            members_added += 1;
        }

        assert_eq!(members_added, num_members);
        // SAFETY: `from_ordered_members` is the only constructor and assigns every unique trie key
        // exactly one unique index in `0..num_members`. `GroupMembers` exposes no mutation that can
        // invalidate this permutation, and `owned_entries` visits every trie value exactly once.
        // The loop therefore initialised every slot in the logical prefix exactly once, while
        // `Vec::with_capacity(num_members)` guarantees sufficient capacity for that prefix.
        unsafe {
            ordered_members.set_len(num_members);
        }

        ordered_members
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

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;
    fn member<const N: usize>(segments: [&str; N]) -> MemberIdentity {
        MemberIdentity::from_array(segments)
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
        assert_eq!(
            members.member_at_index(MemberIndex::new(1)),
            Some(alice.clone())
        );
        assert_eq!(members.ordered_members(), vec![bob, alice, charlie]);
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
    fn group_members_compare_canonical_member_indices() {
        let alice = member(["alice"]);
        let bob = member(["bob"]);
        let first = GroupMembers::from_ordered_members([alice.clone(), bob.clone()])
            .expect("first member set should build");
        let equal = GroupMembers::from_ordered_members([alice.clone(), bob.clone()])
            .expect("equal member set should build");
        let reordered = GroupMembers::from_ordered_members([bob, alice])
            .expect("reordered member set should build");

        assert_eq!(first, equal);
        assert_ne!(first, reordered);
    }
}

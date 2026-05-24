//! Application-facing replication runtime and API contracts.
use arc_swap::ArcSwap;
use flotsync_core::member::TrieMap;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

/// Highest producer/group version accepted by the replication runtime.
///
/// The protocol reserves `u64::MAX` as an exhaustion sentinel. Decode paths
/// reject it before runtime code can derive catch-up intervals from the value.
pub const MAX_VERSION_VALUE: u64 = u64::MAX - 1;

pub mod api;
pub mod delivery;
pub mod runtime;
pub mod security_provisioning;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use api::*;
pub use runtime::{load_replication_runtime, load_replication_runtime_with_runtime_config_toml};
pub use security_provisioning::{
    ProvisionSecurityError,
    ProvisionedReplicationSecurity,
    prepare_initial_group_security_material,
    provision_replication_security,
    validate_initial_group_security_material,
};
pub use store::SqliteReplicationStore;

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

/// Shared snapshot handle used by ingress and semantic delivery.
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
        self.ordered_members()
            .into_iter()
            .nth(index.as_u32() as usize)
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
    use flotsync_core::member::Identifier;
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
                .expect("group members should build");

        assert_eq!(members.member_index(&bob), Some(MemberIndex::new(0)));
        assert_eq!(members.member_index(&alice), Some(MemberIndex::new(1)));
        assert_eq!(members.member_index(&charlie), Some(MemberIndex::new(2)));
        assert_eq!(members.member_at_index(MemberIndex::new(1)), Some(alice));
        assert!(members.contains(&bob));
    }

    #[test]
    fn group_members_reject_duplicate_members() {
        let alice = member(["alice"]);

        let error = GroupMembers::from_ordered_members(vec![alice.clone(), alice])
            .expect_err("duplicate members must be rejected");

        assert_matches!(error, GroupMembersError::DuplicateMember { .. });
    }

    #[test]
    fn group_members_preserve_multi_segment_member_ids() {
        let alice_phone = member(["alice", "phone"]);
        let alice_laptop = member(["alice", "laptop"]);
        let bob_tablet = member(["bob", "tablet"]);

        let members = GroupMembers::from_ordered_members(vec![
            alice_phone.clone(),
            bob_tablet.clone(),
            alice_laptop.clone(),
        ])
        .expect("group members should build");

        assert_eq!(
            members.member_index(&alice_phone),
            Some(MemberIndex::new(0))
        );
        assert_eq!(members.member_index(&bob_tablet), Some(MemberIndex::new(1)));
        assert_eq!(
            members.member_index(&alice_laptop),
            Some(MemberIndex::new(2))
        );
        assert_eq!(
            members.ordered_members(),
            vec![alice_phone, bob_tablet, alice_laptop]
        );
    }

    #[test]
    fn group_memberships_store_indexed_members_authoritatively() {
        let group_id = GroupId(Uuid::from_u128(3));
        let alice = member(["alice"]);
        let bob = member(["bob"]);
        let snapshot = GroupMemberships::from_groups([(
            group_id,
            GroupMembers::from_ordered_members(vec![alice.clone(), bob.clone()])
                .expect("group members should build"),
        )]);
        let members = snapshot
            .members(&group_id)
            .expect("group must exist in delivery snapshot");

        assert!(snapshot.contains_group(&group_id));
        assert!(members.contains(&alice));
        assert!(members.contains(&bob));
        assert_eq!(members.member_index(&alice), Some(MemberIndex::new(0)));
        assert_eq!(members.member_index(&bob), Some(MemberIndex::new(1)));
    }
}

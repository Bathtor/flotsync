//! Application-facing replication runtime and API contracts.
use arc_swap::ArcSwap;
use flotsync_core::member::TrieSet;
use std::{collections::HashMap, sync::Arc};

pub mod api;
pub mod delivery;
pub mod runtime;

pub use api::*;
pub use runtime::load_replication_runtime;

/// Immutable snapshot of the groups currently hosted locally and their
/// currently known members.
#[derive(Clone, Debug, Default)]
pub struct GroupMemberships {
    /// One immutable member snapshot per locally hosted group.
    groups: HashMap<GroupId, TrieSet>,
}

impl GroupMemberships {
    /// Create one empty membership snapshot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build one snapshot from the provided full group-to-members mapping.
    pub fn from_groups(groups: impl IntoIterator<Item = (GroupId, TrieSet)>) -> Self {
        Self {
            groups: groups.into_iter().collect(),
        }
    }

    /// Returns `true` when the given group currently exists in the local
    /// delivery view.
    pub fn contains_group(&self, group_id: &GroupId) -> bool {
        self.groups.contains_key(group_id)
    }

    /// Return the currently known members for the given group when present.
    pub fn members(&self, group_id: &GroupId) -> Option<&TrieSet> {
        self.groups.get(group_id)
    }

    /// Replace the membership set for one group inside this snapshot.
    pub fn insert(&mut self, group_id: GroupId, members: TrieSet) -> Option<TrieSet> {
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
    pub fn new(initial: GroupMemberships) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Load the current immutable snapshot.
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

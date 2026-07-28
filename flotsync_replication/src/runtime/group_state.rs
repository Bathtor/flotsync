//! Authoritative immutable runtime group state and its restricted read-only projections.

use super::{
    errors::{DuplicateStoredGroupSnafu, GroupInstallError},
    in_memory::LoadedGroupMeta,
};
use crate::{
    __seal_replication_group_snapshot,
    __seal_replication_group_view,
    api::{
        GroupSchema,
        ReplicationGroupLifecycle,
        ReplicationGroupRecord,
        ReplicationGroupSnapshot,
        ReplicationGroupView,
    },
};
use arc_swap::ArcSwap;
use flotsync_core::{
    GroupId,
    MemberIdentity,
    membership::{GroupMembers, GroupMemberships, SharedGroupMemberships},
};
use sealed::sealed;
use std::{collections::HashMap, sync::Arc};

/// Project complete test records through the production runtime snapshot implementation.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn application_snapshot_from_records(
    local_member: &MemberIdentity,
    records: impl IntoIterator<Item = ReplicationGroupRecord>,
) -> Result<Arc<dyn ReplicationGroupSnapshot>, GroupInstallError> {
    let snapshot = RuntimeGroupStateSnapshot::from_records(local_member, records)?;
    Ok(Arc::new(snapshot))
}

/// One immutable projection of every active group known to the local runtime.
#[derive(Default)]
pub(super) struct RuntimeGroupStateSnapshot {
    /// Active groups keyed by their stable identifiers.
    groups: HashMap<GroupId, RuntimeGroupState>,
}

impl RuntimeGroupStateSnapshot {
    /// Create one empty runtime snapshot.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Project active storage records into one validated runtime snapshot.
    pub(super) fn from_records(
        local_member: &MemberIdentity,
        records: impl IntoIterator<Item = ReplicationGroupRecord>,
    ) -> Result<Self, GroupInstallError> {
        let mut snapshot = Self::new();
        for record in records {
            snapshot.insert_record(local_member, record)?;
        }
        Ok(snapshot)
    }

    /// Insert one validated active record, rejecting duplicate group identifiers.
    pub(super) fn insert_record(
        &mut self,
        local_member: &MemberIdentity,
        record: ReplicationGroupRecord,
    ) -> Result<(), GroupInstallError> {
        let group_id = record.group_id;
        if self.groups.contains_key(&group_id) {
            return DuplicateStoredGroupSnafu { group_id }.fail();
        }
        let group = RuntimeGroupState::from_record(local_member, record)?;
        self.groups.insert(group_id, group);
        Ok(())
    }
}

impl GroupMemberships for RuntimeGroupStateSnapshot {
    fn members(&self, group_id: &GroupId) -> Option<&GroupMembers> {
        self.groups.get(group_id).map(|group| &group.members)
    }

    fn groups(&self) -> Box<dyn Iterator<Item = (GroupId, &GroupMembers)> + '_> {
        Box::new(
            self.groups
                .values()
                .map(|group| (group.group_id, &group.members)),
        )
    }
}

#[sealed]
impl ReplicationGroupSnapshot for RuntimeGroupStateSnapshot {
    fn group(&self, group_id: &GroupId) -> Option<&dyn ReplicationGroupView> {
        self.groups
            .get(group_id)
            .map(|group| group as &dyn ReplicationGroupView)
    }

    fn groups(&self) -> Box<dyn Iterator<Item = &dyn ReplicationGroupView> + '_> {
        Box::new(
            self.groups
                .values()
                .map(|group| group as &dyn ReplicationGroupView),
        )
    }

    fn readable_groups(&self) -> Box<dyn Iterator<Item = &dyn ReplicationGroupView> + '_> {
        Box::new(
            self.groups
                .values()
                .filter(|group| group.is_readable())
                .map(|group| group as &dyn ReplicationGroupView),
        )
    }
}

/// Single atomically replaceable owner of the current runtime group snapshot.
pub(super) struct SharedGroupState {
    /// Current immutable state published to every restricted reader.
    current: ArcSwap<RuntimeGroupStateSnapshot>,
}

impl SharedGroupState {
    /// Create one empty group-state owner for runtime startup.
    pub(super) fn new() -> Self {
        Self {
            current: ArcSwap::from_pointee(RuntimeGroupStateSnapshot::default()),
        }
    }

    /// Replace the complete runtime group state atomically.
    pub(super) fn replace(&self, snapshot: RuntimeGroupStateSnapshot) {
        self.current.store(Arc::new(snapshot));
    }

    /// Load the application-facing view current at this instant.
    pub(super) fn application_snapshot(&self) -> Arc<dyn ReplicationGroupSnapshot> {
        self.current.load_full()
    }
}

impl SharedGroupMemberships for SharedGroupState {
    fn snapshot(&self) -> Arc<dyn GroupMemberships> {
        self.current.load_full()
    }
}

/// Runtime-only group entry backing both membership and application views.
struct RuntimeGroupState {
    /// Stable group identifier.
    group_id: GroupId,
    /// Optional application-facing group name.
    group_name: Option<String>,
    /// Canonically indexed member identities.
    members: GroupMembers,
    /// Dataset schema fixed for this group.
    group_schema: GroupSchema,
    /// Current application-access and replication lifecycle.
    lifecycle: ReplicationGroupLifecycle,
}

impl RuntimeGroupState {
    /// Project one validated active storage record into the restricted runtime representation.
    fn from_record(
        local_member: &MemberIdentity,
        record: ReplicationGroupRecord,
    ) -> Result<Self, GroupInstallError> {
        let members = LoadedGroupMeta::validated_members_from_replication_group_record(
            local_member,
            &record,
        )?;
        Ok(Self {
            group_id: record.group_id,
            group_name: record.group_name,
            members,
            group_schema: record.group_schema,
            lifecycle: record.lifecycle,
        })
    }
}

#[sealed]
impl ReplicationGroupView for RuntimeGroupState {
    fn group_id(&self) -> GroupId {
        self.group_id
    }

    fn group_name(&self) -> Option<&str> {
        self.group_name.as_deref()
    }

    fn members(&self) -> Box<dyn Iterator<Item = MemberIdentity> + '_> {
        Box::new(self.members.iter())
    }

    fn group_schema(&self) -> &GroupSchema {
        &self.group_schema
    }

    fn lifecycle(&self) -> &ReplicationGroupLifecycle {
        &self.lifecycle
    }
}

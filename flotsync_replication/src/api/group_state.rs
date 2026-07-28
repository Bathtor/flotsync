//! Restricted application views over one immutable runtime group-state snapshot.

use super::{GroupId, GroupSchema, MemberIdentity, ReplicationGroupLifecycle};
use sealed::sealed;

/// Read-only application capabilities for one active replication group.
///
/// Values are borrowed from a [`ReplicationGroupSnapshot`]. Applications cannot construct or
/// mutate implementations of this sealed trait.
#[sealed(pub(crate))]
pub trait ReplicationGroupView: Send + Sync {
    /// Return the stable replication-group identifier.
    fn group_id(&self) -> GroupId;

    /// Return the optional application-facing group name.
    fn group_name(&self) -> Option<&str>;

    /// Iterate member identities in an unspecified order.
    fn members(&self) -> Box<dyn Iterator<Item = MemberIdentity> + '_>;

    /// Return the dataset schema fixed for this group.
    fn group_schema(&self) -> &GroupSchema;

    /// Return the current application-access and replication lifecycle.
    fn lifecycle(&self) -> &ReplicationGroupLifecycle;

    /// Return whether application code may read this group.
    fn is_readable(&self) -> bool {
        self.lifecycle().is_readable()
    }

    /// Return whether application code may publish changes to this group.
    fn is_writable(&self) -> bool {
        self.lifecycle().is_writable()
    }
}

/// Immutable application view over the runtime groups active in local storage.
///
/// A snapshot remains internally consistent while its [`std::sync::Arc`] is retained. A later
/// runtime state transition may publish a replacement, so callers should acquire a new snapshot
/// when they need current group metadata.
#[sealed(pub(crate))]
pub trait ReplicationGroupSnapshot: Send + Sync {
    /// Return one active group when it exists in this snapshot.
    fn group(&self, group_id: &GroupId) -> Option<&dyn ReplicationGroupView>;

    /// Iterate active groups in an unspecified order.
    fn groups(&self) -> Box<dyn Iterator<Item = &dyn ReplicationGroupView> + '_>;

    /// Iterate application-readable groups in an unspecified order.
    fn readable_groups(&self) -> Box<dyn Iterator<Item = &dyn ReplicationGroupView> + '_> {
        Box::new(self.groups().filter(|group| group.is_readable()))
    }
}

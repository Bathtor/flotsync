//! Stable checklist presentation for peer-route diagnostics.

use flotsync_core::MemberIdentity;
use flotsync_replication::{
    ConfiguredRouteMembers,
    ReplicationGroupSnapshot,
    RouteEstablishmentDiagnostics,
};
use std::fmt;

/// Checklist-specific diagnostic report with stable ordering and group annotations.
pub(super) struct ChecklistPeerDiagnostics<'a> {
    /// Canonical snapshot sorted only for this presentation boundary.
    snapshot: RouteEstablishmentDiagnostics,
    /// Distinct identified members in stable order for group annotations.
    identified_members: Vec<MemberIdentity>,
    /// Application-readable group registry used for membership counts.
    groups: &'a dyn ReplicationGroupSnapshot,
}

impl<'a> ChecklistPeerDiagnostics<'a> {
    /// Build one stable checklist presentation from an owned diagnostic snapshot.
    pub(super) fn new(
        mut snapshot: RouteEstablishmentDiagnostics,
        groups: &'a dyn ReplicationGroupSnapshot,
    ) -> Self {
        sort_snapshot_for_presentation(&mut snapshot);
        let mut identified_members = snapshot
            .routes
            .iter()
            .flat_map(|route| route.identified_members.iter().cloned())
            .collect::<Vec<_>>();
        identified_members.sort();
        identified_members.dedup();
        Self {
            snapshot,
            identified_members,
            groups,
        }
    }
}

impl fmt::Display for ChecklistPeerDiagnostics<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.snapshot)?;
        if self.identified_members.is_empty() {
            Ok(())
        } else {
            writeln!(formatter)?;
            writeln!(formatter, "identified member groups:")?;
            for (index, member) in self.identified_members.iter().enumerate() {
                let shared_groups = shared_group_count(self.groups, member);
                if index + 1 == self.identified_members.len() {
                    write!(formatter, "  {member}: shared groups={shared_groups}")?;
                } else {
                    writeln!(formatter, "  {member}: shared groups={shared_groups}")?;
                }
            }
            Ok(())
        }
    }
}

/// Sort one owned snapshot immediately before checklist presentation.
fn sort_snapshot_for_presentation(snapshot: &mut RouteEstablishmentDiagnostics) {
    snapshot.advertised_endpoints.sort();
    snapshot.routes.sort_by_key(|route| route.route.udp_addr());
    for route in &mut snapshot.routes {
        if let Some(ConfiguredRouteMembers::Members(members)) = &mut route.configured_members {
            members.sort();
        }
        route.identified_members.sort();
        route.reachable_members.sort();
    }
}

/// Count currently readable groups containing `member`.
fn shared_group_count(groups: &dyn ReplicationGroupSnapshot, member: &MemberIdentity) -> usize {
    groups
        .readable_groups()
        .filter(|group| group.members().any(|candidate| candidate == *member))
        .count()
}

#[cfg(test)]
mod tests;

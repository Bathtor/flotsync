//! Classification and deterministic ordering of pending startup work.

use crate::api::{ApplicationReadToken, GroupReadToken, GroupSchema};
use flotsync_core::{GroupId, versions::VersionVector};
use std::{
    cmp::Ordering,
    collections::{HashSet, VecDeque},
    sync::Arc,
};

/// Plan all group entries after selecting the application-token mode once.
pub(super) fn plan_group_synchronisation(
    readable_groups: Vec<ReadableGroupSynchronisation>,
    readable_group_ids: &HashSet<GroupId>,
    application_read_token: Option<&ApplicationReadToken>,
) -> VecDeque<PendingGroupSynchronisation> {
    match application_read_token {
        None => readable_groups
            .into_iter()
            .map(ReadableGroupSynchronisation::synchronise_as_snapshot)
            .collect(),
        Some(application_read_token) => {
            let mut planned = VecDeque::with_capacity(readable_groups.len());
            for group in readable_groups {
                match plan_readable_group(group, application_read_token) {
                    ReadableGroupPlanning::Exact => {
                        // The supplied position already represents this group.
                    }
                    ReadableGroupPlanning::Synchronise(group) => planned.push_back(group),
                }
            }
            for (group_id, _) in application_read_token.group_versions() {
                if !readable_group_ids.contains(group_id) {
                    planned.push_back(PendingGroupSynchronisation::Retired {
                        group_id: *group_id,
                    });
                }
            }
            planned
        }
    }
}

/// Complete readable-group input shared by snapshot and incremental planning.
pub(super) struct ReadableGroupSynchronisation {
    /// Group affected by this entry.
    pub(super) group_id: GroupId,
    /// Current position reached after applying the entry.
    pub(super) read_token: GroupReadToken,
    /// Authoritative schemas used for snapshot or incremental projection.
    pub(super) group_schema: Arc<GroupSchema>,
}

impl ReadableGroupSynchronisation {
    /// Plan this group as a complete snapshot.
    fn synchronise_as_snapshot(self) -> PendingGroupSynchronisation {
        PendingGroupSynchronisation::Snapshot(self)
    }

    /// Plan this group as a candidate incremental reconciliation.
    fn synchronise_as_incremental(
        self,
        from_versions: VersionVector,
    ) -> PendingGroupSynchronisation {
        PendingGroupSynchronisation::Incremental(IncrementalCandidate {
            group: self,
            from_versions,
        })
    }
}

/// Unresolved work waiting in deterministic group order.
pub(super) enum PendingGroupSynchronisation {
    /// Complete snapshot whose store rows have not yet been scanned.
    Snapshot(ReadableGroupSynchronisation),
    /// Incremental candidate whose retained history has not yet been inspected.
    Incremental(IncrementalCandidate),
    /// Supplied group absent from the readable store cut.
    Retired { group_id: GroupId },
}

impl PendingGroupSynchronisation {
    /// Return the deterministic group-order key for this pending entry.
    pub(super) const fn group_id(&self) -> GroupId {
        match self {
            Self::Snapshot(group) => group.group_id,
            Self::Incremental(candidate) => candidate.group.group_id,
            Self::Retired { group_id } => *group_id,
        }
    }
}

/// Owned inputs needed while asynchronously preparing one group.
pub(super) struct IncrementalCandidate {
    /// Current readable group targeted by reconciliation.
    pub(super) group: ReadableGroupSynchronisation,
    /// Application position from which changes would begin.
    pub(super) from_versions: VersionVector,
}

/// Result of comparing one readable group with a supplied application token.
enum ReadableGroupPlanning {
    /// The supplied position exactly represents this readable group, so no entry is needed.
    Exact,
    /// This group needs the contained snapshot or incremental work.
    Synchronise(PendingGroupSynchronisation),
}

/// Classify one readable group relative to a supplied application position.
fn plan_readable_group(
    group: ReadableGroupSynchronisation,
    application_read_token: &ApplicationReadToken,
) -> ReadableGroupPlanning {
    if let Some(supplied_versions) = application_read_token.group_version(&group.group_id) {
        match supplied_versions.partial_cmp(group.read_token.version()) {
            Some(Ordering::Equal) => ReadableGroupPlanning::Exact,
            Some(Ordering::Less) => ReadableGroupPlanning::Synchronise(
                group.synchronise_as_incremental(supplied_versions.clone()),
            ),
            Some(Ordering::Greater) | None => {
                ReadableGroupPlanning::Synchronise(group.synchronise_as_snapshot())
            }
        }
    } else {
        ReadableGroupPlanning::Synchronise(group.synchronise_as_snapshot())
    }
}

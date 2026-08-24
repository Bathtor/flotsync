//! Group registry, creation, invitation, and rendering behaviour for the checklist REPL.

use super::{
    repl::{ChecklistRepl, ChecklistSession, PendingGroupInvitation, join_words},
    *,
};
#[cfg(test)]
use flotsync_replication::DataChangeLineage;
use flotsync_replication::{
    GroupInvitationSource,
    InitialSnapshot,
    ReplicationGroupSnapshot,
    ReplicationGroupView,
};
use indoc::formatdoc;

/// Change made while repairing the process-local default from current group lifecycles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultGroupRepair {
    /// No default was selected, or the selected group remains open.
    Unchanged,
    /// A non-writable default was replaced by its first open successor.
    Reassigned {
        /// Group that stopped being a valid default.
        previous_group_id: GroupId,
        /// Open successor selected as the new default.
        successor_group_id: GroupId,
    },
    /// The previous default had no resolvable open successor.
    Cleared {
        /// Group that stopped being a valid default.
        previous_group_id: GroupId,
    },
}

impl ChecklistSession {
    /// Build one session with no inferred default group.
    pub fn new(working_set: ChecklistWorkingSet) -> Self {
        Self {
            default_group: None,
            working_set,
        }
    }

    /// Resolve a group UUID first, otherwise an exact unique display name.
    pub fn resolve_group<'a>(
        groups: &'a dyn ReplicationGroupSnapshot,
        selector: &str,
    ) -> Result<&'a dyn ReplicationGroupView, ReplicatedChecklistError> {
        if let Ok(uuid) = Uuid::parse_str(selector) {
            let group_id = GroupId(uuid);
            return groups
                .group(&group_id)
                .filter(|group| group.is_readable())
                .ok_or_else(|| ReplicatedChecklistError::UnknownGroup {
                    selector: selector.to_owned(),
                });
        }

        let mut matches = groups
            .readable_groups()
            .filter(|group| group.group_name() == Some(selector))
            .collect::<Vec<_>>();
        matches.sort_by_key(|group| group.group_id());
        match matches.as_slice() {
            [group] => Ok(*group),
            [] => Err(ReplicatedChecklistError::UnknownGroup {
                selector: selector.to_owned(),
            }),
            _ => Err(ReplicatedChecklistError::AmbiguousGroupName {
                name: selector.to_owned(),
                candidate_ids: matches.iter().map(|group| group.group_id()).collect(),
            }),
        }
    }

    /// Select one writable group as the session default.
    pub fn set_default(
        &mut self,
        groups: &dyn ReplicationGroupSnapshot,
        selector: &str,
    ) -> Result<GroupId, ReplicatedChecklistError> {
        let group = Self::resolve_group(groups, selector)?;
        if !group.is_writable() {
            return Err(ReplicatedChecklistError::NonWritableDefaultGroup {
                group_id: group.group_id(),
            });
        }
        let group_id = group.group_id();
        self.default_group = Some(group_id);
        Ok(group_id)
    }

    /// Return the current default group record.
    pub fn default_group<'a>(
        &self,
        groups: &'a dyn ReplicationGroupSnapshot,
    ) -> Result<&'a dyn ReplicationGroupView, ReplicatedChecklistError> {
        let group_id = self
            .default_group
            .ok_or(ReplicatedChecklistError::NoDefaultGroup)?;
        groups
            .group(&group_id)
            .filter(|group| group.is_readable())
            .ok_or(ReplicatedChecklistError::UnknownGroup {
                selector: group_id.to_string(),
            })
    }

    /// Return a stable display label, preferring an unambiguous group name.
    pub fn group_label(groups: &dyn ReplicationGroupSnapshot, group_id: GroupId) -> String {
        let Some(group) = groups.group(&group_id).filter(|group| group.is_readable()) else {
            return group_id.to_string();
        };
        let Some(name) = group.group_name() else {
            return group_id.to_string();
        };
        let name_count = groups
            .readable_groups()
            .filter(|candidate| candidate.group_name() == Some(name))
            .count();
        if name_count == 1 {
            name.to_owned()
        } else {
            format!("{name} ({group_id})")
        }
    }

    /// Return the human-readable association for one workspace item.
    pub fn association_label(
        groups: &dyn ReplicationGroupSnapshot,
        association: ChecklistItemAssociation,
    ) -> String {
        match association {
            ChecklistItemAssociation::Local => "local".to_owned(),
            ChecklistItemAssociation::Group(group_id) => Self::group_label(groups, group_id),
        }
    }

    /// Return the canonical unambiguous REPL reference for one item identity.
    pub fn item_reference(
        groups: &dyn ReplicationGroupSnapshot,
        item_id: ChecklistItemId,
    ) -> String {
        let association = match item_id.association {
            ChecklistItemAssociation::Local => "local".to_owned(),
            ChecklistItemAssociation::Group(group_id) => {
                let Some(group) = groups.group(&group_id).filter(|group| group.is_readable())
                else {
                    return format!("{group_id}/{}", item_id.row_key);
                };
                let Some(name) = group.group_name() else {
                    return format!("{group_id}/{}", item_id.row_key);
                };
                let name_is_usable = name != "local"
                    && !name.chars().any(char::is_whitespace)
                    && Uuid::parse_str(name).is_err()
                    && groups
                        .readable_groups()
                        .filter(|candidate| candidate.group_name() == Some(name))
                        .count()
                        == 1;
                if name_is_usable {
                    name.to_owned()
                } else {
                    group_id.to_string()
                }
            }
        };
        format!("{association}/{}", item_id.row_key)
    }

    /// Resolve one list position, bare UUID, or qualified item reference.
    pub fn resolve_item(
        &self,
        groups: &dyn ReplicationGroupSnapshot,
        selector: &ItemSelector,
    ) -> Result<ChecklistItemId, ReplicatedChecklistError> {
        let item_id = match selector {
            ItemSelector::ListIndex(position) => self
                .working_set
                .listed_items()
                .get(position.get() - 1)
                .map(|listed| listed.item_id),
            ItemSelector::RowKey(row_key) => {
                let candidates = self.working_set.item_ids_with_row_key(*row_key);
                match candidates.as_slice() {
                    [item_id] => Some(*item_id),
                    [] => None,
                    _ => {
                        return Err(ReplicatedChecklistError::AmbiguousItemReference {
                            row_key: *row_key,
                            candidates: candidates
                                .into_iter()
                                .map(|item_id| Self::item_reference(groups, item_id))
                                .collect(),
                        });
                    }
                }
            }
            ItemSelector::Qualified {
                association,
                row_key,
            } => {
                let association = match association {
                    ItemAssociationSelector::Local => ChecklistItemAssociation::Local,
                    ItemAssociationSelector::Group(group) => {
                        let group_id = if let Ok(uuid) = Uuid::parse_str(group) {
                            GroupId(uuid)
                        } else {
                            let group = Self::resolve_group(groups, group)?;
                            group.group_id()
                        };
                        ChecklistItemAssociation::Group(group_id)
                    }
                };
                Some(ChecklistItemId {
                    association,
                    row_key: *row_key,
                })
            }
        };
        let Some(item_id) = item_id else {
            return Err(ReplicatedChecklistError::UnknownItemReference {
                selector: selector.clone(),
            });
        };
        if self.working_set.item(item_id).is_none() {
            return Err(ReplicatedChecklistError::UnknownItemReference {
                selector: selector.clone(),
            });
        }
        Ok(item_id)
    }

    /// Resolve one writable real-group target association.
    pub fn resolve_target_association(
        groups: &dyn ReplicationGroupSnapshot,
        selector: &str,
    ) -> Result<ChecklistItemAssociation, ReplicatedChecklistError> {
        let group = Self::resolve_group(groups, selector)?;
        if !group.is_writable() {
            return Err(ReplicatedChecklistError::NonWritableTargetGroup {
                group_id: group.group_id(),
            });
        }
        Ok(ChecklistItemAssociation::Group(group.group_id()))
    }

    /// Ensure a listener batch refers only to groups present in this registry.
    pub fn validate_listener_changes(
        groups: &dyn ReplicationGroupSnapshot,
        changes: &[RowChange],
    ) -> Result<(), ReplicatedChecklistError> {
        for change in changes {
            let group_id = change.row_id().group_id;
            let is_readable = groups
                .group(&group_id)
                .is_some_and(ReplicationGroupView::is_readable);
            if !is_readable {
                return repl_error::UnknownListenerGroupSnafu { group_id }.fail();
            }
        }
        Ok(())
    }

    /// Repair the selected default from one complete operation-local lifecycle snapshot.
    pub fn repair_default_group(
        &mut self,
        groups: &dyn ReplicationGroupSnapshot,
    ) -> DefaultGroupRepair {
        let previous_default = self.default_group;
        let repaired_default =
            previous_default.and_then(|group_id| resolve_open_successor(group_id, groups));
        self.default_group = repaired_default;

        match (previous_default, repaired_default) {
            (None, None) => DefaultGroupRepair::Unchanged,
            (None, Some(group_id)) => {
                unreachable!(
                    "default-group repair cannot select group {group_id} without a previous default"
                )
            }
            (Some(previous_group_id), None) => DefaultGroupRepair::Cleared { previous_group_id },
            (Some(previous_group_id), Some(successor_group_id))
                if previous_group_id == successor_group_id =>
            {
                DefaultGroupRepair::Unchanged
            }
            (Some(previous_group_id), Some(successor_group_id)) => DefaultGroupRepair::Reassigned {
                previous_group_id,
                successor_group_id,
            },
        }
    }
}

impl ChecklistRepl {
    /// Run one group-registry command that never requires an existing default.
    pub async fn handle_group_registry_command(
        &mut self,
        command: ChecklistGroupCommand,
    ) -> Result<(), ReplicatedChecklistError> {
        match command {
            ChecklistGroupCommand::List => self.print_groups()?,
            ChecklistGroupCommand::Create { name } => self.create_group(name).await?,
            ChecklistGroupCommand::Invitations => self.print_invitations(),
            ChecklistGroupCommand::Accept { invitation } => {
                self.accept_invitation(invitation).await?;
            }
            ChecklistGroupCommand::Reject { invitation } => {
                self.reject_invitation(invitation).await?;
            }
            ChecklistGroupCommand::Default { group } => {
                let selector = join_words(group);
                let groups = self
                    .replication
                    .group_state()
                    .context(repl_error::ReplicationSnafu)?;
                let group_id = self.session.set_default(groups.as_ref(), &selector)?;
                println!(
                    "default group: {}",
                    ChecklistSession::group_label(groups.as_ref(), group_id)
                );
            }
            ChecklistGroupCommand::ClearDefault => {
                self.session.default_group = None;
                println!("default group: none");
            }
        }
        Ok(())
    }

    /// Query current group metadata and repair a default invalidated by lifecycle changes.
    pub fn repair_default_group(&mut self) -> Result<(), ReplicatedChecklistError> {
        let groups = self
            .replication
            .group_state()
            .context(repl_error::ReplicationSnafu)?;
        match self.session.repair_default_group(groups.as_ref()) {
            DefaultGroupRepair::Unchanged => {
                // No default is selected, or the selected default remains open;
                // neither case has a selection change to report.
            }
            DefaultGroupRepair::Reassigned {
                previous_group_id,
                successor_group_id,
            } => {
                println!(
                    "default group updated: {previous_group_id} -> {}",
                    ChecklistSession::group_label(groups.as_ref(), successor_group_id)
                );
            }
            DefaultGroupRepair::Cleared { previous_group_id } => {
                println!(
                    "default group cleared: {previous_group_id} has no open successor in local group state"
                );
            }
        }
        Ok(())
    }

    /// Run the interactive named-group creation wizard.
    pub async fn create_group(
        &mut self,
        name: Vec<String>,
    ) -> Result<(), ReplicatedChecklistError> {
        let stdin = io::stdin();
        let mut input = stdin.lock();
        let mut output = io::stdout();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);
        self.create_group_with_dialog(name, &mut dialog).await
    }

    /// Run group creation through an injected dialog for deterministic handler tests.
    pub async fn create_group_with_dialog(
        &mut self,
        name: Vec<String>,
        dialog: &mut ChecklistDialog<'_>,
    ) -> Result<(), ReplicatedChecklistError> {
        let group_name = join_words(name).trim().to_owned();
        let mut known_members = self
            .replication
            .known_member_keys()
            .await
            .context(repl_error::ReplicationSnafu)?;
        sort_known_members_for_display(&mut known_members);
        println!("{known_members}");
        println!("creator (position 0): {}", self.local_member);
        let additional_members = read_additional_group_members(dialog, &self.local_member)?;
        let request = checklist_group_creation_request(
            group_name,
            self.local_member.clone(),
            additional_members,
        );
        print_group_creation_summary(&request);
        if !dialog.confirm("Create this group and send invitations?")? {
            println!("group creation cancelled");
            return Ok(());
        }

        let group_id = self
            .replication
            .create_group(request)
            .await
            .context(repl_error::ReplicationSnafu)?;
        self.repair_default_group()?;
        self.drain_listener_queue()?;
        self.session.working_set.drain_queued_events();
        let groups = self
            .replication
            .group_state()
            .context(repl_error::ReplicationSnafu)?;
        println!(
            "created group: {}",
            ChecklistSession::group_label(groups.as_ref(), group_id)
        );
        Ok(())
    }

    /// Drain newly delivered invitations into the stable REPL-side pending list.
    pub fn drain_invitation_queue(&mut self) -> Result<(), ReplicatedChecklistError> {
        while let Some(invitation) = self.receive_invitation()? {
            self.pending_invitations.push(invitation);
        }
        Ok(())
    }

    /// Return one queued invitation, or `None` when the listener queue is currently empty.
    pub fn receive_invitation(
        &self,
    ) -> Result<Option<PendingGroupInvitation>, ReplicatedChecklistError> {
        match self.invitation_receiver.try_recv() {
            Ok(invitation) => Ok(Some(invitation)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => repl_error::ListenerQueueClosedSnafu.fail(),
        }
    }

    /// Print every pending invitation with the information needed for a decision.
    pub fn print_invitations(&self) {
        if self.pending_invitations.is_empty() {
            println!("group invitations: none");
            return;
        }
        for (index, pending) in self.pending_invitations.iter().enumerate() {
            print_group_invitation(
                NonZeroUsize::new(index + 1).expect("display positions start at one"),
                pending,
            );
        }
    }

    /// Accept and activate one pending invitation through its one-shot responder.
    pub async fn accept_invitation(
        &mut self,
        position: NonZeroUsize,
    ) -> Result<(), ReplicatedChecklistError> {
        let pending = self.take_invitation(position)?;
        let group_id = pending.invitation.group_id;
        pending
            .respond
            .accept()
            .await
            .context(repl_error::ReplicationSnafu)?;
        self.repair_default_group()?;
        self.drain_listener_queue()?;
        self.session.working_set.drain_queued_events();
        let groups = self
            .replication
            .group_state()
            .context(repl_error::ReplicationSnafu)?;
        println!(
            "accepted group invitation: {}",
            ChecklistSession::group_label(groups.as_ref(), group_id)
        );
        Ok(())
    }

    /// Reject one pending invitation with the explicit user-denial reason.
    pub async fn reject_invitation(
        &mut self,
        position: NonZeroUsize,
    ) -> Result<(), ReplicatedChecklistError> {
        let pending = self.take_invitation(position)?;
        let group_id = pending.invitation.group_id;
        pending
            .respond
            .reject(RejectionReason::UserDenied)
            .await
            .context(repl_error::ReplicationSnafu)?;
        println!("rejected group invitation: {group_id}");
        Ok(())
    }

    /// Remove one displayed invitation so its responder can be consumed exactly once.
    pub fn take_invitation(
        &mut self,
        position: NonZeroUsize,
    ) -> Result<PendingGroupInvitation, ReplicatedChecklistError> {
        let index = position.get() - 1;
        if index >= self.pending_invitations.len() {
            return Err(ReplicatedChecklistError::UnknownGroupInvitation {
                position,
                available: self.pending_invitations.len(),
            });
        }
        Ok(self.pending_invitations.remove(index))
    }

    /// Print the members of the current default group in stable presentation order.
    pub fn print_members(&self) -> Result<(), ReplicatedChecklistError> {
        let groups = self
            .replication
            .group_state()
            .context(repl_error::ReplicationSnafu)?;
        let group = self.session.default_group(groups.as_ref())?;
        println!(
            "group {}",
            ChecklistSession::group_label(groups.as_ref(), group.group_id())
        );
        let mut members = group.members().collect::<Vec<_>>();
        members.sort();
        for (index, member) in members.into_iter().enumerate() {
            let marker = if member == self.local_member {
                " (me)"
            } else {
                ""
            };
            println!("{index}: {member}{marker}");
        }
        Ok(())
    }

    /// Request and print replication summaries from every default-group member.
    pub async fn check_members(&self) -> Result<(), ReplicatedChecklistError> {
        let groups = self
            .replication
            .group_state()
            .context(repl_error::ReplicationSnafu)?;
        let group = self.session.default_group(groups.as_ref())?;
        let group_id = group.group_id();
        let mut members = group.members().collect::<Vec<_>>();
        members.sort();
        println!(
            "group {}",
            ChecklistSession::group_label(groups.as_ref(), group_id)
        );
        let requests = members.iter().cloned().enumerate().map(|(index, member)| {
            let replication = self.replication.clone();
            async move {
                let request = SummaryRequest {
                    group_id,
                    target: member.clone(),
                };
                let result = replication.request_summary(request).await;
                (index, member, result)
            }
        });
        let summaries = join_all(requests).await;
        let local_member = self.local_member.clone();
        for (index, member, result) in summaries {
            let marker = if member == local_member { " (me)" } else { "" };
            match result {
                Ok(summary) => {
                    println!("{index}: {member}{marker} has {}", summary.has_versions);
                }
                Err(error) => {
                    println!("{index}: {member}{marker} unavailable: {error}");
                }
            }
        }
        Ok(())
    }

    /// Print the readable group registry in stable UUID order.
    pub fn print_groups(&self) -> Result<(), ReplicatedChecklistError> {
        let groups = self
            .replication
            .group_state()
            .context(repl_error::ReplicationSnafu)?;
        let mut readable_groups = groups.readable_groups().collect::<Vec<_>>();
        if readable_groups.is_empty() {
            println!("groups: none");
            return Ok(());
        }
        readable_groups.sort_by_key(|group| group.group_id());
        for group in readable_groups {
            let group_id = group.group_id();
            let marker = if self.session.default_group == Some(group_id) {
                " default"
            } else {
                ""
            };
            let name = group.group_name().unwrap_or("<unnamed>");
            println!(
                "{} name={name:?} lifecycle={:?} members={}{}",
                group_id,
                group.lifecycle(),
                group.members().count(),
                marker
            );
        }
        Ok(())
    }
}

/// Give the interactive member reference a stable order without imposing one on the API.
pub fn sort_known_members_for_display(report: &mut KnownMemberKeysReport) {
    report
        .members
        .sort_by(|left, right| left.member_id.cmp(&right.member_id));
    for member in &mut report.members {
        member.keys.sort_by_key(|key| key.fingerprint);
    }
}

/// Read ordered members after the automatically inserted creator until a blank line.
fn read_additional_group_members(
    dialog: &mut ChecklistDialog<'_>,
    local_member: &MemberIdentity,
) -> Result<Vec<MemberIdentity>, ReplicatedChecklistError> {
    let mut members = Vec::new();
    let mut seen = HashSet::new();
    while let Some(member) =
        dialog.read_optional_member_identity("additional member id (blank to finish)> ")?
    {
        if member == *local_member {
            return Err(ReplicatedChecklistError::RepeatedGroupCreator { member_id: member });
        }
        if !seen.insert(member.clone()) {
            return Err(ReplicatedChecklistError::DuplicateGroupMember { member_id: member });
        }
        members.push(member);
    }
    Ok(members)
}

/// Build the exact replication request produced by the checklist creation wizard.
pub fn checklist_group_creation_request(
    group_name: String,
    local_member: MemberIdentity,
    additional_members: Vec<MemberIdentity>,
) -> CreateGroupRequest {
    let mut members = Vec::with_capacity(additional_members.len() + 1);
    members.push(local_member);
    members.extend(additional_members);
    CreateGroupRequest {
        group_name: Some(group_name),
        message: None,
        members,
        group_schema: CHECKLIST_GROUP_SCHEMA.clone(),
    }
}

/// Format the final group-creation request for confirmation.
#[must_use]
pub fn format_group_creation_summary(request: &CreateGroupRequest) -> String {
    let group_name = request.group_name.as_deref().unwrap_or("<unnamed>");
    let members = request
        .members
        .iter()
        .enumerate()
        .map(|(index, member)| format!("    {index}: {member}"))
        .join("\n");
    let schemas = format_group_schema(&request.group_schema);
    formatdoc!(
        "
        group creation summary:
          name: {group_name}
          message: none
          members:
        {members}
          schema:
        {schemas}
        "
    )
}

/// Print the final group-creation request before confirmation.
pub fn print_group_creation_summary(request: &CreateGroupRequest) {
    print!("{}", format_group_creation_summary(request));
}

/// Format one pending invitation using its current one-based REPL position.
#[must_use]
pub fn format_group_invitation(position: NonZeroUsize, invitation: &GroupInvitation) -> String {
    let group_name = invitation.group_name.as_deref().unwrap_or("<unnamed>");
    let message = invitation.message.as_deref().unwrap_or("<none>");
    let members = invitation
        .proposed_members
        .iter()
        .enumerate()
        .map(|(index, member)| format!("    {index}: {member}"))
        .join("\n");
    let group_id = invitation.group_id;
    let source = format_group_invitation_source(invitation.source);
    let schemas = format_group_schema(&invitation.group_schema);
    let initial_snapshot = format_initial_snapshot(&invitation.initial_snapshot);
    formatdoc!(
        "
        {position}. group {group_id}
          source: {source}
          name: {group_name}
          message: {message}
          members:
        {members}
          schema:
        {schemas}
          initial snapshot: {initial_snapshot}
        "
    )
}

/// Print one pending invitation using its current one-based REPL position.
pub fn print_group_invitation(position: NonZeroUsize, pending: &PendingGroupInvitation) {
    print!("{}", format_group_invitation(position, &pending.invitation));
}

/// Format one group schema for inclusion beneath a two-space-indented heading.
fn format_group_schema(group_schema: &GroupSchema) -> String {
    let schemas = group_schema
        .datasets()
        .into_iter()
        .map(|dataset| {
            let schema = format!("{:#}", dataset.schema.as_schema())
                .lines()
                .map(|line| format!("      {line}"))
                .join("\n");
            format!("    {}:\n{schema}", dataset.dataset_id)
        })
        .join("\n");
    if schemas.is_empty() {
        "    none".to_owned()
    } else {
        schemas
    }
}

/// Format one invitation source without exposing Rust enum syntax.
fn format_group_invitation_source(source: GroupInvitationSource) -> String {
    match source {
        GroupInvitationSource::Creation => "creation".to_owned(),
        GroupInvitationSource::Migration { migration_id } => {
            format!("migration {migration_id}")
        }
    }
}

/// Summarise initial snapshot work without exposing internal type names.
fn format_initial_snapshot(snapshot: &InitialSnapshot) -> String {
    match snapshot {
        InitialSnapshot::Empty => "empty".to_owned(),
        InitialSnapshot::Inline(rows) => {
            let row_count = rows
                .datasets
                .iter()
                .map(|dataset| dataset.rows.len())
                .sum::<usize>();
            format!(
                "inline (datasets: {}; rows: {row_count})",
                rows.datasets.len()
            )
        }
        InitialSnapshot::Metadata(metadata) => {
            let record_count = metadata
                .record_count
                .map_or_else(|| "unknown".to_owned(), |count| count.to_string());
            format!(
                "metadata (primary group: {}; versions: {}; equivalent references: {}; records: {record_count})",
                metadata.primary_ref.group_id,
                metadata.primary_ref.versions,
                metadata.equivalent_refs.len(),
            )
        }
    }
}

/// Follow stored successor links until an open group is found.
fn resolve_open_successor(
    initial_group_id: GroupId,
    groups: &dyn ReplicationGroupSnapshot,
) -> Option<GroupId> {
    let mut visited = HashSet::new();
    let mut group_id = initial_group_id;
    while visited.insert(group_id) {
        let group = groups.group(&group_id)?;
        if group.is_writable() {
            return Some(group_id);
        }
        group_id = group.lifecycle().successor_group_id()?;
    }
    None
}

#[cfg(test)]
pub mod test_support {
    use super::*;
    use flotsync_core::{MemberIndex, versions::VersionVector};
    use flotsync_replication::{
        GroupMemberKeys,
        LocalStoreSecretProfile,
        MemberKeyId,
        ReplicationGroupLifecycle,
        ReplicationGroupRecord,
        ReplicationStore,
        current_slice_placeholder_group_security_material,
        current_slice_placeholder_group_security_material_with_key_id,
        providers::VecRowProvider,
        test_support::{
            SqliteStoreTestOwner,
            provision_test_security,
            provisioned_sqlite_store,
            test_public_member_keys,
            test_replication_security_secrets,
        },
    };
    use futures_util::future;
    use std::sync::Mutex;

    use crate::replicated_checklist::runner::repl::{
        ChecklistListener,
        ChecklistListenerReceivers,
    };

    /// Decision recorded by the test invitation responder.
    #[derive(Debug, PartialEq, Eq)]
    pub enum RecordedInvitationDecision {
        Accepted,
        Rejected(RejectionReason),
    }

    /// One-shot invitation responder that records the selected test decision.
    pub struct RecordingInvitationResponder {
        pub decisions: Arc<Mutex<Vec<RecordedInvitationDecision>>>,
        pub accepted_event: Option<AcceptedListenerEvent>,
    }

    /// Listener event delivered before a successful test acceptance returns.
    pub struct AcceptedListenerEvent {
        pub listener: Arc<ChecklistListener>,
        pub read_token: ReadToken,
        pub changes: Vec<RowChange>,
    }

    pub type TestSqliteStore = SqliteStoreTestOwner<Arc<SqliteReplicationStore>>;

    impl GroupInvitationResponder for RecordingInvitationResponder {
        fn accept(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send>> {
            async move {
                if let Some(event) = self.accepted_event {
                    event
                        .listener
                        .on_event(ReplicationEvent::DataChanged {
                            lineage: DataChangeLineage::Update,
                            read_token: event.read_token,
                            rows: Box::new(VecRowProvider::new(event.changes)),
                        })
                        .await
                        .expect("accepted test event should reach the listener");
                }
                self.decisions
                    .lock()
                    .expect("decision lock should be available")
                    .push(RecordedInvitationDecision::Accepted);
                Ok(())
            }
            .boxed()
        }

        fn reject(
            self: Box<Self>,
            reason: RejectionReason,
        ) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send>> {
            self.decisions
                .lock()
                .expect("decision lock should be available")
                .push(RecordedInvitationDecision::Rejected(reason));
            future::ready(Ok(())).boxed()
        }
    }

    /// Build one open single-member group record for checklist tests.
    pub fn test_group(group_id: GroupId, member: &MemberIdentity) -> ReplicationGroupRecord {
        let fingerprint = test_public_member_keys(member).fingerprint();
        ReplicationGroupRecord {
            group_id,
            member_keys: GroupMemberKeys::from_ordered_member_keys([MemberKeyId {
                member_id: member.clone(),
                fingerprint,
            }])
            .expect("test group member keys should build"),
            local_member_index: MemberIndex::new(0),
            group_schema: CHECKLIST_GROUP_SCHEMA.clone(),
            version_vector: VersionVector::initial(NonZeroUsize::MIN),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material(group_id),
            ..Default::default()
        }
    }

    /// Build the minimum application config needed by an in-memory REPL test.
    pub fn test_app_config() -> ChecklistAppConfig {
        ChecklistAppConfig {
            source_path: PathBuf::from("test.toml"),
            runtime_config_toml: String::new(),
            store_path: PathBuf::from("test.sqlite"),
            store_secret_profile: LocalStoreSecretProfile::new("unsafe:test")
                .expect("test profile should be valid"),
        }
    }

    /// Insert one test group through a complete store transaction.
    pub async fn insert_test_group(store: &dyn ReplicationStore, group: ReplicationGroupRecord) {
        let mut transaction = store
            .begin_transaction()
            .await
            .expect("test transaction should start");
        transaction
            .insert_replication_group(group)
            .await
            .expect("test group should insert");
        transaction
            .commit()
            .await
            .expect("test transaction should commit");
    }

    /// Load a checklist runtime whose store already contains the requested groups.
    pub fn load_test_runtime_with_groups(
        member: &MemberIdentity,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> (
        TestSqliteStore,
        Arc<dyn ReplicationApi>,
        Arc<ChecklistListener>,
        ChecklistListenerReceivers,
    ) {
        let groups = group_ids
            .into_iter()
            .map(|group_id| test_group(group_id, member));
        load_test_runtime_with_group_records(member, groups)
    }

    /// Load a checklist runtime whose store already contains the supplied group records.
    pub fn load_test_runtime_with_group_records(
        member: &MemberIdentity,
        groups: impl IntoIterator<Item = ReplicationGroupRecord>,
    ) -> (
        TestSqliteStore,
        Arc<dyn ReplicationApi>,
        Arc<ChecklistListener>,
        ChecklistListenerReceivers,
    ) {
        let store = provisioned_sqlite_store(member);
        block_on(provision_test_security(
            checklist_application_id(),
            store.as_ref(),
            member,
            std::iter::empty::<MemberIdentity>(),
        ))
        .expect("test security should provision");
        let security = test_replication_security_secrets();
        let store_secret_key_id = *security.store_secret_key_id();
        for mut group in groups {
            let group_id = group.group_id;
            group.security_material = current_slice_placeholder_group_security_material_with_key_id(
                group_id,
                store_secret_key_id,
            );
            block_on(insert_test_group(store.as_ref(), group));
        }
        let (listener, listener_receivers) = ChecklistListener::pair();
        let runtime = block_on(load_replication_runtime_with_runtime_config_toml(
            checklist_application_id(),
            &CHECKLIST_APPLICATION_SCHEMAS,
            store.clone(),
            listener.clone(),
            ReplicationConfig::default(),
            security,
            "",
        ))
        .expect("test runtime should load");
        (
            SqliteStoreTestOwner::from_store(store),
            runtime,
            listener,
            listener_receivers,
        )
    }

    /// Build one test group with the supplied display name.
    pub fn named_test_group(
        group_id: GroupId,
        member: &MemberIdentity,
        name: &str,
    ) -> ReplicationGroupRecord {
        ReplicationGroupRecord {
            group_name: Some(name.to_owned()),
            ..test_group(group_id, member)
        }
    }
}

#[cfg(test)]
mod tests;

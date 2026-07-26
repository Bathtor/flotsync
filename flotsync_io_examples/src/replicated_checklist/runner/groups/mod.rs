//! Group registry, creation, invitation, and rendering behaviour for the checklist REPL.

use super::{
    repl::{ChecklistRepl, ChecklistSession, PendingGroupInvitation, join_words},
    *,
};
use indoc::{formatdoc, printdoc};

/// Load every group whose rows remain application-readable.
pub async fn load_readable_groups(
    store: &dyn ReplicationStore,
) -> Result<Vec<ReplicationGroupRecord>, ReplicatedChecklistError> {
    let mut groups = load_group_records(store).await?;
    groups.retain(|group| group.lifecycle.is_readable());
    Ok(groups)
}

/// Load every stored group record in stable UUID order, including closed groups.
pub async fn load_group_records(
    store: &dyn ReplicationStore,
) -> Result<Vec<ReplicationGroupRecord>, ReplicatedChecklistError> {
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(repl_error::StoreSnafu)?;
    let mut groups = transaction
        .load_replication_groups()
        .await
        .context(repl_error::StoreSnafu)?;
    transaction
        .release()
        .await
        .context(repl_error::StoreSnafu)?;
    groups.sort_by_key(|group| group.group_id);
    Ok(groups)
}

/// Change made to the process-local default after refreshing group lifecycles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultGroupRefresh {
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
    pub fn new(
        groups: impl IntoIterator<Item = ReplicationGroupRecord>,
        working_set: ChecklistWorkingSet,
    ) -> Self {
        Self {
            groups: groups
                .into_iter()
                .map(|group| (group.group_id, group))
                .collect(),
            default_group: None,
            working_set,
        }
    }

    /// Resolve a group UUID first, otherwise an exact unique display name.
    pub fn resolve_group(
        &self,
        selector: &str,
    ) -> Result<&ReplicationGroupRecord, ReplicatedChecklistError> {
        if let Ok(uuid) = Uuid::parse_str(selector) {
            let group_id = GroupId(uuid);
            return self.groups.get(&group_id).ok_or_else(|| {
                ReplicatedChecklistError::UnknownGroup {
                    selector: selector.to_owned(),
                }
            });
        }

        let mut matches = self
            .groups
            .values()
            .filter(|group| group.group_name.as_deref() == Some(selector))
            .collect::<Vec<_>>();
        matches.sort_by_key(|group| group.group_id);
        match matches.as_slice() {
            [group] => Ok(*group),
            [] => Err(ReplicatedChecklistError::UnknownGroup {
                selector: selector.to_owned(),
            }),
            _ => Err(ReplicatedChecklistError::AmbiguousGroupName {
                name: selector.to_owned(),
                candidate_ids: matches.iter().map(|group| group.group_id).collect(),
            }),
        }
    }

    /// Select one writable group as the session default.
    pub fn set_default(&mut self, selector: &str) -> Result<GroupId, ReplicatedChecklistError> {
        let group = self.resolve_group(selector)?;
        if !group.lifecycle.is_writable() {
            return Err(ReplicatedChecklistError::NonWritableDefaultGroup {
                group_id: group.group_id,
            });
        }
        let group_id = group.group_id;
        self.default_group = Some(group_id);
        Ok(group_id)
    }

    /// Return the current default group record.
    pub fn default_group(&self) -> Result<&ReplicationGroupRecord, ReplicatedChecklistError> {
        let group_id = self
            .default_group
            .ok_or(ReplicatedChecklistError::NoDefaultGroup)?;
        self.groups
            .get(&group_id)
            .ok_or(ReplicatedChecklistError::UnknownGroup {
                selector: group_id.to_string(),
            })
    }

    /// Return a stable display label, preferring an unambiguous group name.
    pub fn group_label(&self, group_id: GroupId) -> String {
        let Some(group) = self.groups.get(&group_id) else {
            return group_id.to_string();
        };
        let Some(name) = &group.group_name else {
            return group_id.to_string();
        };
        let name_count = self
            .groups
            .values()
            .filter(|candidate| candidate.group_name.as_ref() == Some(name))
            .count();
        if name_count == 1 {
            name.clone()
        } else {
            format!("{name} ({group_id})")
        }
    }

    /// Return the human-readable association for one workspace item.
    pub fn association_label(&self, association: ChecklistItemAssociation) -> String {
        match association {
            ChecklistItemAssociation::Local => "local".to_owned(),
            ChecklistItemAssociation::Group(group_id) => self.group_label(group_id),
        }
    }

    /// Return the canonical unambiguous REPL reference for one item identity.
    pub fn item_reference(&self, item_id: ChecklistItemId) -> String {
        let association = match item_id.association {
            ChecklistItemAssociation::Local => "local".to_owned(),
            ChecklistItemAssociation::Group(group_id) => {
                let Some(group) = self.groups.get(&group_id) else {
                    return format!("{group_id}/{}", item_id.row_key);
                };
                let Some(name) = &group.group_name else {
                    return format!("{group_id}/{}", item_id.row_key);
                };
                let name_is_usable = name != "local"
                    && !name.chars().any(char::is_whitespace)
                    && Uuid::parse_str(name).is_err()
                    && self
                        .groups
                        .values()
                        .filter(|candidate| candidate.group_name.as_ref() == Some(name))
                        .count()
                        == 1;
                if name_is_usable {
                    name.clone()
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
                                .map(|item_id| self.item_reference(item_id))
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
                            let group = self.resolve_group(group)?;
                            group.group_id
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
        &self,
        selector: &str,
    ) -> Result<ChecklistItemAssociation, ReplicatedChecklistError> {
        let group = self.resolve_group(selector)?;
        if !group.lifecycle.is_writable() {
            return Err(ReplicatedChecklistError::NonWritableTargetGroup {
                group_id: group.group_id,
            });
        }
        Ok(ChecklistItemAssociation::Group(group.group_id))
    }

    /// Ensure a listener batch refers only to groups present in this registry.
    pub fn validate_listener_changes(
        &self,
        changes: &[RowChange],
    ) -> Result<(), ReplicatedChecklistError> {
        for change in changes {
            let group_id = change.row_id().group_id;
            if !self.groups.contains_key(&group_id) {
                return repl_error::UnknownListenerGroupSnafu { group_id }.fail();
            }
        }
        Ok(())
    }

    /// Replace the readable registry and repair the default from complete lifecycle records.
    pub fn refresh_groups(
        &mut self,
        groups: impl IntoIterator<Item = ReplicationGroupRecord>,
    ) -> DefaultGroupRefresh {
        let all_groups = groups
            .into_iter()
            .map(|group| (group.group_id, group))
            .collect::<HashMap<_, _>>();
        let previous_default = self.default_group;
        let repaired_default =
            previous_default.and_then(|group_id| resolve_open_successor(group_id, &all_groups));
        self.groups = all_groups
            .into_iter()
            .filter(|(_, group)| group.lifecycle.is_readable())
            .collect();
        self.default_group = repaired_default;

        match (previous_default, repaired_default) {
            (None, None) => DefaultGroupRefresh::Unchanged,
            (None, Some(group_id)) => {
                unreachable!(
                    "group registry refresh cannot select default group {group_id} without a previous default"
                )
            }
            (Some(previous_group_id), None) => DefaultGroupRefresh::Cleared { previous_group_id },
            (Some(previous_group_id), Some(successor_group_id))
                if previous_group_id == successor_group_id =>
            {
                DefaultGroupRefresh::Unchanged
            }
            (Some(previous_group_id), Some(successor_group_id)) => {
                DefaultGroupRefresh::Reassigned {
                    previous_group_id,
                    successor_group_id,
                }
            }
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
            ChecklistGroupCommand::List => self.print_groups(),
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
                let group_id = self.session.set_default(&selector)?;
                println!("default group: {}", self.session.group_label(group_id));
            }
            ChecklistGroupCommand::ClearDefault => {
                self.session.default_group = None;
                println!("default group: none");
            }
        }
        Ok(())
    }

    /// Reload group metadata and repair a default invalidated by lifecycle changes.
    pub async fn refresh_group_registry(&mut self) -> Result<(), ReplicatedChecklistError> {
        let groups = load_group_records(self.store.as_ref()).await?;
        match self.session.refresh_groups(groups) {
            DefaultGroupRefresh::Unchanged => {
                // No default is selected, or the selected default remains open;
                // neither case has a selection change to report.
            }
            DefaultGroupRefresh::Reassigned {
                previous_group_id,
                successor_group_id,
            } => {
                println!(
                    "default group updated: {previous_group_id} -> {}",
                    self.session.group_label(successor_group_id)
                );
            }
            DefaultGroupRefresh::Cleared { previous_group_id } => {
                println!(
                    "default group cleared: {previous_group_id} has no open successor in the local registry"
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
        let mut read_member = || {
            read_prompted_line(
                "additional member id (blank to finish)> ",
                "reading proposed group members",
            )
        };
        let mut confirm_creation = confirm;
        self.create_group_with_prompts(name, &mut read_member, &mut confirm_creation)
            .await
    }

    /// Run group creation with injected prompt boundaries for deterministic handler tests.
    pub async fn create_group_with_prompts(
        &mut self,
        name: Vec<String>,
        read_member: &mut dyn FnMut() -> Result<String, ReplicatedChecklistError>,
        confirm_creation: &mut dyn FnMut(&str) -> Result<bool, ReplicatedChecklistError>,
    ) -> Result<(), ReplicatedChecklistError> {
        let group_name = join_words(name).trim().to_owned();
        let mut known_members = self
            .replication
            .known_member_keys()
            .await
            .context(repl_error::ReplicationSnafu)?;
        sort_known_members_for_display(&mut known_members);
        println!("{known_members}");
        println!("creator (position 0): {}", self.config.local_member);
        let additional_members =
            read_additional_group_members(read_member, &self.config.local_member)?;
        let request = checklist_group_creation_request(
            group_name,
            self.config.local_member.clone(),
            additional_members,
        );
        print_group_creation_summary(&request);
        if !confirm_creation("Create this group and send invitations?")? {
            println!("group creation cancelled");
            return Ok(());
        }

        let group_id = self
            .replication
            .create_group(request)
            .await
            .context(repl_error::ReplicationSnafu)?;
        self.refresh_group_registry().await?;
        self.drain_listener_queue()?;
        self.session.working_set.drain_queued_events();
        println!("created group: {}", self.session.group_label(group_id));
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
        self.refresh_group_registry().await?;
        self.drain_listener_queue()?;
        self.session.working_set.drain_queued_events();
        println!(
            "accepted group invitation: {}",
            self.session.group_label(group_id)
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

    /// Print the ordered members of the current default group.
    pub fn print_members(&self) -> Result<(), ReplicatedChecklistError> {
        let group = self.session.default_group()?;
        println!("group {}", self.session.group_label(group.group_id));
        for (index, member) in group.member_ids().enumerate() {
            let marker = if member == &self.config.local_member {
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
        let group = self.session.default_group()?;
        let group_id = group.group_id;
        let members = group.member_ids().cloned().collect::<Vec<_>>();
        println!("group {}", self.session.group_label(group_id));
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
        let local_member = self.config.local_member.clone();
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
    pub fn print_groups(&self) {
        if self.session.groups.is_empty() {
            println!("groups: none");
            return;
        }
        let mut groups = self.session.groups.values().collect::<Vec<_>>();
        groups.sort_by_key(|group| group.group_id);
        for group in groups {
            let marker = if self.session.default_group == Some(group.group_id) {
                " default"
            } else {
                ""
            };
            let name = group.group_name.as_deref().unwrap_or("<unnamed>");
            println!(
                "{} name={name:?} lifecycle={:?} members={}{}",
                group.group_id,
                group.lifecycle,
                group.member_count(),
                marker
            );
        }
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
pub fn read_additional_group_members(
    read_member: &mut dyn FnMut() -> Result<String, ReplicatedChecklistError>,
    local_member: &MemberIdentity,
) -> Result<Vec<MemberIdentity>, ReplicatedChecklistError> {
    let mut members = Vec::new();
    let mut seen = HashSet::new();
    let mut member_input = read_member()?;
    while !member_input.trim().is_empty() {
        let member = member_input
            .trim()
            .parse::<MemberIdentity>()
            .context(repl_error::InvalidGroupMemberIdentitySnafu)?;
        if member == *local_member {
            return Err(ReplicatedChecklistError::RepeatedGroupCreator { member_id: member });
        }
        if !seen.insert(member.clone()) {
            return Err(ReplicatedChecklistError::DuplicateGroupMember { member_id: member });
        }
        members.push(member);
        member_input = read_member()?;
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

/// Read one wizard line after displaying its prompt.
pub fn read_prompted_line(
    prompt: &str,
    action: &'static str,
) -> Result<String, ReplicatedChecklistError> {
    print!("{prompt}");
    io::stdout()
        .flush()
        .context(repl_error::IoSnafu { action })?;
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .context(repl_error::IoSnafu { action })?;
    Ok(line)
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
    let schemas = request
        .group_schema
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
    let schemas = if schemas.is_empty() {
        "    none"
    } else {
        &schemas
    };
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

/// Print one pending invitation using its current one-based REPL position.
pub fn print_group_invitation(position: NonZeroUsize, pending: &PendingGroupInvitation) {
    let invitation = &pending.invitation;
    let group_name = invitation.group_name.as_deref().unwrap_or("<unnamed>");
    let message = invitation.message.as_deref().unwrap_or("<none>");
    let members = invitation
        .proposed_members
        .iter()
        .enumerate()
        .map(|(index, member)| format!("    {index}: {member}"))
        .join("\n");
    let group_id = invitation.group_id;
    let source = &invitation.source;
    let schema = &invitation.group_schema;
    let initial_snapshot = &invitation.initial_snapshot;
    printdoc!(
        "
        {position}. group {group_id}
          source: {source:?}
          name: {group_name:?}
          message: {message:?}
          members:
        {members}
          schema: {schema:#?}
          initial snapshot: {initial_snapshot:?}
        "
    );
}

/// Follow stored successor links until an open group is found.
fn resolve_open_successor(
    initial_group_id: GroupId,
    groups: &HashMap<GroupId, ReplicationGroupRecord>,
) -> Option<GroupId> {
    let mut visited = HashSet::new();
    let mut group_id = initial_group_id;
    while visited.insert(group_id) {
        let group = groups.get(&group_id)?;
        if group.lifecycle.is_writable() {
            return Some(group_id);
        }
        group_id = group.lifecycle.successor_group_id()?;
    }
    None
}

#[cfg(test)]
pub mod test_support {
    use super::*;
    use flotsync_core::{MemberIndex, versions::VersionVector};
    use flotsync_replication::{
        GroupMemberKeys,
        GroupSchema,
        LocalStoreSecretProfile,
        MemberKeyId,
        ReplicationGroupLifecycle,
        current_slice_placeholder_group_security_material,
        current_slice_placeholder_group_security_material_with_key_id,
        providers::VecRowProvider,
        test_support::{
            provision_test_security,
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

    impl GroupInvitationResponder for RecordingInvitationResponder {
        fn accept(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send>> {
            async move {
                if let Some(event) = self.accepted_event {
                    event
                        .listener
                        .on_event(ReplicationEvent::DataChanged {
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
            group_schema: GroupSchema::default(),
            version_vector: VersionVector::initial(NonZeroUsize::MIN),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: current_slice_placeholder_group_security_material(group_id),
            ..Default::default()
        }
    }

    /// Build the minimum application config needed by an in-memory REPL test.
    pub fn test_app_config(member: MemberIdentity) -> ChecklistAppConfig {
        ChecklistAppConfig {
            source_path: PathBuf::from("test.toml"),
            runtime_config_toml: String::new(),
            local_member: member,
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
        Arc<SqliteReplicationStore>,
        Arc<dyn ReplicationApi>,
        Arc<ChecklistListener>,
        ChecklistListenerReceivers,
    ) {
        let store = Arc::new(
            block_on(SqliteReplicationStore::in_memory_with_schema_sources(
                member.clone(),
                [(checklist_dataset_id(), CHECKLIST_SCHEMA.clone())],
            ))
            .expect("test store should open"),
        );
        block_on(provision_test_security(
            checklist_application_id(),
            store.as_ref(),
            member,
            std::iter::empty::<MemberIdentity>(),
        ))
        .expect("test security should provision");
        let security = test_replication_security_secrets();
        let store_secret_key_id = *security.store_secret_key_id();
        for group_id in group_ids {
            let mut group = test_group(group_id, member);
            group.security_material = current_slice_placeholder_group_security_material_with_key_id(
                group_id,
                store_secret_key_id,
            );
            block_on(insert_test_group(store.as_ref(), group));
        }
        let (listener, listener_receivers) = ChecklistListener::pair();
        let runtime = block_on(load_replication_runtime_with_runtime_config_toml(
            checklist_application_id(),
            store.clone(),
            listener.clone(),
            ReplicationConfig::default(),
            security,
            "",
        ))
        .expect("test runtime should load");
        (store, runtime, listener, listener_receivers)
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

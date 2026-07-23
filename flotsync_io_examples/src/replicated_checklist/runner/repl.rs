//! REPL and runtime wiring for the replicated checklist runner.

use super::{setup::load_checklist_store_setup, *};
use indoc::formatdoc;

pub(super) async fn run_configured_peer(
    config_path: &Path,
) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path).await?;
    let persisted_groups = load_readable_groups(setup.store.as_ref()).await?;

    let (listener, listener_receiver) = ChecklistListener::pair();
    let replication = load_replication_runtime_with_runtime_config_toml(
        checklist_application_id(),
        setup.store,
        listener,
        ReplicationConfig::default(),
        setup.replication_security,
        &setup.config.runtime_config_toml,
    )
    .await
    .context(repl_error::LoadRuntimeSnafu)?;

    let mut working_set = ChecklistWorkingSet::new();
    for group in &persisted_groups {
        load_group_snapshot(replication.as_ref(), &mut working_set, group.group_id).await?;
    }
    let session = ChecklistSession::new(persisted_groups, working_set);
    let mut repl = ChecklistRepl::new(setup.config, replication, listener_receiver, session);
    let run_result = repl.run().await;
    let shutdown_result = repl.shutdown().await;
    run_result?;
    shutdown_result
}

/// Load every group whose rows remain application-readable.
async fn load_readable_groups(
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
    groups.retain(|group| group.lifecycle.is_readable());
    groups.sort_by_key(|group| group.group_id);
    Ok(groups)
}

struct ChecklistListener {
    sender: Sender<ChecklistListenerBatch>,
}

struct ChecklistListenerBatch {
    read_token: ReadToken,
    changes: Vec<RowChange>,
}

impl ChecklistListener {
    /// Return the listener plus the receiver used by the REPL to drain queued row-change batches.
    fn pair() -> (Arc<Self>, Receiver<ChecklistListenerBatch>) {
        let (sender, receiver) = mpsc::channel();
        (Arc::new(Self { sender }), receiver)
    }
}

impl ReplicationEventListener for ChecklistListener {
    fn on_event(
        &self,
        event: ReplicationEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), ListenerError>> + Send + '_>> {
        let sender = self.sender.clone();
        async move {
            match event {
                ReplicationEvent::DataChanged {
                    read_token,
                    mut rows,
                } => {
                    while let Some(batch) = rows.next_batch().await.boxed()? {
                        sender
                            .send(ChecklistListenerBatch {
                                read_token: read_token.clone(),
                                changes: batch.into_iter().collect(),
                            })
                            .map_err(|_| ListenerError::Rejected {
                                message: "checklist listener queue is closed".to_owned(),
                            })?;
                    }
                    Ok(())
                }
                ReplicationEvent::GroupInvitation { respond, .. } => {
                    respond
                        .reject(RejectionReason::PolicyDenied)
                        .await
                        .boxed()?;
                    Ok(())
                }
                ReplicationEvent::MigrationProposals { proposals } => {
                    for candidate in proposals {
                        candidate
                            .respond
                            .reject(RejectionReason::PolicyDenied)
                            .await
                            .boxed()?;
                    }
                    Ok(())
                }
            }
        }
        .boxed()
    }
}

struct ChecklistRepl {
    config: ChecklistAppConfig,
    replication: Arc<dyn ReplicationApi>,
    listener_receiver: Receiver<ChecklistListenerBatch>,
    session: ChecklistSession,
}

/// Group registry, session default, and heterogeneous checklist working set.
struct ChecklistSession {
    groups: HashMap<GroupId, ReplicationGroupRecord>,
    default_group: Option<GroupId>,
    working_set: ChecklistWorkingSet,
}

impl ChecklistSession {
    /// Build one session with no inferred default group.
    fn new(
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
    fn resolve_group(
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
    fn set_default(&mut self, selector: &str) -> Result<GroupId, ReplicatedChecklistError> {
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
    fn default_group(&self) -> Result<&ReplicationGroupRecord, ReplicatedChecklistError> {
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
    fn group_label(&self, group_id: GroupId) -> String {
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
    fn association_label(&self, association: ChecklistItemAssociation) -> String {
        match association {
            ChecklistItemAssociation::Local => "local".to_owned(),
            ChecklistItemAssociation::Group(group_id) => self.group_label(group_id),
        }
    }

    /// Ensure a listener batch refers only to groups present in this registry.
    fn validate_listener_changes(
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

    /// Prepare only the current default group for transitional synchronisation.
    fn prepare_default_sync(&self) -> Result<Option<ChecklistSyncPlan>, ChecklistWorkingSetError> {
        match self.default_group {
            Some(group_id) => self.working_set.prepare_group_sync(group_id),
            None => Ok(None),
        }
    }
}

impl ChecklistRepl {
    fn new(
        config: ChecklistAppConfig,
        replication: Arc<dyn ReplicationApi>,
        listener_receiver: Receiver<ChecklistListenerBatch>,
        session: ChecklistSession,
    ) -> Self {
        Self {
            config,
            replication,
            listener_receiver,
            session,
        }
    }

    #[allow(
        clippy::needless_continue,
        reason = "The REPL loop uses explicit continues to make command-processing outcomes obvious."
    )]
    async fn run(&mut self) -> Result<(), ReplicatedChecklistError> {
        println!(
            "replicated checklist: {} readable groups, no default selected",
            self.session.groups.len()
        );
        println!("type 'help' for commands");

        let mut line = String::new();
        'repl: loop {
            print!("checklist> ");
            io::stdout().flush().context(repl_error::IoSnafu {
                action: "flushing prompt",
            })?;
            line.clear();
            if io::stdin()
                .read_line(&mut line)
                .context(repl_error::IoSnafu {
                    action: "reading command",
                })?
                == 0
            {
                // EOF from stdin means the caller requested shutdown.
                break 'repl;
            }

            let command = match parse_checklist_command(&line) {
                Ok(Some(command)) => command,
                Ok(None) => continue 'repl,
                Err(error) => {
                    eprintln!("{error}");
                    continue 'repl;
                }
            };
            match self.handle_command(command).await {
                Ok(true) => continue 'repl,
                Ok(false) => break 'repl,
                Err(error) => {
                    eprintln!("{error}");
                    continue 'repl;
                }
            }
        }
        Ok(())
    }

    async fn handle_command(
        &mut self,
        command: ChecklistCommand,
    ) -> Result<bool, ReplicatedChecklistError> {
        match command {
            ChecklistCommand::Keys { command } => {
                keys::run_runtime_key_command(self.replication.as_ref(), command).await?;
                return Ok(true);
            }
            ChecklistCommand::Me => {
                self.print_me();
                return Ok(true);
            }
            ChecklistCommand::Group { command } => {
                self.handle_group_registry_command(command)?;
                return Ok(true);
            }
            ChecklistCommand::Help => {
                println!("{}", checklist_help());
                return Ok(true);
            }
            ChecklistCommand::Quit => return Ok(false),
            _ => {}
        }
        self.handle_workspace_command(command).await?;
        Ok(true)
    }

    /// Run one command against the heterogeneous checklist workspace.
    async fn handle_workspace_command(
        &mut self,
        command: ChecklistCommand,
    ) -> Result<(), ReplicatedChecklistError> {
        match command {
            ChecklistCommand::Add { text } => {
                let association = self.session.default_group.map_or(
                    ChecklistItemAssociation::Local,
                    ChecklistItemAssociation::Group,
                );
                let item_id = self
                    .session
                    .working_set
                    .add_item(association, join_words(text));
                println!("added:");
                self.print_item_id(item_id)?;
            }
            ChecklistCommand::Rename { item, text } => {
                self.session
                    .working_set
                    .rename_item(item, join_words(text))
                    .context(repl_error::WorkingSetSnafu)?;
                println!("renamed:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Edit {
                command: EditCommand::Note { item },
            } => {
                self.edit_note(item)?;
            }
            ChecklistCommand::Tag { command } => match command {
                TagCommand::Add { item, tag } => {
                    self.session
                        .working_set
                        .add_tag(item, tag)
                        .context(repl_error::WorkingSetSnafu)?;
                    println!("tag added:");
                    self.print_selected_row(item)?;
                }
                TagCommand::Rm { item, tag } => {
                    self.session
                        .working_set
                        .remove_tag(item, &tag)
                        .context(repl_error::WorkingSetSnafu)?;
                    println!("tag removed:");
                    self.print_selected_row(item)?;
                }
            },
            ChecklistCommand::Claim { item } => {
                self.session
                    .working_set
                    .claim_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("claimed:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Complete { item } => {
                self.session
                    .working_set
                    .complete_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("completed:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Priority { item, priority } => {
                self.session
                    .working_set
                    .set_priority(item, priority)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("priority set:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Delete { item } => {
                println!("deleted:");
                self.print_selected_row(item)?;
                self.session
                    .working_set
                    .delete_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
            }
            ChecklistCommand::List => self.print_list(),
            ChecklistCommand::Show { item } => self.print_item(item)?,
            ChecklistCommand::Events { limit } => self.print_events(limit),
            ChecklistCommand::Sync => self.sync().await?,
            ChecklistCommand::Members => self.print_members()?,
            ChecklistCommand::Check => self.check_members().await?,
            ChecklistCommand::Group { .. }
            | ChecklistCommand::Keys { .. }
            | ChecklistCommand::Me
            | ChecklistCommand::Help
            | ChecklistCommand::Quit => {
                unreachable!("commands available without a group are handled before dispatch")
            }
        }
        Ok(())
    }

    /// Run one group-registry command that never requires an existing default.
    fn handle_group_registry_command(
        &mut self,
        command: ChecklistGroupCommand,
    ) -> Result<(), ReplicatedChecklistError> {
        match command {
            ChecklistGroupCommand::List => self.print_groups(),
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

    fn edit_note(&mut self, item: ItemSelector) -> Result<(), ReplicatedChecklistError> {
        let selected = self
            .session
            .working_set
            .selected_item(item)
            .context(repl_error::WorkingSetSnafu)?;
        println!("current note: {}", selected.item.note);
        print!("note> ");
        io::stdout().flush().context(repl_error::IoSnafu {
            action: "flushing note prompt",
        })?;
        let mut note = String::new();
        io::stdin()
            .read_line(&mut note)
            .context(repl_error::IoSnafu {
                action: "reading note",
            })?;
        self.session
            .working_set
            .edit_note(item, note.trim_end_matches(['\r', '\n']))
            .context(repl_error::WorkingSetSnafu)?;
        println!("note updated:");
        self.print_selected_row(item)?;
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), ReplicatedChecklistError> {
        let plan = self
            .session
            .prepare_default_sync()
            .context(repl_error::WorkingSetSnafu)?;
        let published_group = plan
            .as_ref()
            .and(self.session.default_group)
            .map(|group_id| self.session.group_label(group_id));
        if let Some(plan) = &plan {
            let read_token = self
                .session
                .working_set
                .read_token()
                .context(repl_error::WorkingSetSnafu)?;
            let receipt = self
                .replication
                .publish_changes(PublishChangesRequest {
                    read_token,
                    changes: plan.mutations.clone(),
                })
                .await
                .context(repl_error::ReplicationSnafu)?;
            // The receipt token is our previous application read position with
            // this local writer position advanced. Keeping it here makes the
            // next local sync causally depend on the write we just published
            // without waiting for the listener echo to be drained first.
            self.session.working_set.set_read_token(receipt.read_token);
        }
        self.session.working_set.finish_successful_group_sync(plan);
        let listener_batch_count = self.drain_listener_queue()?;
        let applied_events = self.session.working_set.drain_queued_events();
        let dirty_local = self.session.working_set.dirty_local_item_count();
        let mut dirty_groups = self
            .session
            .working_set
            .dirty_group_ids()
            .into_iter()
            .collect::<Vec<_>>();
        dirty_groups.sort();
        let dirty_group_labels = dirty_groups
            .into_iter()
            .map(|group_id| self.session.group_label(group_id))
            .join(", ");
        let dirty_group_summary = if dirty_group_labels.is_empty() {
            "none"
        } else {
            &dirty_group_labels
        };
        let published_group = published_group.as_deref().unwrap_or("none");
        println!(
            "{}",
            formatdoc! {"
                sync complete:
                  published group: {published_group}
                  listener batches: {listener_batch_count}
                  applied events: {applied_events}
                  unsynchronised local items: {dirty_local}
                  dirty groups: {dirty_group_summary}"
            }
        );
        Ok(())
    }

    /// Drain queued listener batches into the working set and return the number of batches drained.
    fn drain_listener_queue(&mut self) -> Result<usize, ReplicatedChecklistError> {
        let mut drained_batch_count = 0;
        while let Some(batch) = self.receive_listener_batch()? {
            self.session.validate_listener_changes(&batch.changes)?;
            self.session
                .working_set
                .enqueue_row_changes(batch.changes)
                .context(repl_error::WorkingSetSnafu)?;
            // No REPL command can run while sync is draining listener batches,
            // so it is safe to merge the event token before the queued rows are
            // applied immediately after this method returns.
            self.session.working_set.merge_read_token(batch.read_token);
            drained_batch_count += 1;
        }
        Ok(drained_batch_count)
    }

    /// Return one queued listener batch, or `None` when the listener queue is currently empty.
    fn receive_listener_batch(
        &self,
    ) -> Result<Option<ChecklistListenerBatch>, ReplicatedChecklistError> {
        match self.listener_receiver.try_recv() {
            Ok(changes) => Ok(Some(changes)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => repl_error::ListenerQueueClosedSnafu.fail(),
        }
    }

    fn print_list(&self) {
        let items = self.session.working_set.listed_items();
        if items.is_empty() {
            println!("checklist is empty");
        } else {
            for item in items {
                self.print_row(&item);
            }
        }
    }

    fn print_item(&self, item: ItemSelector) -> Result<(), ReplicatedChecklistError> {
        let listed = self
            .session
            .working_set
            .selected_item(item)
            .context(repl_error::WorkingSetSnafu)?;
        println!("index: {}", listed.index);
        println!(
            "association: {}",
            self.session.association_label(listed.item_id.association)
        );
        println!("row: {}", listed.item_id.row_key);
        println!("text: {}", listed.item.text);
        println!("note: {}", listed.item.note);
        println!("tags: {}", listed.item.formatted_tags());
        println!("status: {}", listed.item.status);
        println!("priority: {}", listed.item.priority);
        println!("edit_count: {}", listed.item.edit_count);
        Ok(())
    }

    fn print_events(&self, limit: Option<usize>) {
        let events = self.session.working_set.events();
        for event in events.iter().rev().take(limit.unwrap_or(usize::MAX)) {
            println!("event {}:", format_timestamp(event.timestamp));
            for change in &event.changes {
                let item_id = change.item_id();
                let association = self.session.association_label(item_id.association);
                println!("  {association}/{}: {change:?}", item_id.row_key);
            }
        }
        if events.is_empty() {
            println!("no events");
        }
    }

    fn print_members(&self) -> Result<(), ReplicatedChecklistError> {
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

    async fn check_members(&self) -> Result<(), ReplicatedChecklistError> {
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

    async fn shutdown(&self) -> Result<(), ReplicatedChecklistError> {
        self.replication
            .shutdown()
            .await
            .context(repl_error::ReplicationSnafu)
    }

    fn print_me(&self) {
        println!("member: {}", self.config.local_member);
        match self.session.default_group {
            Some(group_id) => println!("default group: {}", self.session.group_label(group_id)),
            None => println!("default group: none"),
        }
        println!("readable groups: {}", self.session.groups.len());
        println!("store: {}", self.config.store_path.display());
        println!("config: {}", self.config.source_path.display());
        println!(
            "dirty rows: {}, queued events: {}",
            self.session.working_set.dirty_row_count(),
            self.session.working_set.queued_event_count()
        );
    }

    fn print_selected_row(&self, item: ItemSelector) -> Result<(), ReplicatedChecklistError> {
        let selected = self
            .session
            .working_set
            .selected_item(item)
            .context(repl_error::WorkingSetSnafu)?;
        self.print_row(&selected);
        Ok(())
    }

    /// Print one row from an already-resolved workspace identity.
    fn print_item_id(&self, item_id: ChecklistItemId) -> Result<(), ReplicatedChecklistError> {
        let selected = self
            .session
            .working_set
            .listed_item(item_id)
            .ok_or_else(|| ReplicatedChecklistError::WorkingSet {
                source: ChecklistWorkingSetError::UnknownItem {
                    selector: ItemSelector::RowKey(item_id.row_key),
                },
            })?;
        self.print_row(&selected);
        Ok(())
    }

    /// Print the readable group registry in stable UUID order.
    fn print_groups(&self) {
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

    /// Print one compact row with its workspace association.
    fn print_row(&self, item: &ListedChecklistItem<'_>) {
        let tags = item.item.formatted_tags();
        let association = self.session.association_label(item.item_id.association);
        println!(
            "{:>3}. [{}] p{} edits={} {} ({association}/{}) {tags}",
            item.index,
            item.item.status,
            item.item.priority,
            item.item.edit_count,
            item.item.text,
            item.item_id.row_key,
        );
    }
}

async fn load_group_snapshot(
    replication: &dyn ReplicationApi,
    working_set: &mut ChecklistWorkingSet,
    group_id: GroupId,
) -> Result<(), ReplicatedChecklistError> {
    let mut snapshot = replication
        .snapshot_rows(SnapshotRowsRequest {
            group_id,
            datasets: HashSet::from([checklist_dataset_id()]),
            max_rows_per_batch: CHECKLIST_SNAPSHOT_BATCH_SIZE,
            include_tombstones: false,
        })
        .await
        .context(repl_error::ReplicationSnafu)?;
    let read_token = snapshot.read_token.clone();
    while let Some(batch) = snapshot
        .rows
        .next_batch()
        .await
        .context(repl_error::SnapshotRowsSnafu)?
    {
        working_set
            .apply_snapshot_rows(batch.rows())
            .context(repl_error::WorkingSetSnafu)?;
    }
    working_set.merge_read_token(read_token);
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "Parsed command words are consumed when building checklist item text."
)]
fn join_words(words: Vec<String>) -> String {
    words.join(" ")
}

fn format_timestamp(timestamp: SystemTime) -> String {
    DateTime::<Local>::from(timestamp)
        .format("%Y-%m-%d %H:%M:%S %:z")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicated_checklist::ChecklistItem;
    use flotsync_core::{MemberIndex, versions::VersionVector};
    use flotsync_replication::{
        GroupMemberKeys,
        GroupSchema,
        MemberKeyId,
        ReplicationGroupLifecycle,
        RowId,
        RowKey,
        RowMutation,
        current_slice_placeholder_group_security_material,
        current_slice_placeholder_group_security_material_with_key_id,
        test_support::{
            provision_test_security,
            publish_changes,
            snapshot_read_token,
            test_public_member_keys,
            test_replication_security_secrets,
        },
    };
    use uuid::Uuid;

    fn test_group(group_id: GroupId, member: &MemberIdentity) -> ReplicationGroupRecord {
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

    async fn insert_test_group(store: &dyn ReplicationStore, group: ReplicationGroupRecord) {
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
    fn load_test_runtime_with_groups(
        member: &MemberIdentity,
        group_ids: impl IntoIterator<Item = GroupId>,
    ) -> (Arc<dyn ReplicationApi>, Receiver<ChecklistListenerBatch>) {
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
        let (listener, listener_receiver) = ChecklistListener::pair();
        let runtime = block_on(load_replication_runtime_with_runtime_config_toml(
            checklist_application_id(),
            store,
            listener,
            ReplicationConfig::default(),
            security,
            "",
        ))
        .expect("test runtime should load");
        (runtime, listener_receiver)
    }

    fn named_test_group(
        group_id: GroupId,
        member: &MemberIdentity,
        name: &str,
    ) -> ReplicationGroupRecord {
        ReplicationGroupRecord {
            group_name: Some(name.to_owned()),
            ..test_group(group_id, member)
        }
    }

    #[test]
    fn group_registry_resolves_uuid_before_exact_names_and_reports_ambiguity() {
        let member = MemberIdentity::from_array(["alice"]);
        let uuid_selected_id = GroupId(Uuid::from_u128(71_001));
        let uuid_named_id = GroupId(Uuid::from_u128(71_002));
        let first_shared_id = GroupId(Uuid::from_u128(71_003));
        let second_shared_id = GroupId(Uuid::from_u128(71_004));
        let mut session = ChecklistSession::new(
            [
                named_test_group(uuid_selected_id, &member, "ordinary"),
                named_test_group(uuid_named_id, &member, &uuid_selected_id.to_string()),
                named_test_group(first_shared_id, &member, "shared"),
                named_test_group(second_shared_id, &member, "shared"),
            ],
            ChecklistWorkingSet::new(),
        );

        assert_eq!(
            session
                .resolve_group(&uuid_selected_id.to_string())
                .expect("UUID should resolve")
                .group_id,
            uuid_selected_id
        );
        let error = session
            .resolve_group("shared")
            .expect_err("duplicate names should be ambiguous");
        assert!(matches!(
            error,
            ReplicatedChecklistError::AmbiguousGroupName { candidate_ids, .. }
                if candidate_ids == vec![first_shared_id, second_shared_id]
        ));

        assert_eq!(
            session
                .set_default("ordinary")
                .expect("writable named group should become default"),
            uuid_selected_id
        );
        assert_eq!(session.default_group, Some(uuid_selected_id));
        session.default_group = None;
        assert!(matches!(
            session.default_group(),
            Err(ReplicatedChecklistError::NoDefaultGroup)
        ));
    }

    #[test]
    fn default_selection_rejects_read_only_groups() {
        let member = MemberIdentity::from_array(["alice"]);
        let group_id = GroupId(Uuid::from_u128(71_005));
        let mut group = named_test_group(group_id, &member, "archived");
        group.lifecycle = ReplicationGroupLifecycle::ReadOnly {
            successor_group_id: GroupId(Uuid::from_u128(71_006)),
            final_versions: VersionVector::initial(NonZeroUsize::MIN),
        };
        let mut session = ChecklistSession::new([group], ChecklistWorkingSet::new());

        assert!(matches!(
            session.set_default("archived"),
            Err(ReplicatedChecklistError::NonWritableDefaultGroup {
                group_id: actual
            }) if actual == group_id
        ));
        assert_eq!(session.default_group, None);
    }

    #[test]
    fn listener_group_validation_rejects_rows_outside_the_registry() {
        let member = MemberIdentity::from_array(["alice"]);
        let known_group = GroupId(Uuid::from_u128(71_007));
        let unknown_group = GroupId(Uuid::from_u128(71_008));
        let session = ChecklistSession::new(
            [test_group(known_group, &member)],
            ChecklistWorkingSet::new(),
        );
        let known = RowChange::Delete {
            row_id: RowId {
                group_id: known_group,
                dataset_id: checklist_dataset_id(),
                row_key: RowKey(Uuid::from_u128(1)),
            },
        };
        let unknown = RowChange::Delete {
            row_id: RowId {
                group_id: unknown_group,
                dataset_id: checklist_dataset_id(),
                row_key: RowKey(Uuid::from_u128(2)),
            },
        };

        session
            .validate_listener_changes(&[known])
            .expect("known group should validate");
        assert!(matches!(
            session.validate_listener_changes(&[unknown]),
            Err(ReplicatedChecklistError::UnknownListenerGroup { group_id })
                if group_id == unknown_group
        ));
    }

    #[test]
    fn transitional_sync_plan_includes_only_the_default_group() {
        let member = MemberIdentity::from_array(["alice"]);
        let default_group = GroupId(Uuid::from_u128(71_009));
        let other_group = GroupId(Uuid::from_u128(71_010));
        let mut working_set = ChecklistWorkingSet::new();
        working_set.add_item(
            ChecklistItemAssociation::Group(default_group),
            "default item",
        );
        working_set.add_item(ChecklistItemAssociation::Group(other_group), "other item");
        working_set.add_item(ChecklistItemAssociation::Local, "local item");
        let mut session = ChecklistSession::new(
            [
                test_group(default_group, &member),
                test_group(other_group, &member),
            ],
            working_set,
        );
        session.default_group = Some(default_group);

        let plan = session
            .prepare_default_sync()
            .expect("default sync plan should build")
            .expect("default group should be dirty");

        assert_eq!(plan.mutations.len(), 1);
        assert!(
            plan.mutations
                .iter()
                .all(|mutation| { mutation.row_id().group_id == default_group })
        );
        assert_eq!(session.working_set.dirty_row_count(), 3);
    }

    #[test]
    fn group_snapshot_loading_combines_rows_and_workspace_read_position() {
        let member = MemberIdentity::from_array(["alice"]);
        let first_group = GroupId(Uuid::from_u128(71_011));
        let second_group = GroupId(Uuid::from_u128(71_012));
        let (runtime, _listener_receiver) =
            load_test_runtime_with_groups(&member, [first_group, second_group]);
        let first_row = RowKey(Uuid::from_u128(91));
        let second_row = RowKey(Uuid::from_u128(92));
        let token = snapshot_read_token(runtime.as_ref(), first_group, checklist_dataset_id());
        let first_receipt = publish_changes(
            runtime.as_ref(),
            token,
            vec![RowMutation::Upsert {
                row_id: RowId {
                    group_id: first_group,
                    dataset_id: checklist_dataset_id(),
                    row_key: first_row,
                },
                row: ChecklistItem::new("first snapshot").to_row_values_patch(),
            }],
        );
        publish_changes(
            runtime.as_ref(),
            first_receipt.read_token,
            vec![RowMutation::Upsert {
                row_id: RowId {
                    group_id: second_group,
                    dataset_id: checklist_dataset_id(),
                    row_key: second_row,
                },
                row: ChecklistItem::new("second snapshot").to_row_values_patch(),
            }],
        );

        let mut working_set = ChecklistWorkingSet::new();
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut working_set,
            first_group,
        ))
        .expect("first snapshot should load");
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut working_set,
            second_group,
        ))
        .expect("second snapshot should load");

        assert_eq!(
            working_set
                .item(ChecklistItemId::group(first_group, first_row))
                .expect("first group item should load")
                .text,
            "first snapshot"
        );
        assert_eq!(
            working_set
                .item(ChecklistItemId::group(second_group, second_row))
                .expect("second group item should load")
                .text,
            "second snapshot"
        );
        assert!(
            format!(
                "{:?}",
                working_set
                    .read_token()
                    .expect("workspace token should load")
            )
            .contains("group_count: 2")
        );

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn readable_group_loader_supports_zero_and_several_groups() {
        let member = MemberIdentity::from_array(["alice"]);
        let store = block_on(SqliteReplicationStore::in_memory(member.clone()))
            .expect("test store should open");

        let empty = block_on(load_readable_groups(&store)).expect("zero-group store should load");
        assert!(empty.is_empty());

        let first_group_id = GroupId(Uuid::from_u128(70_001));
        block_on(insert_test_group(
            &store,
            test_group(first_group_id, &member),
        ));
        let second_group_id = GroupId(Uuid::from_u128(70_002));
        block_on(insert_test_group(
            &store,
            test_group(second_group_id, &member),
        ));
        let closed_group_id = GroupId(Uuid::from_u128(70_003));
        let mut closed_group = test_group(closed_group_id, &member);
        closed_group.lifecycle = ReplicationGroupLifecycle::Closed {
            successor_group_id: GroupId(Uuid::from_u128(70_004)),
            final_versions: VersionVector::initial(NonZeroUsize::MIN),
        };
        block_on(insert_test_group(&store, closed_group));

        let loaded =
            block_on(load_readable_groups(&store)).expect("several readable groups should load");
        assert_eq!(
            loaded
                .into_iter()
                .map(|group| group.group_id)
                .collect::<Vec<_>>(),
            vec![first_group_id, second_group_id]
        );
    }
}

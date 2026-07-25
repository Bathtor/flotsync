//! REPL and runtime wiring for the replicated checklist runner.

use super::{groups::load_readable_groups, setup::load_checklist_store_setup, *};
use indoc::formatdoc;

pub async fn run_configured_peer(config_path: &Path) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path).await?;
    let persisted_groups = load_readable_groups(setup.store.as_ref()).await?;

    let (listener, listener_receivers) = ChecklistListener::pair();
    let store = setup.store.clone();
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
    let mut repl = ChecklistRepl::new(
        setup.config,
        store,
        replication,
        listener_receivers,
        session,
    );
    let run_result = repl.run().await;
    let shutdown_result = repl.shutdown().await;
    run_result?;
    shutdown_result
}

/// Runtime listener that forwards replication events into the interactive REPL queues.
pub struct ChecklistListener {
    pub batch_sender: Sender<ChecklistListenerBatch>,
    pub invitation_sender: Sender<PendingGroupInvitation>,
}

/// One listener-delivered row batch and the read position that accompanies it.
pub struct ChecklistListenerBatch {
    pub read_token: ReadToken,
    pub changes: Vec<RowChange>,
}

/// Listener queues consumed independently by the interactive REPL.
pub struct ChecklistListenerReceivers {
    pub batches: Receiver<ChecklistListenerBatch>,
    pub invitations: Receiver<PendingGroupInvitation>,
}

/// One listener-mediated invitation and its one-shot runtime response.
pub struct PendingGroupInvitation {
    pub invitation: GroupInvitation,
    pub respond: Box<dyn GroupInvitationResponder>,
}

/// Result of publishing one dirty checklist group during an explicit sync.
#[derive(Debug)]
pub enum ChecklistGroupSyncOutcome {
    /// Every mutation in the prepared group plan was published and marked clean.
    Published {
        /// Group whose dirty rows were published.
        group_id: GroupId,
        /// Number of row mutations included in the publication.
        mutation_count: usize,
    },
    /// Publication failed, leaving this group's dirty rows available for retry.
    Failed {
        /// Group whose publication failed.
        group_id: GroupId,
        /// Concrete replication API failure returned for this group.
        error: ApiError,
    },
}

impl ChecklistGroupSyncOutcome {
    /// Return whether this group was published successfully.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Published { .. })
    }
}

/// Structured result of one all-group checklist synchronisation pass.
#[derive(Debug)]
pub struct ChecklistSyncReport {
    /// Per-group outcomes in deterministic group UUID order.
    pub group_outcomes: Vec<ChecklistGroupSyncOutcome>,
    /// Whether listener draining was deferred because at least one group failed.
    pub listener_drain_deferred: bool,
    /// Listener batches ingested after every group publication succeeded.
    pub listener_batch_count: usize,
    /// Queued checklist events applied after listener ingestion.
    pub applied_event_count: usize,
    /// Process-local dirty items deliberately skipped by replication sync.
    pub dirty_local_item_count: usize,
    /// Groups that remain dirty after this pass, ordered by UUID.
    pub remaining_dirty_groups: Vec<GroupId>,
}

impl ChecklistListener {
    /// Return the listener plus independent data and invitation receivers.
    pub fn pair() -> (Arc<Self>, ChecklistListenerReceivers) {
        let (batch_sender, batch_receiver) = mpsc::channel();
        let (invitation_sender, invitation_receiver) = mpsc::channel();
        (
            Arc::new(Self {
                batch_sender,
                invitation_sender,
            }),
            ChecklistListenerReceivers {
                batches: batch_receiver,
                invitations: invitation_receiver,
            },
        )
    }
}

impl ReplicationEventListener for ChecklistListener {
    fn on_event(
        &self,
        event: ReplicationEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), ListenerError>> + Send + '_>> {
        let batch_sender = self.batch_sender.clone();
        async move {
            match event {
                ReplicationEvent::DataChanged {
                    read_token,
                    mut rows,
                } => {
                    let mut emitted_batch = false;
                    while let Some(batch) = rows.next_batch().await.boxed()? {
                        emitted_batch = true;
                        batch_sender
                            .send(ChecklistListenerBatch {
                                read_token: read_token.clone(),
                                changes: batch.into_iter().collect(),
                            })
                            .map_err(|_| ListenerError::Rejected {
                                message: "checklist listener queue is closed".to_owned(),
                            })?;
                    }
                    if !emitted_batch {
                        // Empty group activations still carry the read token
                        // that makes the new group usable by the application.
                        batch_sender
                            .send(ChecklistListenerBatch {
                                read_token,
                                changes: Vec::new(),
                            })
                            .map_err(|_| ListenerError::Rejected {
                                message: "checklist listener queue is closed".to_owned(),
                            })?;
                    }
                    Ok(())
                }
                ReplicationEvent::GroupInvitation {
                    invitation,
                    respond,
                } => {
                    self.invitation_sender
                        .send(PendingGroupInvitation {
                            invitation,
                            respond,
                        })
                        .map_err(|_| ListenerError::Rejected {
                            message: "checklist invitation queue is closed".to_owned(),
                        })?;
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

/// Interactive checklist state and the runtime handles used by its commands.
pub struct ChecklistRepl {
    pub config: ChecklistAppConfig,
    /// Temporary application-side access to rich group records.
    ///
    /// TODO(flotsync-git-c9m): Replace these direct store reads with the
    /// restricted public application view over the runtime's authoritative
    /// group state. Runtime-changing operations must continue to use
    /// `ReplicationApi` rather than this store handle.
    pub store: Arc<SqliteReplicationStore>,
    pub replication: Arc<dyn ReplicationApi>,
    pub batch_receiver: Receiver<ChecklistListenerBatch>,
    pub invitation_receiver: Receiver<PendingGroupInvitation>,
    pub pending_invitations: Vec<PendingGroupInvitation>,
    pub session: ChecklistSession,
}

/// Group registry, session default, and heterogeneous checklist working set.
pub struct ChecklistSession {
    pub groups: HashMap<GroupId, ReplicationGroupRecord>,
    pub default_group: Option<GroupId>,
    pub working_set: ChecklistWorkingSet,
}

impl ChecklistRepl {
    pub fn new(
        config: ChecklistAppConfig,
        store: Arc<SqliteReplicationStore>,
        replication: Arc<dyn ReplicationApi>,
        listener_receivers: ChecklistListenerReceivers,
        session: ChecklistSession,
    ) -> Self {
        Self {
            config,
            store,
            replication,
            batch_receiver: listener_receivers.batches,
            invitation_receiver: listener_receivers.invitations,
            pending_invitations: Vec::new(),
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

    pub async fn handle_command(
        &mut self,
        command: ChecklistCommand,
    ) -> Result<bool, ReplicatedChecklistError> {
        if !matches!(
            command,
            ChecklistCommand::Keys { .. } | ChecklistCommand::Help | ChecklistCommand::Quit
        ) {
            self.refresh_group_registry().await?;
        }
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
                self.drain_invitation_queue()?;
                self.handle_group_registry_command(command).await?;
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
        let report = self.synchronise_groups().await?;
        println!("{}", self.format_sync_report(&report));
        Ok(())
    }

    /// Publish every dirty real group while preserving failures for display and retry.
    pub async fn synchronise_groups(
        &mut self,
    ) -> Result<ChecklistSyncReport, ReplicatedChecklistError> {
        let mut dirty_groups = self
            .session
            .working_set
            .dirty_group_ids()
            .into_iter()
            .collect::<Vec<_>>();
        dirty_groups.sort();

        let mut group_outcomes = Vec::with_capacity(dirty_groups.len());
        for group_id in dirty_groups {
            let mut plan = self
                .session
                .working_set
                .prepare_group_sync(group_id)
                .context(repl_error::WorkingSetSnafu)?
                .expect("dirty group must produce a non-empty sync plan");
            let mutation_count = plan.mutations.len();
            let changes = std::mem::take(&mut plan.mutations);
            let read_token = self
                .session
                .working_set
                .read_token()
                .context(repl_error::WorkingSetSnafu)?;
            let publish_result = self
                .replication
                .publish_changes(PublishChangesRequest {
                    read_token,
                    changes,
                })
                .await;
            match publish_result {
                Ok(receipt) => {
                    // The working set already reflects this local write, but its read token
                    // does not until we retain the receipt or receive the listener echo.
                    // Retain the receipt immediately so a completed sync cannot leave the
                    // token behind the local state. Carrying it into later requests merely
                    // accumulates independent group positions; it creates no cross-group
                    // causal dependency.
                    self.session.working_set.set_read_token(receipt.read_token);
                    self.session
                        .working_set
                        .finish_successful_group_sync(Some(plan));
                    group_outcomes.push(ChecklistGroupSyncOutcome::Published {
                        group_id,
                        mutation_count,
                    });
                }
                Err(error) => {
                    group_outcomes.push(ChecklistGroupSyncOutcome::Failed { group_id, error });
                }
            }
        }

        let listener_drain_deferred = group_outcomes.iter().any(|outcome| !outcome.is_success());
        let (listener_batch_count, applied_event_count) = if listener_drain_deferred {
            (0, 0)
        } else {
            let listener_batch_count = self.drain_listener_queue()?;
            let applied_event_count = self.session.working_set.drain_queued_events();
            (listener_batch_count, applied_event_count)
        };
        let dirty_local_item_count = self.session.working_set.dirty_local_item_count();
        let mut remaining_dirty_groups = self
            .session
            .working_set
            .dirty_group_ids()
            .into_iter()
            .collect::<Vec<_>>();
        remaining_dirty_groups.sort();
        Ok(ChecklistSyncReport {
            group_outcomes,
            listener_drain_deferred,
            listener_batch_count,
            applied_event_count,
            dirty_local_item_count,
            remaining_dirty_groups,
        })
    }

    /// Format one structured all-group sync report using current group labels.
    #[must_use]
    pub fn format_sync_report(&self, report: &ChecklistSyncReport) -> String {
        let group_outcomes = if report.group_outcomes.is_empty() {
            "    none".to_owned()
        } else {
            report
                .group_outcomes
                .iter()
                .map(|outcome| match outcome {
                    ChecklistGroupSyncOutcome::Published {
                        group_id,
                        mutation_count,
                    } => {
                        let group = self.session.group_label(*group_id);
                        format!("    {group}: published {mutation_count} mutation(s)")
                    }
                    ChecklistGroupSyncOutcome::Failed { group_id, error } => {
                        let group = self.session.group_label(*group_id);
                        format!("    {group}: failed: {error}")
                    }
                })
                .join("\n")
        };
        let listener_batches = if report.listener_drain_deferred {
            "deferred because a group publication failed".to_owned()
        } else {
            report.listener_batch_count.to_string()
        };
        let dirty_group_labels = report
            .remaining_dirty_groups
            .iter()
            .map(|group_id| self.session.group_label(*group_id))
            .join(", ");
        let dirty_group_summary = if dirty_group_labels.is_empty() {
            "none"
        } else {
            &dirty_group_labels
        };
        let heading = if report.listener_drain_deferred {
            "sync incomplete:"
        } else {
            "sync complete:"
        };
        let applied_events = report.applied_event_count;
        let dirty_local = report.dirty_local_item_count;
        formatdoc! {"
            {heading}
              groups:
            {group_outcomes}
              listener batches: {listener_batches}
              applied events: {applied_events}
              unsynchronised local items: {dirty_local}
              dirty groups: {dirty_group_summary}"
        }
    }

    /// Drain queued listener batches into the working set and return the number of batches drained.
    pub fn drain_listener_queue(&mut self) -> Result<usize, ReplicatedChecklistError> {
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
        match self.batch_receiver.try_recv() {
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

pub async fn load_group_snapshot(
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
pub fn join_words(words: Vec<String>) -> String {
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
    use crate::replicated_checklist::{
        ChecklistItem,
        ChecklistRowChange,
        runner::groups::test_support::{
            load_test_runtime_with_groups,
            named_test_group,
            test_app_config,
        },
    };
    use flotsync_replication::{
        RowId,
        RowKey,
        RowMutation,
        providers::VecRowProvider,
        test_support::{publish_changes, snapshot_read_token},
    };

    #[test]
    fn listener_preserves_the_read_token_for_an_empty_data_event() {
        let member = MemberIdentity::from_array(["alice"]);
        let group_id = GroupId::new_random();
        let (_store, runtime, listener, receivers) =
            load_test_runtime_with_groups(&member, [group_id]);
        let read_token = snapshot_read_token(runtime.as_ref(), group_id, checklist_dataset_id());

        block_on(listener.on_event(ReplicationEvent::DataChanged {
            read_token: read_token.clone(),
            rows: Box::new(VecRowProvider::new(Vec::new())),
        }))
        .expect("empty data event should reach the listener");

        let batch = receivers
            .batches
            .try_recv()
            .expect("empty event should retain one token-only batch");
        assert_eq!(batch.read_token, read_token);
        assert!(batch.changes.is_empty());

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn group_snapshot_loading_combines_rows_and_workspace_read_position() {
        let member = MemberIdentity::from_array(["alice"]);
        let first_group = GroupId(Uuid::from_u128(71_011));
        let second_group = GroupId(Uuid::from_u128(71_012));
        let (_store, runtime, _listener, _receivers) =
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
    fn sync_publishes_every_dirty_group_and_skips_local_items() {
        let member = MemberIdentity::from_array(["alice"]);
        let first_group = GroupId(Uuid::from_u128(72_001));
        let second_group = GroupId(Uuid::from_u128(72_002));
        let (store, runtime, _listener, receivers) =
            load_test_runtime_with_groups(&member, [second_group, first_group]);
        let mut working_set = ChecklistWorkingSet::new();
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut working_set,
            first_group,
        ))
        .expect("first group token should load");
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut working_set,
            second_group,
        ))
        .expect("second group token should load");
        let first_item = working_set.add_item(
            ChecklistItemAssociation::Group(first_group),
            "first group item",
        );
        let second_item = working_set.add_item(
            ChecklistItemAssociation::Group(second_group),
            "second group item",
        );
        working_set.add_item(ChecklistItemAssociation::Local, "local item");
        let session = ChecklistSession::new(
            [
                named_test_group(first_group, &member, "first"),
                named_test_group(second_group, &member, "second"),
            ],
            working_set,
        );
        let mut repl = ChecklistRepl::new(
            test_app_config(member),
            store,
            runtime.clone(),
            receivers,
            session,
        );

        let report = block_on(repl.synchronise_groups()).expect("all groups should synchronise");

        assert!(matches!(
            report.group_outcomes.as_slice(),
            [
                ChecklistGroupSyncOutcome::Published {
                    group_id: actual_first,
                    mutation_count: 1,
                },
                ChecklistGroupSyncOutcome::Published {
                    group_id: actual_second,
                    mutation_count: 1,
                },
            ] if *actual_first == first_group && *actual_second == second_group
        ));
        assert!(!report.listener_drain_deferred);
        assert_eq!(report.listener_batch_count, 2);
        assert_eq!(report.applied_event_count, 2);
        assert_eq!(report.dirty_local_item_count, 1);
        assert!(report.remaining_dirty_groups.is_empty());
        assert_eq!(
            repl.format_sync_report(&report),
            "sync complete:\n  groups:\n    first: published 1 mutation(s)\n    second: published 1 mutation(s)\n  listener batches: 2\n  applied events: 2\n  unsynchronised local items: 1\n  dirty groups: none"
        );

        let mut verified = ChecklistWorkingSet::new();
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut verified,
            first_group,
        ))
        .expect("published first group should reload");
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut verified,
            second_group,
        ))
        .expect("published second group should reload");
        assert_eq!(
            verified
                .item(first_item)
                .expect("first published item should exist")
                .text,
            "first group item"
        );
        assert_eq!(
            verified
                .item(second_item)
                .expect("second published item should exist")
                .text,
            "second group item"
        );

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn sync_continues_after_failure_and_defers_listener_draining() {
        let member = MemberIdentity::from_array(["alice"]);
        let unknown_group = GroupId(Uuid::from_u128(72_010));
        let known_group = GroupId(Uuid::from_u128(72_011));
        let (store, runtime, _listener, receivers) =
            load_test_runtime_with_groups(&member, [known_group]);
        let mut working_set = ChecklistWorkingSet::new();
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut working_set,
            known_group,
        ))
        .expect("known group token should load");
        working_set.add_item(
            ChecklistItemAssociation::Group(unknown_group),
            "cannot publish",
        );
        working_set.add_item(
            ChecklistItemAssociation::Group(known_group),
            "still publishes",
        );
        working_set
            .enqueue_checklist_changes(vec![ChecklistRowChange::Delete {
                item_id: ChecklistItemId::group(known_group, RowKey(Uuid::from_u128(72_012))),
            }])
            .expect("non-conflicting listener event should queue");
        let session = ChecklistSession::new(
            [named_test_group(known_group, &member, "known")],
            working_set,
        );
        let mut repl = ChecklistRepl::new(
            test_app_config(member),
            store,
            runtime.clone(),
            receivers,
            session,
        );

        let report = block_on(repl.synchronise_groups())
            .expect("a group publication failure should remain in the report");

        assert!(matches!(
            report.group_outcomes.as_slice(),
            [
                ChecklistGroupSyncOutcome::Failed {
                    group_id: actual_unknown,
                    error: ApiError::ApiExternal { .. },
                },
                ChecklistGroupSyncOutcome::Published {
                    group_id: actual_known,
                    mutation_count: 1,
                },
            ] if *actual_unknown == unknown_group && *actual_known == known_group
        ));
        assert!(report.listener_drain_deferred);
        assert_eq!(report.listener_batch_count, 0);
        assert_eq!(report.applied_event_count, 0);
        assert_eq!(report.remaining_dirty_groups, vec![unknown_group]);
        assert_eq!(repl.session.working_set.queued_event_count(), 1);
        assert!(
            repl.batch_receiver.try_recv().is_ok(),
            "the successful group's listener echo should remain deferred"
        );
        let failed_report = ChecklistSyncReport {
            group_outcomes: vec![ChecklistGroupSyncOutcome::Failed {
                group_id: known_group,
                error: ApiError::RuntimeUnavailable,
            }],
            listener_drain_deferred: true,
            listener_batch_count: 0,
            applied_event_count: 0,
            dirty_local_item_count: 0,
            remaining_dirty_groups: vec![known_group],
        };
        assert_eq!(
            repl.format_sync_report(&failed_report),
            "sync incomplete:\n  groups:\n    known: failed: Replication runtime component became unavailable.\n  listener batches: deferred because a group publication failed\n  applied events: 0\n  unsynchronised local items: 0\n  dirty groups: known"
        );

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }
}

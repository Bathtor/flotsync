//! REPL and runtime wiring for the replicated checklist runner.

use super::{
    diagnostics::ChecklistPeerDiagnostics,
    setup::{
        ChecklistStoreSetup,
        activate_checklist_store_setup,
        create_checklist_store_provisioner,
        load_checklist_config,
        load_existing_checklist_replication_security,
        load_or_create_checklist_replication_security,
        open_checklist_store_provisioner,
    },
    *,
};
use indoc::formatdoc;

pub async fn run_configured_peer(config_path: &Path) -> Result<(), ReplicatedChecklistError> {
    let config = load_checklist_config(config_path)?;
    let (config, local_member, listener_receivers, replication, store) = {
        let stdin = io::stdin();
        let mut input = stdin.lock();
        let mut output = io::stdout();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);
        let setup = prepare_checklist_store_setup(config, &mut dialog).await?;
        let Some(setup) = setup else {
            println!("store setup cancelled");
            return Ok(());
        };

        let (listener, listener_receivers) = ChecklistListener::pair();
        let replication = match load_checklist_runtime(&setup, listener).await {
            Ok(replication) => replication,
            Err(source) => {
                if let Err(error) = setup.store.close().await.context(repl_error::StoreSnafu) {
                    log::error!(
                        "SQLite store closure also failed after replication runtime loading failed: {error}"
                    );
                }
                return Err(source).context(repl_error::LoadRuntimeSnafu);
            }
        };
        (
            setup.config,
            setup.local_member,
            listener_receivers,
            replication,
            setup.store,
        )
    };

    let run_result = run_loaded_checklist_repl(
        config,
        local_member,
        listener_receivers,
        replication.clone(),
    )
    .await;
    let shutdown_result = replication
        .shutdown()
        .await
        .context(repl_error::ReplicationSnafu);
    let close_result = store.close().await.context(repl_error::StoreSnafu);
    preserve_primary_shutdown_error(run_result, shutdown_result, close_result)
}

/// Run the interactive checklist after every application-owned resource has loaded.
async fn run_loaded_checklist_repl(
    config: ChecklistAppConfig,
    local_member: MemberIdentity,
    listener_receivers: ChecklistListenerReceivers,
    replication: Arc<dyn ReplicationApi>,
) -> Result<(), ReplicatedChecklistError> {
    let mut working_set = ChecklistWorkingSet::new();
    let group_state = replication
        .group_state()
        .context(repl_error::ReplicationSnafu)?;
    let readable_group_ids = group_state
        .readable_groups()
        .map(ReplicationGroupView::group_id)
        .collect::<Vec<_>>();
    for group_id in readable_group_ids {
        load_group_snapshot(replication.as_ref(), &mut working_set, group_id).await?;
    }
    let session = ChecklistSession::new(working_set);
    let mut repl = ChecklistRepl::new(
        config,
        local_member,
        replication,
        listener_receivers,
        session,
    );
    repl.run().await
}

/// Return the earliest application failure and log every later cleanup failure.
fn preserve_primary_shutdown_error(
    run_result: Result<(), ReplicatedChecklistError>,
    shutdown_result: Result<(), ReplicatedChecklistError>,
    close_result: Result<(), ReplicatedChecklistError>,
) -> Result<(), ReplicatedChecklistError> {
    let results = [
        ("checklist run", run_result),
        ("replication runtime shutdown", shutdown_result),
        ("SQLite store closure", close_result),
    ];
    let mut primary_error = None;
    for (phase, result) in results {
        if let Err(error) = result {
            if primary_error.is_none() {
                primary_error = Some(error);
            } else {
                log::error!("Secondary failure during {phase}: {error}");
            }
        }
    }
    primary_error.map_or(Ok(()), Err)
}

/// Runtime listener that forwards replication events into the interactive REPL queues.
pub struct ChecklistListener {
    /// Complete data events awaiting application-side ingestion.
    pub event_sender: Sender<ChecklistListenerEvent>,
    /// Invitations awaiting an interactive application decision.
    pub invitation_sender: Sender<PendingGroupInvitation>,
}

/// One complete listener-delivered data event and its resulting read position.
pub struct ChecklistListenerEvent {
    /// Framework lineage needed to interpret predecessor metadata.
    pub lineage: DataChangeLineage,
    /// Read position to merge only after the complete event is applied.
    pub read_token: ReadToken,
    /// Every row change collected from the event's provider pages.
    pub changes: Vec<RowChange>,
}

/// Listener queues consumed independently by the interactive REPL.
pub struct ChecklistListenerReceivers {
    /// Complete data events awaiting application-side ingestion.
    pub events: Receiver<ChecklistListenerEvent>,
    /// Invitations awaiting an interactive application decision.
    pub invitations: Receiver<PendingGroupInvitation>,
}

/// Result of draining complete listener events during one command.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChecklistListenerDrainReport {
    /// Complete framework data events consumed from the listener queue.
    pub event_count: usize,
    /// Checklist events applied to the working set.
    pub applied_event_count: usize,
    /// Reconciliation results retained as dirty successor-group work.
    pub dirty_resolution_count: usize,
    /// Successor groups containing retained resolution work.
    pub dirty_resolution_groups: HashSet<GroupId>,
}

impl ChecklistListenerDrainReport {
    /// Accumulate a later ordered drain into this command's report.
    fn merge(&mut self, later: Self) {
        self.event_count += later.event_count;
        self.applied_event_count += later.applied_event_count;
        self.dirty_resolution_count += later.dirty_resolution_count;
        self.dirty_resolution_groups
            .extend(later.dirty_resolution_groups);
    }
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
    /// Whether listener events arriving during publication were left queued after a failure.
    pub listener_drain_deferred: bool,
    /// Complete listener events ingested during the synchronisation command.
    pub listener_event_count: usize,
    /// Queued checklist events applied after listener ingestion.
    pub applied_event_count: usize,
    /// Reconciliation results retained for publication by a later sync.
    pub dirty_resolution_count: usize,
    /// Process-local dirty items deliberately skipped by replication sync.
    pub dirty_local_item_count: usize,
    /// Groups that remain dirty after this pass, ordered by UUID.
    pub remaining_dirty_groups: Vec<GroupId>,
}

impl ChecklistListener {
    /// Return the listener plus independent data and invitation receivers.
    pub fn pair() -> (Arc<Self>, ChecklistListenerReceivers) {
        let (event_sender, event_receiver) = mpsc::channel();
        let (invitation_sender, invitation_receiver) = mpsc::channel();
        (
            Arc::new(Self {
                event_sender,
                invitation_sender,
            }),
            ChecklistListenerReceivers {
                events: event_receiver,
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
        let event_sender = self.event_sender.clone();
        async move {
            match event {
                ReplicationEvent::DataChanged {
                    lineage,
                    read_token,
                    mut rows,
                } => {
                    let mut changes = Vec::new();
                    while let Some(batch) = rows.next_batch().await.boxed()? {
                        changes.extend(batch);
                    }
                    // Empty group activations still carry the read token that
                    // makes the new group usable by the application.
                    event_sender
                        .send(ChecklistListenerEvent {
                            lineage,
                            read_token,
                            changes,
                        })
                        .map_err(|_| ListenerError::Rejected {
                            message: "checklist listener queue is closed".to_owned(),
                        })?;
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
    /// Current application configuration.
    pub config: ChecklistAppConfig,
    /// Authoritative local identity loaded from the replication store.
    pub local_member: MemberIdentity,
    /// Runtime control and operation-local group-state access.
    pub replication: Arc<dyn ReplicationApi>,
    /// Complete listener events awaiting command-side application.
    pub event_receiver: Receiver<ChecklistListenerEvent>,
    /// Listener-delivered invitations awaiting user decisions.
    pub invitation_receiver: Receiver<PendingGroupInvitation>,
    /// Stable user-facing invitation queue with one-shot responders.
    pub pending_invitations: Vec<PendingGroupInvitation>,
    /// Process-local default selection and checklist working set.
    pub session: ChecklistSession,
}

/// Process-local default selection and heterogeneous checklist working set.
pub struct ChecklistSession {
    /// Writable group selected for unqualified new checklist items.
    pub default_group: Option<GroupId>,
    /// Visible rows, local edits, listener events, and read position.
    pub working_set: ChecklistWorkingSet,
}

/// Valid actions offered for one ambiguous replacement row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReconciliationChoice {
    /// Keep the local value or absence unchanged.
    AcceptLocal,
    /// Keep the framework successor value or absence unchanged.
    AcceptRemote,
    /// Open the local value in the detached item editor.
    EditLocal,
    /// Open the framework successor value in the detached item editor.
    EditRemote,
}

impl ReconciliationChoice {
    /// Labels and values accepted by the reconciliation prompt.
    const CHOICES: &'static [(&'static str, Self)] = &[
        ("accept local", Self::AcceptLocal),
        ("accept remote", Self::AcceptRemote),
        ("edit local", Self::EditLocal),
        ("edit remote", Self::EditRemote),
    ];
}

impl ChecklistRepl {
    /// Build one REPL around a running replication API and listener queues.
    pub fn new(
        config: ChecklistAppConfig,
        local_member: MemberIdentity,
        replication: Arc<dyn ReplicationApi>,
        listener_receivers: ChecklistListenerReceivers,
        session: ChecklistSession,
    ) -> Self {
        Self {
            config,
            local_member,
            replication,
            event_receiver: listener_receivers.events,
            invitation_receiver: listener_receivers.invitations,
            pending_invitations: Vec::new(),
            session,
        }
    }

    /// Load the current operation-local group-state view.
    fn group_state(
        &self,
    ) -> Result<Arc<dyn flotsync_replication::ReplicationGroupSnapshot>, ReplicatedChecklistError>
    {
        self.replication
            .group_state()
            .context(repl_error::ReplicationSnafu)
    }

    /// Count groups whose rows remain application-readable in the current runtime view.
    fn readable_group_count(&self) -> Result<usize, ReplicatedChecklistError> {
        let groups = self.group_state()?;
        Ok(groups.readable_groups().count())
    }

    #[allow(
        clippy::needless_continue,
        reason = "The REPL loop uses explicit continues to make command-processing outcomes obvious."
    )]
    async fn run(&mut self) -> Result<(), ReplicatedChecklistError> {
        println!(
            "replicated checklist: {} readable groups, no default selected",
            self.readable_group_count()?
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
            self.repair_default_group()?;
        }
        match command {
            ChecklistCommand::Keys { command } => {
                keys::run_runtime_key_command(self.replication.as_ref(), command).await?;
                return Ok(true);
            }
            ChecklistCommand::Me => {
                self.print_me()?;
                return Ok(true);
            }
            ChecklistCommand::Peers => {
                self.print_peer_routes().await?;
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
            _ => {
                // All remaining commands operate on the heterogeneous workspace below.
            }
        }
        self.handle_workspace_command(command).await?;
        Ok(true)
    }

    /// Run one command against the heterogeneous checklist workspace.
    async fn handle_workspace_command(
        &mut self,
        command: ChecklistCommand,
    ) -> Result<(), ReplicatedChecklistError> {
        let groups = self.group_state()?;
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
                self.print_item_id(groups.as_ref(), item_id)?;
            }
            ChecklistCommand::Rename { item, text } => {
                let item_id = self.session.resolve_item(groups.as_ref(), &item)?;
                self.session
                    .working_set
                    .rename_item(item_id, join_words(text))
                    .context(repl_error::WorkingSetSnafu)?;
                println!("renamed:");
                self.print_item_id(groups.as_ref(), item_id)?;
            }
            ChecklistCommand::Edit { item, command } => {
                self.handle_edit_command(groups.as_ref(), &item, command)?;
            }
            ChecklistCommand::Tag { command } => match command {
                TagCommand::Add { item, tag } => {
                    let item_id = self.session.resolve_item(groups.as_ref(), &item)?;
                    self.session
                        .working_set
                        .add_tag(item_id, tag)
                        .context(repl_error::WorkingSetSnafu)?;
                    println!("tag added:");
                    self.print_item_id(groups.as_ref(), item_id)?;
                }
                TagCommand::Rm { item, tag } => {
                    let item_id = self.session.resolve_item(groups.as_ref(), &item)?;
                    self.session
                        .working_set
                        .remove_tag(item_id, &tag)
                        .context(repl_error::WorkingSetSnafu)?;
                    println!("tag removed:");
                    self.print_item_id(groups.as_ref(), item_id)?;
                }
            },
            ChecklistCommand::Claim { item } => {
                let item_id = self.session.resolve_item(groups.as_ref(), &item)?;
                self.session
                    .working_set
                    .claim_item(item_id)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("claimed:");
                self.print_item_id(groups.as_ref(), item_id)?;
            }
            ChecklistCommand::Complete { item } => {
                let item_id = self.session.resolve_item(groups.as_ref(), &item)?;
                self.session
                    .working_set
                    .complete_item(item_id)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("completed:");
                self.print_item_id(groups.as_ref(), item_id)?;
            }
            ChecklistCommand::Priority { item, priority } => {
                let item_id = self.session.resolve_item(groups.as_ref(), &item)?;
                self.session
                    .working_set
                    .set_priority(item_id, priority)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("priority set:");
                self.print_item_id(groups.as_ref(), item_id)?;
            }
            ChecklistCommand::Delete { item } => {
                let item_id = self.session.resolve_item(groups.as_ref(), &item)?;
                println!("deleted:");
                self.print_item_id(groups.as_ref(), item_id)?;
                self.session
                    .working_set
                    .delete_item(item_id)
                    .context(repl_error::WorkingSetSnafu)?;
            }
            ChecklistCommand::List => self.print_list(groups.as_ref()),
            ChecklistCommand::Show { item } => self.print_item(groups.as_ref(), &item)?,
            ChecklistCommand::Events { limit } => self.print_events(groups.as_ref(), limit),
            ChecklistCommand::Sync => self.sync().await?,
            ChecklistCommand::Members => self.print_members()?,
            ChecklistCommand::Check => self.check_members().await?,
            ChecklistCommand::Group { .. }
            | ChecklistCommand::Keys { .. }
            | ChecklistCommand::Me
            | ChecklistCommand::Peers
            | ChecklistCommand::Help
            | ChecklistCommand::Quit => {
                unreachable!("commands available without a group are handled before dispatch")
            }
        }
        Ok(())
    }

    /// Apply one item-first note, copy, or move command.
    fn handle_edit_command(
        &mut self,
        groups: &dyn flotsync_replication::ReplicationGroupSnapshot,
        item: &ItemSelector,
        command: EditCommand,
    ) -> Result<(), ReplicatedChecklistError> {
        let item_id = self.session.resolve_item(groups, item)?;
        match command {
            EditCommand::Note => self.edit_note(groups, item_id)?,
            EditCommand::Copy { group } => {
                let group = join_words(group);
                let target = ChecklistSession::resolve_target_association(groups, &group)?;
                let target_id = self
                    .session
                    .working_set
                    .copy_item(item_id, target)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("copied:");
                self.print_item_id(groups, target_id)?;
            }
            EditCommand::Move { group } => {
                let group = join_words(group);
                let target = ChecklistSession::resolve_target_association(groups, &group)?;
                let target_id = self
                    .session
                    .working_set
                    .move_item(item_id, target)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("moved:");
                self.print_item_id(groups, target_id)?;
            }
        }
        Ok(())
    }

    fn edit_note(
        &mut self,
        groups: &dyn flotsync_replication::ReplicationGroupSnapshot,
        item_id: ChecklistItemId,
    ) -> Result<(), ReplicatedChecklistError> {
        let selected = self
            .session
            .working_set
            .require_listed_item(item_id)
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
            .edit_note(item_id, note.trim_end_matches(['\r', '\n']))
            .context(repl_error::WorkingSetSnafu)?;
        println!("note updated:");
        self.print_item_id(groups, item_id)?;
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), ReplicatedChecklistError> {
        let stdin = io::stdin();
        let mut input = stdin.lock();
        let mut output = io::stdout();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);
        let report = self.synchronise_groups(&mut dialog).await?;
        let formatted_report = self.format_sync_report(&report)?;
        println!("{formatted_report}");
        Ok(())
    }

    /// Apply queued events, then publish groups which were dirty when sync began.
    ///
    /// Reconciliation results created by the initial event drain deliberately
    /// remain dirty for the next explicit sync.
    pub async fn synchronise_groups(
        &mut self,
        dialog: &mut ChecklistDialog<'_>,
    ) -> Result<ChecklistSyncReport, ReplicatedChecklistError> {
        let publishable_groups = self.session.working_set.dirty_group_ids();
        let mut drain_report = self.drain_listener_queue(dialog)?;
        let mut dirty_groups = self
            .session
            .working_set
            .dirty_group_ids()
            .into_iter()
            .filter(|group_id| publishable_groups.contains(group_id))
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
        if !listener_drain_deferred {
            let later_drain = self.drain_listener_queue(dialog)?;
            drain_report.merge(later_drain);
        }
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
            listener_event_count: drain_report.event_count,
            applied_event_count: drain_report.applied_event_count,
            dirty_resolution_count: drain_report.dirty_resolution_count,
            dirty_local_item_count,
            remaining_dirty_groups,
        })
    }

    /// Format one structured all-group sync report using current group labels.
    pub fn format_sync_report(
        &self,
        report: &ChecklistSyncReport,
    ) -> Result<String, ReplicatedChecklistError> {
        let groups = self.group_state()?;
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
                        let group = ChecklistSession::group_label(groups.as_ref(), *group_id);
                        format!("    {group}: published {mutation_count} mutation(s)")
                    }
                    ChecklistGroupSyncOutcome::Failed { group_id, error } => {
                        let group = ChecklistSession::group_label(groups.as_ref(), *group_id);
                        format!("    {group}: failed: {error}")
                    }
                })
                .join("\n")
        };
        let listener_events = if report.listener_drain_deferred {
            format!(
                "{} applied before publication; later events deferred because a group publication failed",
                report.listener_event_count
            )
        } else {
            report.listener_event_count.to_string()
        };
        let dirty_group_labels = report
            .remaining_dirty_groups
            .iter()
            .map(|group_id| ChecklistSession::group_label(groups.as_ref(), *group_id))
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
        let resolution_guidance = if report.dirty_resolution_count == 0 {
            String::new()
        } else {
            let resolution_count = report.dirty_resolution_count;
            format!(
                "\n  retained reconciliation changes: {resolution_count}\n  next step: run `sync` again to publish them"
            )
        };
        Ok(formatdoc! {"
            {heading}
              groups:
            {group_outcomes}
              listener events: {listener_events}
              applied events: {applied_events}
              unsynchronised local items: {dirty_local}
              dirty groups: {dirty_group_summary}{resolution_guidance}"
        })
    }

    /// Drain complete listener events in order, resolving replacements before token merge.
    pub fn drain_listener_queue(
        &mut self,
        dialog: &mut ChecklistDialog<'_>,
    ) -> Result<ChecklistListenerDrainReport, ReplicatedChecklistError> {
        let groups = self.group_state()?;
        let mut report = ChecklistListenerDrainReport::default();
        while let Some(event) = self.receive_listener_event()? {
            ChecklistSession::validate_listener_changes(
                groups.as_ref(),
                event.lineage,
                &event.changes,
            )?;
            match event.lineage {
                DataChangeLineage::Update => {
                    self.session
                        .working_set
                        .enqueue_row_changes(event.changes)
                        .context(repl_error::WorkingSetSnafu)?;
                    report.applied_event_count += self.session.working_set.drain_queued_events();
                }
                DataChangeLineage::GroupReplacement { migration_id } => {
                    let mut plan = self
                        .session
                        .working_set
                        .prepare_group_replacement(migration_id, event.changes)
                        .context(repl_error::WorkingSetSnafu)?;
                    Self::resolve_group_replacement(dialog, &mut plan)?;
                    let outcome = plan.commit(&mut self.session.working_set);
                    report.applied_event_count += 1;
                    report.dirty_resolution_count += outcome.dirty_resolution_count;
                    if outcome.dirty_resolution_count != 0 {
                        report
                            .dirty_resolution_groups
                            .insert(migration_id.new_group_id);
                    }
                }
            }
            self.session.working_set.merge_read_token(event.read_token);
            report.event_count += 1;
        }
        Ok(report)
    }

    /// Explain retained local resolutions which require another explicit sync.
    pub(super) fn write_reconciliation_follow_up(
        &self,
        dialog: &mut ChecklistDialog<'_>,
        report: &ChecklistListenerDrainReport,
    ) -> Result<(), ReplicatedChecklistError> {
        if report.dirty_resolution_count == 0 {
            return Ok(());
        }
        let groups = self.group_state()?;
        let labels = report
            .dirty_resolution_groups
            .iter()
            .map(|group_id| ChecklistSession::group_label(groups.as_ref(), *group_id))
            .sorted()
            .join(", ");
        write!(
            dialog.output,
            "{} reconciliation change(s) were retained as local changes in {labels}.
Run `sync` again to publish them.
",
            report.dirty_resolution_count
        )
        .context(repl_error::IoSnafu {
            action: "writing reconciliation follow-up",
        })?;
        Ok(())
    }

    /// Guide the user through every ambiguous row before a replacement is committed.
    fn resolve_group_replacement(
        dialog: &mut ChecklistDialog<'_>,
        plan: &mut ChecklistReplacementPlan,
    ) -> Result<(), ReplicatedChecklistError> {
        if plan.reconciliations().is_empty() {
            return Ok(());
        }
        let migration_id = plan.migration_id();
        writeln!(
            dialog.output,
            "group replacement {} -> {} needs reconciliation:",
            migration_id.old_group_id, migration_id.new_group_id
        )
        .context(repl_error::IoSnafu {
            action: "writing reconciliation heading",
        })?;

        let reconciliation_count = plan.reconciliations().len();
        for index in 0..reconciliation_count {
            let reconciliation = plan.reconciliations()[index].clone();
            Self::write_reconciliation(dialog, index, reconciliation_count, &reconciliation)?;
            let selected = Self::read_reconciliation_choice(dialog, &reconciliation)?;
            plan.resolve_next(selected);
        }
        Ok(())
    }

    /// Display one complete local-versus-remote decision and its evidence.
    fn write_reconciliation(
        dialog: &mut ChecklistDialog<'_>,
        index: usize,
        count: usize,
        reconciliation: &ChecklistReconciliation,
    ) -> Result<(), ReplicatedChecklistError> {
        let local = format_reconciliation_candidate("local", reconciliation.local.as_ref());
        let remote = format_reconciliation_candidate("remote", reconciliation.remote.as_ref());
        let differences = match reconciliation.differing_fields.as_deref() {
            None => "comparison unavailable".to_owned(),
            Some([]) => "none".to_owned(),
            Some(differences) => differences
                .iter()
                .map(RowFieldDifference::field_name)
                .join(", "),
        };
        let evidence = format_reconciliation_evidence(reconciliation.evidence);
        write!(
            dialog.output,
            "reconciliation {}/{}:
  old: {:?}
  successor: {:?}
{local}{remote}  differing fields: {differences}
{evidence}",
            index + 1,
            count,
            reconciliation.old_item_id,
            reconciliation.new_item_id,
        )
        .context(repl_error::IoSnafu {
            action: "writing reconciliation",
        })?;
        Ok(())
    }

    /// Read one of the four agreed choices, retrying invalid or unavailable input.
    #[allow(
        clippy::needless_continue,
        reason = "Explicit retries keep unavailable reconciliation choices in the choice loop."
    )]
    fn read_reconciliation_choice(
        dialog: &mut ChecklistDialog<'_>,
        reconciliation: &ChecklistReconciliation,
    ) -> Result<Option<ChecklistItem>, ReplicatedChecklistError> {
        'choice: loop {
            let choice = dialog.read_choice(
                "resolution [accept local/accept remote/edit local/edit remote]> ",
                "reading reconciliation choice",
                ReconciliationChoice::CHOICES,
            )?;
            match choice {
                ReconciliationChoice::AcceptLocal => return Ok(reconciliation.local.clone()),
                ReconciliationChoice::AcceptRemote => return Ok(reconciliation.remote.clone()),
                ReconciliationChoice::EditLocal => {
                    if let Some(local) = &reconciliation.local {
                        return Self::edit_reconciliation_item(dialog, local.clone()).map(Some);
                    }
                    writeln!(dialog.output, "local is absent and cannot be edited").context(
                        repl_error::IoSnafu {
                            action: "writing unavailable reconciliation choice",
                        },
                    )?;
                    continue 'choice;
                }
                ReconciliationChoice::EditRemote => {
                    if let Some(remote) = &reconciliation.remote {
                        return Self::edit_reconciliation_item(dialog, remote.clone()).map(Some);
                    }
                    writeln!(dialog.output, "remote is absent and cannot be edited").context(
                        repl_error::IoSnafu {
                            action: "writing unavailable reconciliation choice",
                        },
                    )?;
                    continue 'choice;
                }
            }
        }
    }

    /// Edit a selected reconciliation base until the user accepts the result.
    fn edit_reconciliation_item(
        dialog: &mut ChecklistDialog<'_>,
        mut item: ChecklistItem,
    ) -> Result<ChecklistItem, ReplicatedChecklistError> {
        'editor: loop {
            let command = dialog.read_line(
                "edit [text/note/tags/status/priority/accept]> ",
                "reading reconciliation edit",
            )?;
            let (field, value) = command
                .split_once(char::is_whitespace)
                .map_or((command.trim(), ""), |(field, value)| {
                    (field.trim(), value.trim())
                });
            let edit = match field.to_ascii_lowercase().as_str() {
                "accept" if value.is_empty() => return Ok(item),
                "text" => ChecklistItemEdit::Text(value.to_owned()),
                "note" => ChecklistItemEdit::Note(value.to_owned()),
                "tags" => {
                    ChecklistItemEdit::Tags(value.split_whitespace().map(str::to_owned).collect())
                }
                "status" => {
                    let Some(status) = ChecklistStatus::from_schema_value(value) else {
                        writeln!(dialog.output, "status must be open, in_progress, or done")
                            .context(repl_error::IoSnafu {
                                action: "writing reconciliation edit help",
                            })?;
                        continue 'editor;
                    };
                    ChecklistItemEdit::Status(status)
                }
                "priority" => {
                    let Ok(priority) = value.parse::<u8>() else {
                        writeln!(dialog.output, "priority must be an integer from 0 to 255")
                            .context(repl_error::IoSnafu {
                                action: "writing reconciliation edit help",
                            })?;
                        continue 'editor;
                    };
                    ChecklistItemEdit::Priority(priority)
                }
                _ => {
                    writeln!(
                        dialog.output,
                        "enter a field and value, or `accept` to use the edited row"
                    )
                    .context(repl_error::IoSnafu {
                        action: "writing reconciliation edit help",
                    })?;
                    continue 'editor;
                }
            };
            item.apply_edit(edit);
            write!(
                dialog.output,
                "{}",
                format_reconciliation_candidate("edited", Some(&item))
            )
            .context(repl_error::IoSnafu {
                action: "writing edited reconciliation candidate",
            })?;
        }
    }

    /// Return one queued listener event, or `None` when the listener queue is currently empty.
    fn receive_listener_event(
        &self,
    ) -> Result<Option<ChecklistListenerEvent>, ReplicatedChecklistError> {
        match self.event_receiver.try_recv() {
            Ok(changes) => Ok(Some(changes)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => repl_error::ListenerQueueClosedSnafu.fail(),
        }
    }

    fn print_list(&self, groups: &dyn flotsync_replication::ReplicationGroupSnapshot) {
        let items = self.session.working_set.listed_items();
        if items.is_empty() {
            println!("checklist is empty");
        } else {
            for item in items {
                Self::print_row(groups, &item);
            }
        }
    }

    fn print_item(
        &self,
        groups: &dyn flotsync_replication::ReplicationGroupSnapshot,
        item: &ItemSelector,
    ) -> Result<(), ReplicatedChecklistError> {
        let item_id = self.session.resolve_item(groups, item)?;
        let listed = self
            .session
            .working_set
            .require_listed_item(item_id)
            .context(repl_error::WorkingSetSnafu)?;
        println!("index: {}", listed.index);
        println!(
            "item: {}",
            ChecklistSession::item_reference(groups, listed.item_id)
        );
        println!(
            "association: {}",
            ChecklistSession::association_label(groups, listed.item_id.association)
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

    fn print_events(
        &self,
        groups: &dyn flotsync_replication::ReplicationGroupSnapshot,
        limit: Option<usize>,
    ) {
        let events = self.session.working_set.events();
        for event in events.iter().rev().take(limit.unwrap_or(usize::MAX)) {
            println!("event {}:", format_timestamp(event.timestamp));
            for change in &event.changes {
                let item_id = change.item_id();
                println!(
                    "  {}: {change:?}",
                    ChecklistSession::item_reference(groups, item_id)
                );
            }
        }
        if events.is_empty() {
            println!("no events");
        }
    }

    fn print_me(&self) -> Result<(), ReplicatedChecklistError> {
        let groups = self.group_state()?;
        println!("member: {}", self.local_member);
        match self.session.default_group {
            Some(group_id) => println!(
                "default group: {}",
                ChecklistSession::group_label(groups.as_ref(), group_id)
            ),
            None => println!("default group: none"),
        }
        let readable_group_count = groups.readable_groups().count();
        println!("readable groups: {readable_group_count}");
        println!("store: {}", self.config.store_path.display());
        println!("config: {}", self.config.source_path.display());
        println!(
            "dirty rows: {}, queued events: {}",
            self.session.working_set.dirty_row_count(),
            self.session.working_set.queued_event_count()
        );
        Ok(())
    }

    /// Query and print one action-driven peer-route diagnostic snapshot.
    async fn print_peer_routes(&self) -> Result<(), ReplicatedChecklistError> {
        let diagnostics_api = self.replication.diagnostics();
        let snapshot = diagnostics_api
            .peer_routes()
            .await
            .context(repl_error::ReplicationSnafu)?;
        let groups = self.group_state()?;
        let report = ChecklistPeerDiagnostics::new(snapshot, groups.as_ref());
        println!("{report}");
        Ok(())
    }

    /// Print one row from an already-resolved workspace identity.
    fn print_item_id(
        &self,
        groups: &dyn flotsync_replication::ReplicationGroupSnapshot,
        item_id: ChecklistItemId,
    ) -> Result<(), ReplicatedChecklistError> {
        let selected = self.session.working_set.listed_item(item_id).ok_or(
            ReplicatedChecklistError::UnknownItemReference {
                selector: ItemSelector::Qualified {
                    association: match item_id.association {
                        ChecklistItemAssociation::Local => ItemAssociationSelector::Local,
                        ChecklistItemAssociation::Group(group_id) => {
                            ItemAssociationSelector::Group(group_id.to_string())
                        }
                    },
                    row_key: item_id.row_key,
                },
            },
        )?;
        Self::print_row(groups, &selected);
        Ok(())
    }

    /// Print one compact row with its workspace association.
    fn print_row(
        groups: &dyn flotsync_replication::ReplicationGroupSnapshot,
        item: &ListedChecklistItem<'_>,
    ) {
        let tags = item.item.formatted_tags();
        let reference = ChecklistSession::item_reference(groups, item.item_id);
        println!(
            "{:>3}. [{}] p{} edits={} {} ({reference}) {tags}",
            item.index, item.item.status, item.item.priority, item.item.edit_count, item.item.text,
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

/// Open an existing checklist store or create a new one only after explicit confirmation.
async fn prepare_checklist_store_setup(
    config: ChecklistAppConfig,
    dialog: &mut ChecklistDialog<'_>,
) -> Result<Option<ChecklistStoreSetup>, ReplicatedChecklistError> {
    let store_exists = config
        .store_path
        .try_exists()
        .context(repl_error::IoSnafu {
            action: "checking checklist store path",
        })?;
    let (provisioning, provisioning_confirmed) = if store_exists {
        (open_checklist_store_provisioner(config).await?, false)
    } else {
        let prompt = first_run_provisioning_prompt(&config.store_path);
        let accepted = dialog.confirm(&prompt)?;
        if accepted {
            (create_checklist_store_provisioner(config).await?, true)
        } else {
            println!("local identity provisioning declined; no store setup was created");
            return Ok(None);
        }
    };

    let existing_member = provisioning
        .store
        .local_member_identity()
        .await
        .context(repl_error::StoreSnafu)?;
    let replication_security = if let Some(member_id) = existing_member {
        println!("local identity loaded: {member_id}");
        load_existing_checklist_replication_security(&provisioning.config)
            .context(repl_error::LocalStoreSecretSnafu)?
    } else {
        let accepted = if provisioning_confirmed {
            true
        } else {
            let prompt = unprovisioned_store_prompt(&provisioning.config.store_path);
            dialog.confirm(&prompt)?
        };
        if !accepted {
            println!("local identity provisioning declined");
            return Ok(None);
        }
        let local_member = dialog.read_member_identity("local member identity> ")?;
        let replication_security =
            load_or_create_checklist_replication_security(&provisioning.config)
                .context(repl_error::LocalStoreSecretSnafu)?;
        let provisioned =
            provision_local_identity(&provisioning.store, local_member, &replication_security)
                .await
                .context(repl_error::ProvisionLocalIdentitySnafu)?;
        println!("local identity provisioned: {}", provisioned.member_id());
        replication_security
    };

    let setup = activate_checklist_store_setup(provisioning, replication_security).await?;
    Ok(Some(setup))
}

/// Load the configured replication runtime once without applying recovery policy.
async fn load_checklist_runtime(
    setup: &ChecklistStoreSetup,
    listener: Arc<ChecklistListener>,
) -> Result<Arc<dyn ReplicationApi>, LoadError> {
    load_replication_runtime_with_runtime_config_toml(
        checklist_application_id(),
        &CHECKLIST_APPLICATION_SCHEMAS,
        setup.store.clone(),
        listener,
        ReplicationConfig::default(),
        setup.replication_security.clone(),
        &setup.config.runtime_config_toml,
    )
    .await
}

/// Build the confirmation prompt shown before creating any first-run setup state.
fn first_run_provisioning_prompt(store_path: &Path) -> String {
    format!(
        "Checklist store {} does not exist. Create it and provision a local identity and keys?",
        store_path.display()
    )
}

/// Build the confirmation prompt for an existing store without local identity material.
fn unprovisioned_store_prompt(store_path: &Path) -> String {
    format!(
        "Checklist store {} has no local identity. Provision a local identity and keys?",
        store_path.display()
    )
}

/// Format one complete reconciliation candidate or explicit row absence.
fn format_reconciliation_candidate(label: &str, item: Option<&ChecklistItem>) -> String {
    if let Some(item) = item {
        let tags = item.formatted_tags();
        formatdoc! {"
              {label}:
                text: {text}
                note: {note}
                tags: {tags}
                status: {status}
                priority: {priority}
                edit_count: {edit_count}
            ",
            text = item.text,
            note = item.note,
            status = item.status,
            priority = item.priority,
            edit_count = item.edit_count,
        }
    } else {
        format!("  {label}: absent\n")
    }
}

/// Format user-facing facts about whether the replacement included the old row.
fn format_reconciliation_evidence(evidence: Option<PreviousRowEvidence>) -> String {
    let Some(evidence) = evidence else {
        return "  old-row evidence: unavailable\n".to_owned();
    };
    let creator = match evidence.creator {
        Some(PreviousRowCreator::Local) => "this member",
        Some(PreviousRowCreator::Other) => "another member",
        None => "unknown",
    };
    let creation = cut_relation_label(evidence.creation);
    let last_state = cut_relation_label(evidence.last_state);
    formatdoc! {"
          old-row creator: {creator}
          row creation: {creation}
          latest old-row state: {last_state}
        "}
}

/// Describe whether one old-group fact reached the accepted replacement input.
const fn cut_relation_label(relation: Option<AcceptedCutRelation>) -> &'static str {
    match relation {
        Some(AcceptedCutRelation::Included) => "included in the replacement",
        Some(AcceptedCutRelation::NotIncluded) => {
            "newer than the accepted cut and not included in the replacement"
        }
        None => "unknown",
    }
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
        FIELD_TEXT,
        runner::groups::test_support::{
            load_test_runtime_with_group_records,
            load_test_runtime_with_groups,
            named_test_group,
            test_app_config,
        },
    };
    use flotsync_replication::{
        BatchProvider,
        MigrationId,
        PreviousRow,
        PreviousRowAbsence,
        ReplicationStore,
        RowChangeBatch,
        RowChangeKind,
        RowId,
        RowKey,
        RowMutation,
        RowValues,
        providers::VecRowProvider,
        test_support::{publish_changes, snapshot_read_token},
    };
    use flotsync_security::{LocalStoreSecretError, install_local_store_secret_test_store};
    use futures_util::{
        FutureExt,
        future::{self, BoxFuture},
    };
    use std::{borrow::Cow, collections::VecDeque, io::Cursor};

    /// Deterministic provider which emits one pre-built row-change page per call.
    struct PagedRowProvider {
        /// Remaining pages in delivery order.
        pages: VecDeque<RowChangeBatch>,
    }

    impl BatchProvider for PagedRowProvider {
        type Batch = RowChangeBatch;

        fn new_batch(&self) -> Self::Batch {
            RowChangeBatch::new()
        }

        fn fill_batch(
            &mut self,
            _reuse: Self::Batch,
        ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
            future::ready(Ok(self.pages.pop_front())).boxed()
        }
    }

    /// Run one sync with an empty dialog which must not be read by these tests.
    fn synchronise_test_groups(
        repl: &mut ChecklistRepl,
    ) -> Result<ChecklistSyncReport, ReplicatedChecklistError> {
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);
        block_on(repl.synchronise_groups(&mut dialog))
    }

    /// Build one wizard decision with both candidate rows available.
    fn test_reconciliation() -> ChecklistReconciliation {
        ChecklistReconciliation {
            old_item_id: ChecklistItemId::group(
                GroupId(Uuid::from_u128(50)),
                RowKey(Uuid::from_u128(51)),
            ),
            new_item_id: ChecklistItemId::group(
                GroupId(Uuid::from_u128(52)),
                RowKey(Uuid::from_u128(51)),
            ),
            local: Some(ChecklistItem::new("local")),
            remote: Some(ChecklistItem::new("remote")),
            differing_fields: Some(
                vec![RowFieldDifference::ValueChanged {
                    field_name: Cow::Borrowed(FIELD_TEXT),
                }]
                .into_boxed_slice(),
            ),
            evidence: None,
        }
    }

    /// Build one replacement collision between a dirty old-group insert and a successor row.
    fn replacement_collision_change(new_group_id: GroupId, row_key: RowKey) -> RowChange {
        let remote = ChecklistItem::new("remote");
        let row = RowValues::from_fields_unchecked(remote.to_row_values_patch().fields);
        RowChange {
            previous: PreviousRow::Absent(PreviousRowAbsence::NotStored),
            change: RowChangeKind::Upsert {
                row_id: RowId::new(new_group_id, checklist_dataset_id(), row_key),
                row: Arc::new(row),
                previous_value_differences: None,
            },
        }
    }

    #[test]
    fn reconciliation_wizard_accepts_either_candidate_unchanged() {
        let reconciliation = test_reconciliation();
        for (answer, expected) in [
            ("accept local\n", reconciliation.local.clone()),
            ("accept remote\n", reconciliation.remote.clone()),
        ] {
            let mut input = Cursor::new(answer.as_bytes());
            let mut output = Vec::new();
            let mut dialog = ChecklistDialog::new(&mut input, &mut output);

            let selected = ChecklistRepl::read_reconciliation_choice(&mut dialog, &reconciliation)
                .expect("accept choice should resolve");

            assert_eq!(selected, expected);
        }
    }

    #[test]
    fn reconciliation_wizard_edits_local_or_remote_as_the_selected_base() {
        let reconciliation = test_reconciliation();
        for (answer, expected_text) in [
            ("edit local\ntext merged local\naccept\n", "merged local"),
            ("edit remote\ntext merged remote\naccept\n", "merged remote"),
        ] {
            let mut input = Cursor::new(answer.as_bytes());
            let mut output = Vec::new();
            let mut dialog = ChecklistDialog::new(&mut input, &mut output);

            let selected = ChecklistRepl::read_reconciliation_choice(&mut dialog, &reconciliation)
                .expect("edit choice should resolve")
                .expect("edited candidate remains visible");

            assert_eq!(selected.text, expected_text);
            assert_eq!(selected.edit_count, 2);
        }
    }

    #[test]
    fn reconciliation_wizard_explains_absent_edit_and_retries() {
        let mut reconciliation = test_reconciliation();
        reconciliation.local = None;
        let mut input = Cursor::new(b"not a choice\nedit local\naccept remote\n");
        let mut output = Vec::new();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);

        let selected = ChecklistRepl::read_reconciliation_choice(&mut dialog, &reconciliation)
            .expect("wizard should retry unavailable edit");

        assert_eq!(selected, reconciliation.remote);
        let output = String::from_utf8(output).expect("wizard output should be UTF-8");
        assert!(output.contains("enter one of: `accept local`"));
        assert!(output.contains("local is absent and cannot be edited"));
    }

    #[test]
    fn replacement_input_eof_leaves_the_working_set_and_token_unchanged() {
        let member = MemberIdentity::from_array(["alice"]);
        let old_group_id = GroupId(Uuid::from_u128(73_001));
        let new_group_id = GroupId(Uuid::from_u128(73_002));
        let row_key = RowKey(Uuid::from_u128(73_003));
        let (_store, runtime, listener, receivers) =
            load_test_runtime_with_groups(&member, [new_group_id]);
        let read_token =
            snapshot_read_token(runtime.as_ref(), new_group_id, checklist_dataset_id());
        block_on(listener.on_event(ReplicationEvent::DataChanged {
            lineage: DataChangeLineage::GroupReplacement {
                migration_id: MigrationId {
                    old_group_id,
                    new_group_id,
                },
            },
            read_token,
            rows: Box::new(VecRowProvider::new(vec![replacement_collision_change(
                new_group_id,
                row_key,
            )])),
        }))
        .expect("replacement should reach the listener");
        let old_item_id = ChecklistItemId::group(old_group_id, row_key);
        let mut working_set = ChecklistWorkingSet::new();
        working_set.add_item_with_id(old_item_id, "local");
        let mut repl = ChecklistRepl::new(
            test_app_config(),
            member,
            runtime.clone(),
            receivers,
            ChecklistSession::new(working_set),
        );
        let mut input = Cursor::new(Vec::<u8>::new());
        let mut output = Vec::new();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);

        let result = repl.drain_listener_queue(&mut dialog);

        assert!(matches!(
            result,
            Err(ReplicatedChecklistError::EndOfInput {
                action: "reading reconciliation choice"
            })
        ));
        assert_eq!(
            repl.session
                .working_set
                .item(old_item_id)
                .map(|item| item.text.as_str()),
            Some("local")
        );
        assert!(
            repl.session
                .working_set
                .item(ChecklistItemId::group(new_group_id, row_key))
                .is_none()
        );
        assert!(matches!(
            repl.session.working_set.read_token(),
            Err(ChecklistWorkingSetError::MissingReadToken)
        ));

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn retained_replacement_choice_merges_token_and_prints_next_sync_guidance() {
        let member = MemberIdentity::from_array(["alice"]);
        let old_group_id = GroupId(Uuid::from_u128(73_004));
        let new_group_id = GroupId(Uuid::from_u128(73_005));
        let row_key = RowKey(Uuid::from_u128(73_006));
        let (_store, runtime, listener, receivers) =
            load_test_runtime_with_groups(&member, [new_group_id]);
        let read_token =
            snapshot_read_token(runtime.as_ref(), new_group_id, checklist_dataset_id());
        block_on(listener.on_event(ReplicationEvent::DataChanged {
            lineage: DataChangeLineage::GroupReplacement {
                migration_id: MigrationId {
                    old_group_id,
                    new_group_id,
                },
            },
            read_token: read_token.clone(),
            rows: Box::new(VecRowProvider::new(vec![replacement_collision_change(
                new_group_id,
                row_key,
            )])),
        }))
        .expect("replacement should reach the listener");
        let old_item_id = ChecklistItemId::group(old_group_id, row_key);
        let mut working_set = ChecklistWorkingSet::new();
        working_set.add_item_with_id(old_item_id, "local");
        let mut repl = ChecklistRepl::new(
            test_app_config(),
            member,
            runtime.clone(),
            receivers,
            ChecklistSession::new(working_set),
        );
        let mut input = Cursor::new(b"accept local\n");
        let mut output = Vec::new();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);

        let report = repl
            .drain_listener_queue(&mut dialog)
            .expect("local replacement choice should commit");
        repl.write_reconciliation_follow_up(&mut dialog, &report)
            .expect("follow-up should render");

        assert_eq!(report.dirty_resolution_count, 1);
        assert_eq!(
            repl.session
                .working_set
                .read_token()
                .expect("committed replacement should merge its read token"),
            read_token
        );
        let output = String::from_utf8(output).expect("dialog output should be UTF-8");
        assert!(output.contains("1 reconciliation change(s) were retained"));
        assert!(output.contains("Run `sync` again to publish them."));

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn shutdown_error_preservation_returns_the_earliest_failure() {
        let result = preserve_primary_shutdown_error(
            Err(ReplicatedChecklistError::ListenerQueueClosed),
            Err(ReplicatedChecklistError::NoDefaultGroup),
            Err(ReplicatedChecklistError::UnknownGroup {
                selector: "closed store".to_owned(),
            }),
        );

        assert!(matches!(
            result,
            Err(ReplicatedChecklistError::ListenerQueueClosed)
        ));
    }

    #[test]
    fn shutdown_error_preservation_falls_through_to_cleanup_failures() {
        let shutdown_result = preserve_primary_shutdown_error(
            Ok(()),
            Err(ReplicatedChecklistError::NoDefaultGroup),
            Err(ReplicatedChecklistError::UnknownGroup {
                selector: "closed store".to_owned(),
            }),
        );
        assert!(matches!(
            shutdown_result,
            Err(ReplicatedChecklistError::NoDefaultGroup)
        ));

        let close_result = preserve_primary_shutdown_error(
            Ok(()),
            Ok(()),
            Err(ReplicatedChecklistError::UnknownGroup {
                selector: "closed store".to_owned(),
            }),
        );
        assert!(matches!(
            close_result,
            Err(ReplicatedChecklistError::UnknownGroup { selector })
                if selector == "closed store"
        ));
    }

    #[test]
    fn shutdown_error_preservation_succeeds_when_every_phase_succeeds() {
        assert!(preserve_primary_shutdown_error(Ok(()), Ok(()), Ok(())).is_ok());
    }

    /// Build one isolated checklist config for startup recovery tests.
    fn startup_test_config(test_name: &str) -> (PathBuf, ChecklistAppConfig) {
        let test_id = Uuid::new_v4();
        let test_root =
            std::env::temp_dir().join(format!("flotsync-checklist-startup-{test_name}-{test_id}"));
        let config = ChecklistAppConfig {
            source_path: test_root.join("alice.toml"),
            runtime_config_toml: String::new(),
            store_path: test_root.join("alice.sqlite"),
            store_secret_profile: flotsync_replication::LocalStoreSecretProfile::new(format!(
                "unsafe:startup-{test_id}"
            ))
            .expect("test profile should build"),
        };
        (test_root, config)
    }

    #[test]
    fn new_store_decline_leaves_no_setup_state() {
        install_local_store_secret_test_store().expect("sample keyring should install");
        let (test_root, mut config) = startup_test_config("new-decline");
        let profile = flotsync_replication::LocalStoreSecretProfile::new(format!(
            "managed-startup-decline-{}",
            Uuid::new_v4()
        ))
        .expect("managed test profile should build");
        config.store_secret_profile = profile.clone();
        let store_path = config.store_path.clone();
        let expected_application_id = checklist_application_id();
        let expected_prompt = format!(
            "Checklist store {} does not exist. Create it and provision a local identity and keys?",
            store_path.display()
        );
        let mut input = Cursor::new(b"no\n".as_slice());
        let mut output = Vec::new();
        let mut confirmation = ChecklistDialog::new(&mut input, &mut output);

        let setup = block_on(prepare_checklist_store_setup(config, &mut confirmation))
            .expect("declined setup should succeed");

        assert!(setup.is_none());
        assert_eq!(
            String::from_utf8(output).expect("confirmation prompt should be UTF-8"),
            format!("{expected_prompt} [y/N] ")
        );
        assert!(!store_path.exists());
        assert!(!test_root.exists());
        let error = ReplicationSecuritySecrets::load_local(&expected_application_id, &profile)
            .expect_err("declining first-run setup must not create a local store secret");
        assert!(matches!(
            error,
            LoadSecurityError::LocalStoreSecret { source }
                if matches!(
                    source.as_ref(),
                    LocalStoreSecretError::Missing {
                        application_id,
                        profile: missing_profile,
                    } if application_id == &expected_application_id && missing_profile == &profile
                )
        ));
    }

    #[test]
    fn new_store_accept_provisions_identity_before_returning_setup() {
        let (test_root, config) = startup_test_config("new-accept");
        let member_id = MemberIdentity::from_array(["alice"]);
        let expected_prompt = format!(
            "Checklist store {} does not exist. Create it and provision a local identity and keys?",
            config.store_path.display()
        );
        let mut input = Cursor::new(b"yes\nalice\n".as_slice());
        let mut output = Vec::new();
        let mut confirmation = ChecklistDialog::new(&mut input, &mut output);

        let setup = block_on(prepare_checklist_store_setup(config, &mut confirmation))
            .expect("accepted setup should succeed")
            .expect("accepted setup should be returned");
        assert_eq!(
            String::from_utf8(output).expect("confirmation prompt should be UTF-8"),
            format!("{expected_prompt} [y/N] local member identity> ")
        );
        assert_eq!(setup.local_member, member_id);

        let mut transaction =
            block_on(setup.store.begin_read_transaction()).expect("read transaction should start");
        let private_keys = block_on(transaction.load_local_member_private_keys(&member_id))
            .expect("private keys should load");
        let public_keys = block_on(transaction.load_member_public_keys_for_member(&member_id))
            .expect("public keys should load");
        block_on(transaction.release()).expect("transaction should release");
        let Some(private_keys) = private_keys else {
            panic!("private keys should be stored");
        };
        assert_eq!(private_keys.member_id, member_id);
        let [public_keys] = public_keys.as_slice() else {
            panic!("exactly one public-key binding should be stored: {public_keys:?}");
        };
        assert_eq!(public_keys.key_id.member_id, member_id);

        block_on(setup.store.close()).expect("test SQLite store should close");
        drop(setup);
        std::fs::remove_dir_all(test_root).expect("test directory should be removed");
    }

    #[test]
    fn existing_unprovisioned_store_can_be_declined() {
        let (test_root, config) = startup_test_config("existing-decline");
        let provisioning = block_on(create_checklist_store_provisioner(config.clone()))
            .expect("empty store should be created");
        drop(provisioning);
        let expected_prompt = format!(
            "Checklist store {} has no local identity. Provision a local identity and keys?",
            config.store_path.display()
        );
        let mut input = Cursor::new(b"no\n".as_slice());
        let mut output = Vec::new();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);

        let setup = block_on(prepare_checklist_store_setup(config, &mut dialog))
            .expect("declined provisioning should succeed");
        assert!(setup.is_none());
        assert_eq!(
            String::from_utf8(output).expect("confirmation prompt should be UTF-8"),
            format!("{expected_prompt} [y/N] ")
        );
        std::fs::remove_dir_all(test_root).expect("test directory should be removed");
    }

    #[test]
    fn existing_unprovisioned_store_can_be_provisioned() {
        let (test_root, config) = startup_test_config("existing-accept");
        let provisioning = block_on(create_checklist_store_provisioner(config.clone()))
            .expect("empty store should be created");
        drop(provisioning);
        let expected_prompt = format!(
            "Checklist store {} has no local identity. Provision a local identity and keys?",
            config.store_path.display()
        );
        let mut input = Cursor::new(b"yes\nbob\n".as_slice());
        let mut output = Vec::new();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);

        let setup = block_on(prepare_checklist_store_setup(config, &mut dialog))
            .expect("accepted provisioning should succeed")
            .expect("provisioned setup should be returned");
        assert_eq!(setup.local_member, MemberIdentity::from_array(["bob"]));
        assert_eq!(
            String::from_utf8(output).expect("confirmation prompt should be UTF-8"),
            format!("{expected_prompt} [y/N] local member identity> ")
        );

        block_on(setup.store.close()).expect("test SQLite store should close");
        drop(setup);
        std::fs::remove_dir_all(test_root).expect("test directory should be removed");
    }

    #[test]
    fn restart_loads_stored_identity_without_prompting() {
        let (test_root, config) = startup_test_config("existing-ready");
        let mut input = Cursor::new(b"yes\nalice\n".as_slice());
        let mut output = Vec::new();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);
        let first_setup = block_on(prepare_checklist_store_setup(config.clone(), &mut dialog))
            .expect("first setup should succeed")
            .expect("first setup should be returned");
        assert_eq!(
            first_setup.local_member,
            MemberIdentity::from_array(["alice"])
        );
        block_on(first_setup.store.close()).expect("first test SQLite store should close");
        drop(first_setup);

        let mut input = Cursor::new([]);
        let mut output = Vec::new();
        let mut dialog = ChecklistDialog::new(&mut input, &mut output);
        let second_setup = block_on(prepare_checklist_store_setup(config, &mut dialog))
            .expect("restart setup should succeed")
            .expect("restart setup should be returned");
        assert_eq!(
            second_setup.local_member,
            MemberIdentity::from_array(["alice"])
        );
        assert_eq!(output, Vec::<u8>::new());

        block_on(second_setup.store.close()).expect("second test SQLite store should close");
        drop(second_setup);
        std::fs::remove_dir_all(test_root).expect("test directory should be removed");
    }

    #[test]
    fn listener_preserves_the_read_token_for_an_empty_data_event() {
        let member = MemberIdentity::from_array(["alice"]);
        let group_id = GroupId::new_random();
        let (_store, runtime, listener, receivers) =
            load_test_runtime_with_groups(&member, [group_id]);
        let read_token = snapshot_read_token(runtime.as_ref(), group_id, checklist_dataset_id());

        block_on(listener.on_event(ReplicationEvent::DataChanged {
            lineage: DataChangeLineage::Update,
            read_token: read_token.clone(),
            rows: Box::new(VecRowProvider::new(Vec::new())),
        }))
        .expect("empty data event should reach the listener");

        let event = receivers
            .events
            .try_recv()
            .expect("empty event should retain one token-only event");
        assert_eq!(event.read_token, read_token);
        assert!(event.changes.is_empty());

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn listener_collects_all_provider_pages_into_one_atomic_event() {
        let member = MemberIdentity::from_array(["alice"]);
        let group_id = GroupId::new_random();
        let (_store, runtime, listener, receivers) =
            load_test_runtime_with_groups(&member, [group_id]);
        let read_token = snapshot_read_token(runtime.as_ref(), group_id, checklist_dataset_id());
        let row_ids = [
            RowId::new(group_id, checklist_dataset_id(), RowKey(Uuid::from_u128(1))),
            RowId::new(group_id, checklist_dataset_id(), RowKey(Uuid::from_u128(2))),
        ];
        let pages = row_ids
            .iter()
            .cloned()
            .map(|row_id| {
                RowChangeBatch::from_iter([RowChange {
                    previous: PreviousRow::NotCompared,
                    change: RowChangeKind::Delete { row_id },
                }])
            })
            .collect();

        block_on(listener.on_event(ReplicationEvent::DataChanged {
            lineage: DataChangeLineage::Update,
            read_token: read_token.clone(),
            rows: Box::new(PagedRowProvider { pages }),
        }))
        .expect("paged data event should reach the listener");

        let event = receivers
            .events
            .try_recv()
            .expect("all pages should produce one listener event");
        assert_eq!(event.lineage, DataChangeLineage::Update);
        assert_eq!(event.read_token, read_token);
        assert_eq!(
            event
                .changes
                .iter()
                .map(RowChange::row_id)
                .collect::<Vec<_>>(),
            row_ids.iter().collect::<Vec<_>>()
        );
        assert!(matches!(
            receivers.events.try_recv(),
            Err(TryRecvError::Empty)
        ));

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
        assert_eq!(
            format!(
                "{:?}",
                working_set
                    .read_token()
                    .expect("workspace token should load")
            ),
            "ReadToken { group_count: 2, .. }"
        );

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn sync_applies_replacement_before_publishing_its_dirty_predecessor() {
        let member = MemberIdentity::from_array(["alice"]);
        let old_group = GroupId(Uuid::from_u128(71_101));
        let new_group = GroupId(Uuid::from_u128(71_102));
        let row_key = RowKey(Uuid::from_u128(71_103));
        let (_store, runtime, listener, receivers) =
            load_test_runtime_with_groups(&member, [new_group]);
        let read_token = snapshot_read_token(runtime.as_ref(), new_group, checklist_dataset_id());
        block_on(listener.on_event(ReplicationEvent::DataChanged {
            lineage: DataChangeLineage::GroupReplacement {
                migration_id: MigrationId {
                    old_group_id: old_group,
                    new_group_id: new_group,
                },
            },
            read_token,
            rows: Box::new(VecRowProvider::new(Vec::new())),
        }))
        .expect("replacement should reach the listener before sync");
        let old_item_id = ChecklistItemId::group(old_group, row_key);
        let mut working_set = ChecklistWorkingSet::new();
        working_set.add_item_with_id(old_item_id, "unpublished predecessor row");
        let mut repl = ChecklistRepl::new(
            test_app_config(),
            member,
            runtime.clone(),
            receivers,
            ChecklistSession::new(working_set),
        );

        let first_report = synchronise_test_groups(&mut repl)
            .expect("replacement should prevent predecessor publication");

        let new_item_id = ChecklistItemId::group(new_group, row_key);
        assert!(first_report.group_outcomes.is_empty());
        assert_eq!(first_report.listener_event_count, 1);
        assert_eq!(first_report.dirty_resolution_count, 1);
        assert_eq!(first_report.remaining_dirty_groups, vec![new_group]);
        assert!(repl.session.working_set.item(old_item_id).is_none());
        assert_eq!(
            repl.session
                .working_set
                .item(new_item_id)
                .map(|item| item.text.as_str()),
            Some("unpublished predecessor row")
        );

        let second_report = synchronise_test_groups(&mut repl)
            .expect("next sync should publish the retained successor row");
        assert!(matches!(
            second_report.group_outcomes.as_slice(),
            [ChecklistGroupSyncOutcome::Published {
                group_id,
                mutation_count: 1,
            }] if *group_id == new_group
        ));
        assert!(second_report.remaining_dirty_groups.is_empty());

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn sync_publishes_every_dirty_group_and_skips_local_items() {
        let member = MemberIdentity::from_array(["alice"]);
        let first_group = GroupId(Uuid::from_u128(72_001));
        let second_group = GroupId(Uuid::from_u128(72_002));
        let (_store, runtime, _listener, receivers) = load_test_runtime_with_group_records(
            &member,
            [
                named_test_group(second_group, &member, "second"),
                named_test_group(first_group, &member, "first"),
            ],
        );
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
        let session = ChecklistSession::new(working_set);
        let mut repl = ChecklistRepl::new(
            test_app_config(),
            member,
            runtime.clone(),
            receivers,
            session,
        );

        let report = synchronise_test_groups(&mut repl).expect("all groups should synchronise");

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
        assert_eq!(report.listener_event_count, 2);
        assert_eq!(report.applied_event_count, 2);
        assert_eq!(report.dirty_local_item_count, 1);
        assert!(report.remaining_dirty_groups.is_empty());
        assert_eq!(
            repl.format_sync_report(&report)
                .expect("sync report should format"),
            "sync complete:\n  groups:\n    first: published 1 mutation(s)\n    second: published 1 mutation(s)\n  listener events: 2\n  applied events: 2\n  unsynchronised local items: 1\n  dirty groups: none"
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
    fn edit_copy_and_move_publish_as_ordinary_group_mutations() {
        let member = MemberIdentity::from_array(["alice"]);
        let source_group = GroupId(Uuid::from_u128(72_003));
        let target_group = GroupId(Uuid::from_u128(72_004));
        let (_store, runtime, _listener, receivers) = load_test_runtime_with_group_records(
            &member,
            [
                named_test_group(source_group, &member, "source"),
                named_test_group(target_group, &member, "target"),
            ],
        );
        let mut working_set = ChecklistWorkingSet::new();
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut working_set,
            source_group,
        ))
        .expect("source token should load");
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut working_set,
            target_group,
        ))
        .expect("target token should load");
        let copied_source =
            working_set.add_item(ChecklistItemAssociation::Group(source_group), "copy source");
        let moved_source =
            working_set.add_item(ChecklistItemAssociation::Group(source_group), "move source");
        let session = ChecklistSession::new(working_set);
        let mut repl = ChecklistRepl::new(
            test_app_config(),
            member,
            runtime.clone(),
            receivers,
            session,
        );
        synchronise_test_groups(&mut repl).expect("source setup should publish");

        block_on(repl.handle_workspace_command(ChecklistCommand::Edit {
            item: ItemSelector::RowKey(copied_source.row_key),
            command: EditCommand::Copy {
                group: vec![target_group.to_string()],
            },
        }))
        .expect("copy command should stage");
        block_on(repl.handle_workspace_command(ChecklistCommand::Edit {
            item: ItemSelector::Qualified {
                association: ItemAssociationSelector::Group(source_group.to_string()),
                row_key: moved_source.row_key,
            },
            command: EditCommand::Move {
                group: vec![target_group.to_string()],
            },
        }))
        .expect("move command should stage");

        let report = synchronise_test_groups(&mut repl).expect("transfers should publish");

        assert!(matches!(
            report.group_outcomes.as_slice(),
            [
                ChecklistGroupSyncOutcome::Published {
                    group_id: actual_source,
                    mutation_count: 1,
                },
                ChecklistGroupSyncOutcome::Published {
                    group_id: actual_target,
                    mutation_count: 2,
                },
            ] if *actual_source == source_group && *actual_target == target_group
        ));
        let mut verified = ChecklistWorkingSet::new();
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut verified,
            source_group,
        ))
        .expect("source snapshot should reload");
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut verified,
            target_group,
        ))
        .expect("target snapshot should reload");
        assert!(verified.item(copied_source).is_some());
        assert!(verified.item(moved_source).is_none());
        assert!(
            verified
                .item(ChecklistItemId::group(target_group, copied_source.row_key))
                .is_some()
        );
        assert!(
            verified
                .item(ChecklistItemId::group(target_group, moved_source.row_key))
                .is_some()
        );

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn move_source_failure_retains_only_the_tombstone_for_retry() {
        let member = MemberIdentity::from_array(["alice"]);
        let target_group = GroupId(Uuid::from_u128(72_007));
        let unavailable_source = GroupId(Uuid::from_u128(72_008));
        let (_store, runtime, _listener, receivers) = load_test_runtime_with_group_records(
            &member,
            [
                named_test_group(target_group, &member, "target"),
                named_test_group(unavailable_source, &member, "unavailable source"),
            ],
        );
        let mut working_set = ChecklistWorkingSet::new();
        block_on(load_group_snapshot(
            runtime.as_ref(),
            &mut working_set,
            target_group,
        ))
        .expect("target token should load");
        let row_key = RowKey(Uuid::from_u128(72_009));
        let source_id = ChecklistItemId::group(unavailable_source, row_key);
        let target_id = ChecklistItemId::group(target_group, row_key);
        working_set
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id: source_id,
                item: ChecklistItem::new("at-risk source delete"),
            }])
            .expect("clean source item should queue");
        working_set.drain_queued_events();
        let session = ChecklistSession::new(working_set);
        let mut repl = ChecklistRepl::new(
            test_app_config(),
            member,
            runtime.clone(),
            receivers,
            session,
        );
        block_on(repl.handle_workspace_command(ChecklistCommand::Edit {
            item: ItemSelector::RowKey(row_key),
            command: EditCommand::Move {
                group: vec!["target".to_owned()],
            },
        }))
        .expect("move should stage against the writable target registry");

        let first_report =
            synchronise_test_groups(&mut repl).expect("partial publication should return a report");

        assert!(matches!(
            first_report.group_outcomes.as_slice(),
            [
                ChecklistGroupSyncOutcome::Published {
                    group_id: actual_target,
                    mutation_count: 1,
                },
                ChecklistGroupSyncOutcome::Failed {
                    group_id: actual_source,
                    ..
                },
            ] if *actual_target == target_group && *actual_source == unavailable_source
        ));
        assert!(first_report.listener_drain_deferred);
        assert_eq!(first_report.listener_event_count, 0);
        assert_eq!(first_report.applied_event_count, 0);
        assert!(repl.session.working_set.item(source_id).is_none());
        assert!(repl.session.working_set.item(target_id).is_some());
        assert_eq!(
            first_report.remaining_dirty_groups,
            vec![unavailable_source]
        );
        assert!(
            repl.session
                .working_set
                .prepare_group_sync(target_group)
                .expect("published target state should remain valid")
                .is_none(),
            "the successful target upsert should be clean"
        );
        assert!(
            repl.event_receiver.try_recv().is_ok(),
            "the successful target listener echo should remain deferred"
        );

        let retry_report = synchronise_test_groups(&mut repl)
            .expect("ordinary retry should return another partial report");
        assert!(matches!(
            retry_report.group_outcomes.as_slice(),
            [ChecklistGroupSyncOutcome::Failed { group_id, .. }]
                if *group_id == unavailable_source
        ));
        assert_eq!(
            retry_report.remaining_dirty_groups,
            vec![unavailable_source]
        );
        assert!(repl.session.working_set.item(source_id).is_none());
        assert!(repl.session.working_set.item(target_id).is_some());

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }

    #[test]
    fn sync_continues_after_failure_and_defers_listener_draining() {
        let member = MemberIdentity::from_array(["alice"]);
        let unknown_group = GroupId(Uuid::from_u128(72_010));
        let known_group = GroupId(Uuid::from_u128(72_011));
        let (_store, runtime, _listener, receivers) = load_test_runtime_with_group_records(
            &member,
            [named_test_group(known_group, &member, "known")],
        );
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
        let session = ChecklistSession::new(working_set);
        let mut repl = ChecklistRepl::new(
            test_app_config(),
            member,
            runtime.clone(),
            receivers,
            session,
        );

        let report = synchronise_test_groups(&mut repl)
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
        assert_eq!(report.listener_event_count, 0);
        assert_eq!(report.applied_event_count, 0);
        assert_eq!(report.remaining_dirty_groups, vec![unknown_group]);
        assert_eq!(repl.session.working_set.queued_event_count(), 1);
        assert!(
            repl.event_receiver.try_recv().is_ok(),
            "the successful group's listener echo should remain deferred"
        );
        let failed_report = ChecklistSyncReport {
            group_outcomes: vec![ChecklistGroupSyncOutcome::Failed {
                group_id: known_group,
                error: ApiError::RuntimeUnavailable,
            }],
            listener_drain_deferred: true,
            listener_event_count: 0,
            applied_event_count: 0,
            dirty_resolution_count: 0,
            dirty_local_item_count: 0,
            remaining_dirty_groups: vec![known_group],
        };
        assert_eq!(
            repl.format_sync_report(&failed_report)
                .expect("failed sync report should format"),
            "sync incomplete:\n  groups:\n    known: failed: Replication runtime component became unavailable.\n  listener events: 0 applied before publication; later events deferred because a group publication failed\n  applied events: 0\n  unsynchronised local items: 0\n  dirty groups: known"
        );

        block_on(runtime.shutdown()).expect("test runtime should shut down");
    }
}

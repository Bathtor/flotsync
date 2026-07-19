//! REPL and runtime wiring for the replicated checklist runner.

use super::{setup::load_checklist_store_setup, static_group::ensure_configured_group, *};

pub(super) async fn run_configured_peer(
    config_path: &Path,
) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path).await?;
    ensure_configured_group(
        setup.store.as_ref(),
        &setup.config,
        &setup.replication_security,
    )
    .await
    .context(repl_error::StaticGroupSnafu)?;

    let (listener, listener_receiver) = ChecklistListener::pair();
    let group_id = setup.config.group_id;
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

    let working_set = load_checklist_working_set(replication.as_ref(), group_id).await?;
    let mut repl = ChecklistRepl::new(setup.config, replication, listener_receiver, working_set);
    let run_result = repl.run().await;
    let shutdown_result = repl.shutdown().await;
    run_result?;
    shutdown_result
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
    working_set: ChecklistWorkingSet,
}

impl ChecklistRepl {
    fn new(
        config: ChecklistAppConfig,
        replication: Arc<dyn ReplicationApi>,
        listener_receiver: Receiver<ChecklistListenerBatch>,
        working_set: ChecklistWorkingSet,
    ) -> Self {
        Self {
            config,
            replication,
            listener_receiver,
            working_set,
        }
    }

    #[allow(
        clippy::needless_continue,
        reason = "The REPL loop uses explicit continues to make command-processing outcomes obvious."
    )]
    async fn run(&mut self) -> Result<(), ReplicatedChecklistError> {
        println!("replicated checklist group {}", self.config.group_id);
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
            ChecklistCommand::Add { text } => {
                let row_key = self.working_set.add_item(join_words(text));
                println!("added:");
                self.print_selected_row(ItemSelector::RowKey(row_key))?;
            }
            ChecklistCommand::Rename { item, text } => {
                self.working_set
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
                    self.working_set
                        .add_tag(item, tag)
                        .context(repl_error::WorkingSetSnafu)?;
                    println!("tag added:");
                    self.print_selected_row(item)?;
                }
                TagCommand::Rm { item, tag } => {
                    self.working_set
                        .remove_tag(item, &tag)
                        .context(repl_error::WorkingSetSnafu)?;
                    println!("tag removed:");
                    self.print_selected_row(item)?;
                }
            },
            ChecklistCommand::Claim { item } => {
                self.working_set
                    .claim_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("claimed:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Complete { item } => {
                self.working_set
                    .complete_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("completed:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Priority { item, priority } => {
                self.working_set
                    .set_priority(item, priority)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("priority set:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Delete { item } => {
                println!("deleted:");
                self.print_selected_row(item)?;
                self.working_set
                    .delete_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
            }
            ChecklistCommand::List => self.print_list(),
            ChecklistCommand::Show { item } => self.print_item(item)?,
            ChecklistCommand::Events { limit } => self.print_events(limit),
            ChecklistCommand::Sync => self.sync().await?,
            ChecklistCommand::Members => self.print_members(),
            ChecklistCommand::Check => self.check_members().await,
            ChecklistCommand::Me => self.print_me(),
            ChecklistCommand::Help => println!("{}", checklist_help()),
            ChecklistCommand::Quit => return Ok(false),
        }
        Ok(true)
    }

    fn edit_note(&mut self, item: ItemSelector) -> Result<(), ReplicatedChecklistError> {
        let selected = self
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
        self.working_set
            .edit_note(item, note.trim_end_matches(['\r', '\n']))
            .context(repl_error::WorkingSetSnafu)?;
        println!("note updated:");
        self.print_selected_row(item)?;
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), ReplicatedChecklistError> {
        let plan = self
            .working_set
            .prepare_sync()
            .context(repl_error::WorkingSetSnafu)?;
        if let Some(plan) = &plan {
            let read_token = self
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
            self.working_set.set_read_token(receipt.read_token);
        }
        let listener_batch_count = self.drain_listener_queue()?;
        let applied_events = self.working_set.finish_successful_sync(plan);
        println!(
            "sync complete: received {listener_batch_count} listener batches, applied {applied_events} events"
        );
        Ok(())
    }

    /// Drain queued listener batches into the working set and return the number of batches drained.
    fn drain_listener_queue(&mut self) -> Result<usize, ReplicatedChecklistError> {
        let mut drained_batch_count = 0;
        while let Some(batch) = self.receive_listener_batch()? {
            self.working_set
                .enqueue_row_changes(batch.changes)
                .context(repl_error::WorkingSetSnafu)?;
            // No REPL command can run while sync is draining listener batches,
            // so it is safe to merge the event token before the queued rows are
            // applied immediately below by finish_successful_sync.
            self.working_set.merge_read_token(batch.read_token);
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
        let items = self.working_set.listed_items();
        if items.is_empty() {
            println!("checklist is empty");
        } else {
            for item in items {
                print_row(&item);
            }
        }
    }

    fn print_item(&self, item: ItemSelector) -> Result<(), ReplicatedChecklistError> {
        let listed = self
            .working_set
            .selected_item(item)
            .context(repl_error::WorkingSetSnafu)?;
        println!("index: {}", listed.index);
        println!("row: {}", listed.row_key);
        println!("text: {}", listed.item.text);
        println!("note: {}", listed.item.note);
        println!("tags: {}", listed.item.formatted_tags());
        println!("status: {}", listed.item.status);
        println!("priority: {}", listed.item.priority);
        println!("edit_count: {}", listed.item.edit_count);
        Ok(())
    }

    fn print_events(&self, limit: Option<usize>) {
        let events = self.working_set.events();
        for event in events.iter().rev().take(limit.unwrap_or(usize::MAX)) {
            println!("event {}:", format_timestamp(event.timestamp));
            for change in &event.changes {
                println!("  {change:?}");
            }
        }
        if events.is_empty() {
            println!("no events");
        }
    }

    fn print_members(&self) {
        println!("group {}", self.config.group_id);
        for (index, member) in self.config.ordered_members.iter().enumerate() {
            let marker = if member == &self.config.local_member {
                " (me)"
            } else {
                ""
            };
            println!("{index}: {member}{marker}");
        }
    }

    async fn check_members(&self) {
        println!("group {}", self.config.group_id);
        let requests =
            self.config
                .ordered_members
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, member)| {
                    let replication = self.replication.clone();
                    let group_id = self.config.group_id;
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
    }

    async fn shutdown(&self) -> Result<(), ReplicatedChecklistError> {
        self.replication
            .shutdown()
            .await
            .context(repl_error::ReplicationSnafu)
    }

    fn print_me(&self) {
        println!("member: {}", self.config.local_member);
        println!("group: {}", self.config.group_id);
        println!("store: {}", self.config.store_path.display());
        println!("config: {}", self.config.source_path.display());
        println!(
            "dirty rows: {}, queued events: {}",
            self.working_set.dirty_row_count(),
            self.working_set.queued_event_count()
        );
    }

    fn print_selected_row(&self, item: ItemSelector) -> Result<(), ReplicatedChecklistError> {
        let selected = self
            .working_set
            .selected_item(item)
            .context(repl_error::WorkingSetSnafu)?;
        print_row(&selected);
        Ok(())
    }
}

async fn load_checklist_working_set(
    replication: &dyn ReplicationApi,
    group_id: GroupId,
) -> Result<ChecklistWorkingSet, ReplicatedChecklistError> {
    let mut working_set = ChecklistWorkingSet::new(group_id);
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
    working_set.set_read_token(read_token);
    Ok(working_set)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "Parsed command words are consumed when building checklist item text."
)]
fn join_words(words: Vec<String>) -> String {
    words.join(" ")
}

fn print_row(item: &ListedChecklistItem<'_>) {
    let tags = item.item.formatted_tags();
    println!(
        "{:>3}. [{}] p{} edits={} {} ({}) {tags}",
        item.index,
        item.item.status,
        item.item.priority,
        item.item.edit_count,
        item.item.text,
        item.row_key,
    );
}

fn format_timestamp(timestamp: SystemTime) -> String {
    DateTime::<Local>::from(timestamp)
        .format("%Y-%m-%d %H:%M:%S %:z")
        .to_string()
}

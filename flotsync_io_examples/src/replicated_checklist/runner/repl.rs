//! REPL and runtime wiring for the replicated checklist runner.

use super::{setup::load_checklist_store_setup, *};

pub(super) async fn run_configured_peer(
    config_path: &Path,
) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path).await?;
    let persisted_group = load_single_active_group(setup.store.as_ref()).await?;

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

    let session = match persisted_group {
        Some(group) => {
            let working_set =
                load_checklist_working_set(replication.as_ref(), group.group_id).await?;
            Some(ChecklistSession::new(&group, working_set))
        }
        None => None,
    };
    let mut repl = ChecklistRepl::new(setup.config, replication, listener_receiver, session);
    let run_result = repl.run().await;
    let shutdown_result = repl.shutdown().await;
    run_result?;
    shutdown_result
}

/// Load the optional group supported by this transitional single-group REPL.
async fn load_single_active_group(
    store: &dyn ReplicationStore,
) -> Result<Option<ReplicationGroupRecord>, ReplicatedChecklistError> {
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
    match groups.len() {
        0 => Ok(None),
        1 => Ok(groups.pop()),
        actual_count => repl_error::MultipleActiveGroupsSnafu { actual_count }.fail(),
    }
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
    session: Option<ChecklistSession>,
}

/// Checklist state for the optional persisted group supported by this child.
struct ChecklistSession {
    group_id: GroupId,
    members: Vec<MemberIdentity>,
    working_set: ChecklistWorkingSet,
}

impl ChecklistSession {
    /// Build the application session from the persisted group record and loaded snapshot.
    fn new(group: &ReplicationGroupRecord, working_set: ChecklistWorkingSet) -> Self {
        Self {
            group_id: group.group_id,
            members: group.member_ids().cloned().collect(),
            working_set,
        }
    }
}

impl ChecklistRepl {
    fn new(
        config: ChecklistAppConfig,
        replication: Arc<dyn ReplicationApi>,
        listener_receiver: Receiver<ChecklistListenerBatch>,
        session: Option<ChecklistSession>,
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
        match &self.session {
            Some(session) => println!("replicated checklist group {}", session.group_id),
            None => println!("replicated checklist: no active group"),
        }
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
            ChecklistCommand::Help => {
                println!("{}", checklist_help());
                return Ok(true);
            }
            ChecklistCommand::Quit => return Ok(false),
            _ => {}
        }
        if self.session.is_none() {
            return repl_error::NoActiveGroupSnafu.fail();
        }
        self.handle_group_command(command).await?;
        Ok(true)
    }

    /// Run one command that requires the transitional single-group session.
    async fn handle_group_command(
        &mut self,
        command: ChecklistCommand,
    ) -> Result<(), ReplicatedChecklistError> {
        match command {
            ChecklistCommand::Add { text } => {
                let row_key = self.session_mut()?.working_set.add_item(join_words(text));
                println!("added:");
                self.print_selected_row(ItemSelector::RowKey(row_key))?;
            }
            ChecklistCommand::Rename { item, text } => {
                self.session_mut()?
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
                    self.session_mut()?
                        .working_set
                        .add_tag(item, tag)
                        .context(repl_error::WorkingSetSnafu)?;
                    println!("tag added:");
                    self.print_selected_row(item)?;
                }
                TagCommand::Rm { item, tag } => {
                    self.session_mut()?
                        .working_set
                        .remove_tag(item, &tag)
                        .context(repl_error::WorkingSetSnafu)?;
                    println!("tag removed:");
                    self.print_selected_row(item)?;
                }
            },
            ChecklistCommand::Claim { item } => {
                self.session_mut()?
                    .working_set
                    .claim_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("claimed:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Complete { item } => {
                self.session_mut()?
                    .working_set
                    .complete_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("completed:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Priority { item, priority } => {
                self.session_mut()?
                    .working_set
                    .set_priority(item, priority)
                    .context(repl_error::WorkingSetSnafu)?;
                println!("priority set:");
                self.print_selected_row(item)?;
            }
            ChecklistCommand::Delete { item } => {
                println!("deleted:");
                self.print_selected_row(item)?;
                self.session_mut()?
                    .working_set
                    .delete_item(item)
                    .context(repl_error::WorkingSetSnafu)?;
            }
            ChecklistCommand::List => self.print_list()?,
            ChecklistCommand::Show { item } => self.print_item(item)?,
            ChecklistCommand::Events { limit } => self.print_events(limit)?,
            ChecklistCommand::Sync => self.sync().await?,
            ChecklistCommand::Members => self.print_members()?,
            ChecklistCommand::Check => self.check_members().await?,
            ChecklistCommand::Keys { .. }
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
            .session()?
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
        self.session_mut()?
            .working_set
            .edit_note(item, note.trim_end_matches(['\r', '\n']))
            .context(repl_error::WorkingSetSnafu)?;
        println!("note updated:");
        self.print_selected_row(item)?;
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), ReplicatedChecklistError> {
        let plan = self
            .session_mut()?
            .working_set
            .prepare_sync()
            .context(repl_error::WorkingSetSnafu)?;
        if let Some(plan) = &plan {
            let read_token = self
                .session()?
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
            self.session_mut()?
                .working_set
                .set_read_token(receipt.read_token);
        }
        let listener_batch_count = self.drain_listener_queue()?;
        let applied_events = self.session_mut()?.working_set.finish_successful_sync(plan);
        println!(
            "sync complete: received {listener_batch_count} listener batches, applied {applied_events} events"
        );
        Ok(())
    }

    /// Drain queued listener batches into the working set and return the number of batches drained.
    fn drain_listener_queue(&mut self) -> Result<usize, ReplicatedChecklistError> {
        let mut drained_batch_count = 0;
        while let Some(batch) = self.receive_listener_batch()? {
            self.session_mut()?
                .working_set
                .enqueue_row_changes(batch.changes)
                .context(repl_error::WorkingSetSnafu)?;
            // No REPL command can run while sync is draining listener batches,
            // so it is safe to merge the event token before the queued rows are
            // applied immediately below by finish_successful_sync.
            self.session_mut()?
                .working_set
                .merge_read_token(batch.read_token);
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

    fn print_list(&self) -> Result<(), ReplicatedChecklistError> {
        let items = self.session()?.working_set.listed_items();
        if items.is_empty() {
            println!("checklist is empty");
        } else {
            for item in items {
                print_row(&item);
            }
        }
        Ok(())
    }

    fn print_item(&self, item: ItemSelector) -> Result<(), ReplicatedChecklistError> {
        let listed = self
            .session()?
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

    fn print_events(&self, limit: Option<usize>) -> Result<(), ReplicatedChecklistError> {
        let events = self.session()?.working_set.events();
        for event in events.iter().rev().take(limit.unwrap_or(usize::MAX)) {
            println!("event {}:", format_timestamp(event.timestamp));
            for change in &event.changes {
                println!("  {change:?}");
            }
        }
        if events.is_empty() {
            println!("no events");
        }
        Ok(())
    }

    fn print_members(&self) -> Result<(), ReplicatedChecklistError> {
        let session = self.session()?;
        println!("group {}", session.group_id);
        for (index, member) in session.members.iter().enumerate() {
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
        let session = self.session()?;
        let group_id = session.group_id;
        let members = session.members.clone();
        println!("group {group_id}");
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
        match &self.session {
            Some(session) => println!("group: {}", session.group_id),
            None => println!("group: none"),
        }
        println!("store: {}", self.config.store_path.display());
        println!("config: {}", self.config.source_path.display());
        if let Some(session) = &self.session {
            println!(
                "dirty rows: {}, queued events: {}",
                session.working_set.dirty_row_count(),
                session.working_set.queued_event_count()
            );
        }
    }

    fn print_selected_row(&self, item: ItemSelector) -> Result<(), ReplicatedChecklistError> {
        let selected = self
            .session()?
            .working_set
            .selected_item(item)
            .context(repl_error::WorkingSetSnafu)?;
        print_row(&selected);
        Ok(())
    }

    /// Borrow the active checklist session or report the zero-group limitation.
    fn session(&self) -> Result<&ChecklistSession, ReplicatedChecklistError> {
        self.session
            .as_ref()
            .ok_or(ReplicatedChecklistError::NoActiveGroup)
    }

    /// Mutably borrow the active checklist session or report the zero-group limitation.
    fn session_mut(&mut self) -> Result<&mut ChecklistSession, ReplicatedChecklistError> {
        self.session
            .as_mut()
            .ok_or(ReplicatedChecklistError::NoActiveGroup)
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

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_core::{MemberIndex, versions::VersionVector};
    use flotsync_replication::{
        GroupMemberKeys,
        GroupSchema,
        MemberKeyId,
        ReplicationGroupLifecycle,
        current_slice_placeholder_group_security_material,
        test_support::test_public_member_keys,
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

    #[test]
    fn transitional_session_loads_zero_or_one_group() {
        let member = MemberIdentity::from_array(["alice"]);
        let store = block_on(SqliteReplicationStore::in_memory(member.clone()))
            .expect("test store should open");

        let empty =
            block_on(load_single_active_group(&store)).expect("zero-group store should load");
        assert!(empty.is_none());

        let group_id = GroupId(Uuid::from_u128(70_001));
        block_on(insert_test_group(&store, test_group(group_id, &member)));
        let loaded = block_on(load_single_active_group(&store))
            .expect("single-group store should load")
            .expect("single group should be present");
        assert_eq!(loaded.group_id, group_id);
    }

    #[test]
    fn transitional_session_rejects_multiple_groups() {
        let member = MemberIdentity::from_array(["alice"]);
        let store = block_on(SqliteReplicationStore::in_memory(member.clone()))
            .expect("test store should open");
        block_on(insert_test_group(
            &store,
            test_group(GroupId(Uuid::from_u128(70_002)), &member),
        ));
        block_on(insert_test_group(
            &store,
            test_group(GroupId(Uuid::from_u128(70_003)), &member),
        ));

        let error = block_on(load_single_active_group(&store))
            .expect_err("multiple groups should remain a transitional error");
        assert!(matches!(
            error,
            ReplicatedChecklistError::MultipleActiveGroups { actual_count: 2 }
        ));
    }
}

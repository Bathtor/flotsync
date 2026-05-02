use super::{
    CHECKLIST_SCHEMA,
    ChecklistCommand,
    ChecklistWorkingSet,
    ChecklistWorkingSetError,
    EditCommand,
    ItemSelector,
    ListedChecklistItem,
    TagCommand,
    checklist_dataset_id,
    checklist_help,
    config::{ChecklistAppConfig, ChecklistConfigError},
    parse_checklist_command,
};
use chrono::{DateTime, Local};
use clap::Parser;
use flotsync_core::{member::Identifier, versions::VersionVector};
use flotsync_replication::{
    ApiError,
    GroupId,
    GroupMembers,
    GroupMembersError,
    ListenerError,
    LoadError,
    MemberIdentity,
    MemberIndex,
    PublishChangesRequest,
    ReadToken,
    RejectionReason,
    ReplicationApi,
    ReplicationConfig,
    ReplicationEvent,
    ReplicationEventListener,
    ReplicationGroupRecord,
    ReplicationStore,
    RowChange,
    RowProviderError,
    SnapshotRowsRequest,
    SqliteReplicationStore,
    StoreError,
    load_replication_runtime_with_runtime_config_toml,
};
use futures_util::FutureExt;
use kompact::prelude::block_on;
use snafu::prelude::*;
use std::{
    collections::HashSet,
    fs,
    future::Future,
    io::{self, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        mpsc::{self, Receiver, Sender, TryRecvError},
    },
    time::SystemTime,
};

const CHECKLIST_SNAPSHOT_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// Command-line arguments for the replicated checklist example.
#[derive(Clone, Debug, Parser)]
#[command(name = "replicated-checklist")]
pub struct ReplicatedChecklistArgs {
    /// Path to the node-specific checklist TOML config.
    pub config: PathBuf,
}

/// Run one configured replicated checklist REPL.
pub fn run(args: ReplicatedChecklistArgs) -> Result<(), ReplicatedChecklistError> {
    let config = ChecklistAppConfig::load(&args.config).context(repl_error::ConfigSnafu)?;
    ensure_store_parent_exists(&config.store_path)?;
    let store = Arc::new(
        SqliteReplicationStore::file_with_schema_sources(
            config.local_member.clone(),
            &config.store_path,
            [(checklist_dataset_id(), &*CHECKLIST_SCHEMA)],
        )
        .context(repl_error::StoreSnafu)?,
    );

    block_on(ensure_configured_group(store.as_ref(), &config))
        .context(repl_error::StaticGroupSnafu)?;

    let (listener, listener_receiver) = ChecklistListener::pair();
    let replication = block_on(load_replication_runtime_with_runtime_config_toml(
        Identifier::from_array(["flotsync", "examples", "replicated-checklist"]),
        store,
        listener,
        ReplicationConfig::default(),
        &config.runtime_config_toml,
    ))
    .context(repl_error::LoadRuntimeSnafu)?;

    let working_set = load_checklist_working_set(replication.as_ref(), config.group_id)?;
    let mut repl = ChecklistRepl::new(config, replication, listener_receiver, working_set);
    repl.run()
}

/// Errors from the replicated checklist binary.
#[derive(Debug, Snafu)]
#[snafu(module(repl_error))]
pub enum ReplicatedChecklistError {
    #[snafu(display("{source}"))]
    Config { source: ChecklistConfigError },
    #[snafu(display("Failed to prepare checklist store directory {}: {source}", path.display()))]
    CreateStoreDirectory { path: PathBuf, source: io::Error },
    #[snafu(display("Failed to open checklist replication store: {source}"))]
    Store { source: StoreError },
    #[snafu(display("{source}"))]
    StaticGroup { source: StaticGroupError },
    #[snafu(display("Failed to load replication runtime: {source}"))]
    LoadRuntime { source: LoadError },
    #[snafu(display("Replication API call failed: {source}"))]
    Replication { source: ApiError },
    #[snafu(display("Failed to load checklist snapshot from replication store: {source}"))]
    SnapshotRows { source: RowProviderError },
    #[snafu(display("{source}"))]
    WorkingSet { source: ChecklistWorkingSetError },
    #[snafu(display("I/O failed while {action}: {source}"))]
    Io {
        action: &'static str,
        source: io::Error,
    },
    #[snafu(display("Checklist listener queue closed."))]
    ListenerQueueClosed,
}

#[derive(Debug, Snafu)]
#[snafu(module(static_group_error))]
pub enum StaticGroupError {
    #[snafu(display("Failed to access replication store while preparing static group: {source}"))]
    Store { source: StoreError },
    #[snafu(display("Configured static group must contain at least one member."))]
    EmptyMembers,
    #[snafu(display("Configured local member {local_member} is not part of the static group."))]
    LocalMemberMissing { local_member: MemberIdentity },
    #[snafu(display("Configured static group is invalid: {source}"))]
    InvalidMembers { source: GroupMembersError },
    #[snafu(display(
        "Store contains unexpected group {actual_group_id}; checklist config only permits {configured_group_id}."
    ))]
    UnexpectedGroup {
        configured_group_id: GroupId,
        actual_group_id: GroupId,
    },
    #[snafu(display(
        "Store contains {actual_count} replication groups; checklist config only permits zero or one group {configured_group_id}."
    ))]
    MultipleGroups {
        configured_group_id: GroupId,
        actual_count: usize,
    },
    #[snafu(display(
        "Stored group {group_id} has different ordered members; expected {expected:?}, found {actual:?}."
    ))]
    MemberMismatch {
        group_id: GroupId,
        expected: Vec<MemberIdentity>,
        actual: Vec<MemberIdentity>,
    },
    #[snafu(display(
        "Stored group {group_id} has local member index {actual}, expected {expected}."
    ))]
    LocalMemberIndexMismatch {
        group_id: GroupId,
        expected: MemberIndex,
        actual: MemberIndex,
    },
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

    fn run(&mut self) -> Result<(), ReplicatedChecklistError> {
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
            match self.handle_command(command) {
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

    fn handle_command(
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
            ChecklistCommand::Sync => self.sync()?,
            ChecklistCommand::Members => self.print_members(),
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

    fn sync(&mut self) -> Result<(), ReplicatedChecklistError> {
        let plan = self
            .working_set
            .prepare_sync()
            .context(repl_error::WorkingSetSnafu)?;
        if let Some(plan) = &plan {
            let read_token = self
                .working_set
                .read_token()
                .context(repl_error::WorkingSetSnafu)?;
            let receipt = block_on(self.replication.publish_changes(PublishChangesRequest {
                read_token,
                changes: plan.mutations.clone(),
            }))
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

    fn print_me(&self) {
        println!("member: {}", self.config.local_member);
        println!("group: {}", self.config.group_id);
        println!("store: {}", self.config.store_path.display());
        println!("config: {}", self.config.source_path.display());
        println!("local endpoint: {}", self.config.local_endpoint_bind_addr);
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

fn load_checklist_working_set(
    replication: &dyn ReplicationApi,
    group_id: GroupId,
) -> Result<ChecklistWorkingSet, ReplicatedChecklistError> {
    block_on(load_checklist_working_set_async(replication, group_id))
}

async fn load_checklist_working_set_async(
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
            .apply_snapshot_rows(batch)
            .context(repl_error::WorkingSetSnafu)?;
    }
    working_set.set_read_token(read_token);
    Ok(working_set)
}

/// Ensure the store contains either no group or exactly the configured static group.
///
/// When no group exists this inserts the configured group. When one group exists
/// it must match the configured id, member order, and local member index.
async fn ensure_configured_group(
    store: &dyn ReplicationStore,
    config: &ChecklistAppConfig,
) -> Result<(), StaticGroupError> {
    let member_count =
        NonZeroUsize::new(config.ordered_members.len()).ok_or(StaticGroupError::EmptyMembers)?;
    let members = GroupMembers::from_ordered_members(config.ordered_members.clone())
        .context(static_group_error::InvalidMembersSnafu)?;
    let local_member_index =
        members
            .member_index(&config.local_member)
            .ok_or(StaticGroupError::LocalMemberMissing {
                local_member: config.local_member.clone(),
            })?;

    let mut transaction = store
        .begin_transaction()
        .await
        .context(static_group_error::StoreSnafu)?;
    let persisted_groups = transaction
        .load_replication_groups()
        .await
        .context(static_group_error::StoreSnafu)?;

    let existing_group = match persisted_groups.as_slice() {
        [] => None,
        [existing_group] => Some(existing_group),
        groups => {
            return static_group_error::MultipleGroupsSnafu {
                configured_group_id: config.group_id,
                actual_count: groups.len(),
            }
            .fail();
        }
    };

    if let Some(existing_group) = existing_group {
        ensure!(
            existing_group.group_id == config.group_id,
            static_group_error::UnexpectedGroupSnafu {
                configured_group_id: config.group_id,
                actual_group_id: existing_group.group_id,
            }
        );
        ensure!(
            existing_group.members == config.ordered_members,
            static_group_error::MemberMismatchSnafu {
                group_id: config.group_id,
                expected: config.ordered_members.clone(),
                actual: existing_group.members.clone(),
            }
        );
        ensure!(
            existing_group.local_member_index == local_member_index,
            static_group_error::LocalMemberIndexMismatchSnafu {
                group_id: config.group_id,
                expected: local_member_index,
                actual: existing_group.local_member_index,
            }
        );
    } else {
        transaction
            .insert_replication_group(ReplicationGroupRecord {
                group_id: config.group_id,
                members: config.ordered_members.clone(),
                local_member_index,
                version_vector: VersionVector::initial(member_count),
            })
            .await
            .context(static_group_error::StoreSnafu)?;
    }

    transaction
        .commit()
        .await
        .context(static_group_error::StoreSnafu)?;
    Ok(())
}

fn ensure_store_parent_exists(path: &Path) -> Result<(), ReplicatedChecklistError> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => {
            fs::create_dir_all(parent).context(repl_error::CreateStoreDirectorySnafu {
                path: parent.to_path_buf(),
            })
        }
        // A bare filename has `Some("")` as parent, meaning the store lives in the current directory.
        Some(_) | None => Ok(()),
    }
}

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
    use uuid::Uuid;

    fn test_config(group_id: GroupId, store_path: PathBuf) -> ChecklistAppConfig {
        ChecklistAppConfig {
            source_path: PathBuf::from("test.toml"),
            runtime_config_toml: String::new(),
            local_member: MemberIdentity::from_array(["alice"]),
            store_path,
            group_id,
            ordered_members: vec![
                MemberIdentity::from_array(["alice"]),
                MemberIdentity::from_array(["bob"]),
            ],
            local_endpoint_bind_addr: "127.0.0.1:45100".parse().unwrap(),
        }
    }

    #[test]
    fn ensure_configured_group_inserts_missing_static_group() {
        let group_id = GroupId(Uuid::from_u128(100));
        let config = test_config(group_id, PathBuf::from("unused.sqlite"));
        let store = SqliteReplicationStore::in_memory(config.local_member.clone())
            .expect("store should build");

        block_on(ensure_configured_group(&store, &config)).expect("group should be prepared");
        let mut transaction =
            block_on(store.begin_transaction()).expect("transaction should start");
        let groups = block_on(transaction.load_replication_groups()).expect("groups should load");
        block_on(transaction.rollback()).expect("transaction should roll back");

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id, group_id);
        assert_eq!(groups[0].members, config.ordered_members);
    }

    #[test]
    fn ensure_configured_group_rejects_member_mismatch() {
        let group_id = GroupId(Uuid::from_u128(101));
        let config = test_config(group_id, PathBuf::from("unused.sqlite"));
        let store = SqliteReplicationStore::in_memory(config.local_member.clone())
            .expect("store should build");
        let mut existing = config.clone();
        existing.ordered_members = vec![
            MemberIdentity::from_array(["alice"]),
            MemberIdentity::from_array(["carol"]),
        ];
        block_on(ensure_configured_group(&store, &existing))
            .expect("existing group should be inserted");

        let result = block_on(ensure_configured_group(&store, &config));

        assert!(matches!(
            result,
            Err(StaticGroupError::MemberMismatch { .. })
        ));
    }
}

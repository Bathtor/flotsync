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
use clap::{Parser, Subcommand};
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
    ProvisionSecurityError,
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
    SummaryRequest,
    load_replication_runtime_with_runtime_config_toml,
    prepare_initial_group_security_material,
    provision_replication_security,
    validate_initial_group_security_material,
};
use flotsync_security::{SecurityError, generate_member_key_files};
use futures_util::{FutureExt, future::join_all};
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
const GENERATED_PRIVATE_JWKS_FILE_NAME: &str = "private.jwks";
const GENERATED_PUBLIC_JWKS_FILE_NAME: &str = "public.jwks";

/// Command-line arguments for the replicated checklist example.
#[derive(Clone, Debug, Parser)]
#[command(name = "replicated-checklist")]
pub struct ReplicatedChecklistArgs {
    #[command(subcommand)]
    pub command: ReplicatedChecklistCommand,
}

/// Top-level replicated checklist commands.
#[derive(Clone, Debug, Subcommand)]
pub enum ReplicatedChecklistCommand {
    /// Run one configured checklist peer.
    Run {
        /// Path to the node-specific checklist TOML config.
        config: PathBuf,
    },
    /// Generate local-private and public JWKS files for one member.
    GenerateKeys {
        /// Member identity to encode into the generated JWKS files.
        member: MemberIdentity,
        /// Directory where private.jwks and public.jwks will be written.
        out_dir: PathBuf,
        /// Replace existing generated files in the output directory.
        #[arg(long)]
        force: bool,
    },
}

/// Sensitivity class for generated key-file write policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GeneratedKeyFileKind {
    /// Local private identity JWKS; final and temporary files must be owner-only.
    Private,
    /// Public identity JWKS intended to be copied to trusted peers.
    Public,
}

/// Run one configured replicated checklist REPL.
///
/// # Errors
///
/// See `ReplicatedChecklistError` for failure conditions.
#[allow(
    clippy::needless_pass_by_value,
    reason = "Example entry points consume parsed CLI argument structs."
)]
pub fn run(args: ReplicatedChecklistArgs) -> Result<(), ReplicatedChecklistError> {
    match args.command {
        ReplicatedChecklistCommand::Run { config } => run_configured_peer(&config),
        ReplicatedChecklistCommand::GenerateKeys {
            member,
            out_dir,
            force,
        } => generate_checklist_key_files(member, &out_dir, force),
    }
}

fn run_configured_peer(config_path: &Path) -> Result<(), ReplicatedChecklistError> {
    let config = ChecklistAppConfig::load(config_path).context(repl_error::ConfigSnafu)?;
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
        config.replication_security.clone(),
        &config.runtime_config_toml,
    ))
    .context(repl_error::LoadRuntimeSnafu)?;

    let working_set = load_checklist_working_set(replication.as_ref(), config.group_id)?;
    let mut repl = ChecklistRepl::new(config, replication, listener_receiver, working_set);
    repl.run()
}

/// Generate the fixed key-file pair expected by replicated-checklist config.
fn generate_checklist_key_files(
    member: MemberIdentity,
    out_dir: &Path,
    force: bool,
) -> Result<(), ReplicatedChecklistError> {
    fs::create_dir_all(out_dir).context(repl_error::CreateKeyDirectorySnafu {
        path: out_dir.to_path_buf(),
    })?;
    let private_path = out_dir.join(GENERATED_PRIVATE_JWKS_FILE_NAME);
    let public_path = out_dir.join(GENERATED_PUBLIC_JWKS_FILE_NAME);
    ensure_generated_key_targets_available([&private_path, &public_path], force)?;

    let generated = generate_member_key_files(member).context(repl_error::GenerateKeysSnafu)?;
    let public_temp = write_generated_key_temp_file(
        &public_path,
        &generated.public_jwks,
        GeneratedKeyFileKind::Public,
    )?;
    let private_temp = match write_generated_key_temp_file(
        &private_path,
        generated.local_private_jwks.as_str(),
        GeneratedKeyFileKind::Private,
    ) {
        Ok(temp_path) => temp_path,
        Err(error) => {
            let _ = fs::remove_file(&public_temp);
            return Err(error);
        }
    };
    if let Err(error) = promote_generated_key_file(&public_temp, &public_path) {
        let _ = fs::remove_file(&public_temp);
        let _ = fs::remove_file(&private_temp);
        return Err(error);
    }
    if let Err(error) = promote_generated_key_file(&private_temp, &private_path) {
        let _ = fs::remove_file(&public_path);
        let _ = fs::remove_file(&private_temp);
        return Err(error);
    }

    println!("wrote {}", private_path.display());
    println!("wrote {}", public_path.display());
    Ok(())
}

/// Fail before generating new material if any final key-file target already exists.
fn ensure_generated_key_targets_available<'a>(
    paths: impl IntoIterator<Item = &'a PathBuf>,
    force: bool,
) -> Result<(), ReplicatedChecklistError> {
    for path in paths {
        ensure!(
            force || !path.exists(),
            repl_error::KeyFileExistsSnafu { path: path.clone() }
        );
    }
    Ok(())
}

/// Write one generated key file to a same-directory temporary path.
#[cfg_attr(not(unix), allow(unused_variables))]
fn write_generated_key_temp_file(
    path: &Path,
    contents: &str,
    kind: GeneratedKeyFileKind,
) -> Result<PathBuf, ReplicatedChecklistError> {
    let temp_path = generated_key_temp_path(path);
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if kind == GeneratedKeyFileKind::Private {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp_path)
        .context(repl_error::WriteKeyFileSnafu {
            path: temp_path.clone(),
        })?;
    file.write_all(contents.as_bytes())
        .context(repl_error::WriteKeyFileSnafu {
            path: temp_path.clone(),
        })?;
    Ok(temp_path)
}

/// Build a collision-resistant temporary path next to the final key-file target.
fn generated_key_temp_path(path: &Path) -> PathBuf {
    path.with_file_name(format!(".key-{}.tmp", uuid::Uuid::new_v4()))
}

/// Move a completed temporary key file into its final location.
fn promote_generated_key_file(
    temp_path: &Path,
    path: &Path,
) -> Result<(), ReplicatedChecklistError> {
    fs::rename(temp_path, path).context(repl_error::PromoteKeyFileSnafu {
        temp_path: temp_path.to_path_buf(),
        path: path.to_path_buf(),
    })
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
    #[snafu(display("Failed to create key output directory {}: {source}", path.display()))]
    CreateKeyDirectory { path: PathBuf, source: io::Error },
    #[snafu(display("Failed to generate member key files: {source}"))]
    GenerateKeys { source: SecurityError },
    #[snafu(display("Refusing to overwrite existing key file {}", path.display()))]
    KeyFileExists { path: PathBuf },
    #[snafu(display("Failed to write key file {}: {source}", path.display()))]
    WriteKeyFile { path: PathBuf, source: io::Error },
    #[snafu(display(
        "Failed to promote generated key file {} to {}: {source}",
        temp_path.display(),
        path.display()
    ))]
    PromoteKeyFile {
        temp_path: PathBuf,
        path: PathBuf,
        source: io::Error,
    },
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
    #[snafu(display("Failed to read security key file {}: {source}", path.display()))]
    ReadSecurityFile { path: PathBuf, source: io::Error },
    #[snafu(display("Failed to provision security records: {source}"))]
    ProvisionSecurity { source: ProvisionSecurityError },
    #[snafu(display("Configured static group member {member_id} has no trusted public JWKS."))]
    MissingTrustedPublicKeys { member_id: MemberIdentity },
    #[snafu(display(
        "Configured trusted public JWKS includes local member {member_id}; local private JWKS already provides local identity."
    ))]
    TrustedPublicKeysForLocalMember { member_id: MemberIdentity },
    #[snafu(display("Configured trusted public JWKS includes non-group member {member_id}."))]
    UnexpectedTrustedPublicKeys { member_id: MemberIdentity },
    #[snafu(display("Failed to prepare initial static group security material: {source}"))]
    InitialGroupSecurity { source: ProvisionSecurityError },
    #[snafu(display(
        "Stored static group {group_id} security material does not match checklist config: {source}"
    ))]
    ExistingGroupSecurity {
        group_id: GroupId,
        source: ProvisionSecurityError,
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

    #[allow(
        clippy::needless_continue,
        reason = "The REPL loop uses explicit continues to make command-processing outcomes obvious."
    )]
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
            ChecklistCommand::Check => self.check_members(),
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

    fn check_members(&self) {
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
        let summaries = block_on(join_all(requests));
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

    let persisted_groups = load_persisted_groups(store).await?;

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
        validate_existing_static_group(existing_group, config, local_member_index)?;
        validate_existing_static_group_security(existing_group, config)?;
        let provisioned = provision_configured_security(store, config).await?;
        validate_provisioned_group_security(&members, &config.local_member, &provisioned)?;
        return Ok(());
    }

    let provisioned = provision_configured_security(store, config).await?;
    validate_provisioned_group_security(&members, &config.local_member, &provisioned)?;
    let security_material = prepare_initial_group_security_material(
        config.group_id,
        &config.replication_security,
        config.group_key.as_ref(),
    )
    .context(static_group_error::InitialGroupSecuritySnafu)?;
    let mut transaction = store
        .begin_transaction()
        .await
        .context(static_group_error::StoreSnafu)?;
    transaction
        .insert_replication_group(ReplicationGroupRecord {
            group_id: config.group_id,
            members: config.ordered_members.clone(),
            local_member_index,
            version_vector: VersionVector::initial(member_count),
            security_material,
        })
        .await
        .context(static_group_error::StoreSnafu)?;
    transaction
        .commit()
        .await
        .context(static_group_error::StoreSnafu)?;
    Ok(())
}

/// Validate an existing static group's encrypted key against current config.
fn validate_existing_static_group_security(
    existing_group: &ReplicationGroupRecord,
    config: &ChecklistAppConfig,
) -> Result<(), StaticGroupError> {
    validate_initial_group_security_material(
        existing_group.group_id,
        &config.replication_security,
        config.group_key.as_ref(),
        &existing_group.security_material,
    )
    .context(static_group_error::ExistingGroupSecuritySnafu {
        group_id: existing_group.group_id,
    })
}

/// Load current static-group records before deciding whether setup can mutate security state.
async fn load_persisted_groups(
    store: &dyn ReplicationStore,
) -> Result<Vec<ReplicationGroupRecord>, StaticGroupError> {
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(static_group_error::StoreSnafu)?;
    let groups = transaction
        .load_replication_groups()
        .await
        .context(static_group_error::StoreSnafu)?;
    transaction
        .release()
        .await
        .context(static_group_error::StoreSnafu)?;
    Ok(groups)
}

/// Validate the existing static group before temporary security provisioning writes records.
fn validate_existing_static_group(
    existing_group: &ReplicationGroupRecord,
    config: &ChecklistAppConfig,
    local_member_index: MemberIndex,
) -> Result<(), StaticGroupError> {
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
    Ok(())
}

async fn provision_configured_security(
    store: &dyn ReplicationStore,
    config: &ChecklistAppConfig,
) -> Result<flotsync_replication::ProvisionedReplicationSecurity, StaticGroupError> {
    let local_private_jwks = read_security_file(&config.local_private_jwks_path)?;
    let mut trusted_public_jwks = Vec::new();
    for path in &config.trusted_public_jwks_paths {
        let jwks = read_security_file(path)?;
        trusted_public_jwks.push(jwks);
    }
    let trusted_public_jwks = trusted_public_jwks.iter().map(String::as_str);
    provision_replication_security(
        store,
        &config.local_member,
        &config.replication_security,
        &local_private_jwks,
        trusted_public_jwks,
    )
    .await
    .context(static_group_error::ProvisionSecuritySnafu)
}

/// Read one configured security input file from disk.
fn read_security_file(path: &Path) -> Result<String, StaticGroupError> {
    fs::read_to_string(path).with_context(|_| static_group_error::ReadSecurityFileSnafu {
        path: path.to_path_buf(),
    })
}

/// Ensure provisioned trusted public keys exactly cover the configured remote group members.
fn validate_provisioned_group_security(
    members: &GroupMembers,
    local_member: &MemberIdentity,
    provisioned: &flotsync_replication::ProvisionedReplicationSecurity,
) -> Result<(), StaticGroupError> {
    let trusted_members: HashSet<_> = provisioned.trusted_members.iter().cloned().collect();
    for member in members.ordered_members() {
        if member == *local_member {
            continue;
        }
        ensure!(
            trusted_members.contains(&member),
            static_group_error::MissingTrustedPublicKeysSnafu { member_id: member }
        );
    }
    for trusted_member in trusted_members {
        ensure!(
            &trusted_member != local_member,
            static_group_error::TrustedPublicKeysForLocalMemberSnafu {
                member_id: trusted_member.clone(),
            }
        );
        ensure!(
            members.contains(&trusted_member),
            static_group_error::UnexpectedTrustedPublicKeysSnafu {
                member_id: trusted_member,
            }
        );
    }
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
    use flotsync_io::test_support::{ReservedSocketKind, reserve_sockets};
    use flotsync_replication::{RowKey, test_support::test_replication_security_secrets};
    use flotsync_security::{
        GROUP_KEY_LENGTH,
        GroupKey,
        local_member_keys_from_jwks,
        public_member_keys_from_jwks,
    };
    use uuid::Uuid;

    fn test_config(group_id: GroupId, store_path: PathBuf) -> ChecklistAppConfig {
        test_config_with_members(
            group_id,
            store_path,
            vec![MemberIdentity::from_array(["alice"])],
        )
    }

    fn test_config_with_members(
        group_id: GroupId,
        store_path: PathBuf,
        ordered_members: Vec<MemberIdentity>,
    ) -> ChecklistAppConfig {
        let local_member = MemberIdentity::from_array(["alice"]);
        let key_dir = std::env::temp_dir().join(format!("flotsync-checklist-test-{}", group_id.0));
        std::fs::create_dir_all(&key_dir).expect("test key dir should be created");
        let local_private_jwks_path = key_dir.join("alice-private.jwks");
        let local_keys = generate_member_key_files(local_member.clone())
            .expect("local test key files should generate");
        std::fs::write(
            &local_private_jwks_path,
            local_keys.local_private_jwks.as_str(),
        )
        .expect("local private JWKS should be written");
        let mut trusted_public_jwks_paths = Vec::new();
        for (index, member) in ordered_members.iter().enumerate() {
            if member == &local_member {
                continue;
            }
            let generated = generate_member_key_files(member.clone())
                .expect("trusted test key files should generate");
            let path = key_dir.join(format!("trusted-{index}.jwks"));
            std::fs::write(&path, &generated.public_jwks)
                .expect("trusted public JWKS should be written");
            trusted_public_jwks_paths.push(path);
        }
        ChecklistAppConfig {
            source_path: PathBuf::from("test.toml"),
            runtime_config_toml: String::new(),
            local_member,
            store_path,
            replication_security: test_replication_security_secrets(),
            group_key: test_group_key(1),
            local_private_jwks_path,
            trusted_public_jwks_paths,
            group_id,
            ordered_members,
            local_endpoint_bind_addr: "127.0.0.1:45100".parse().unwrap(),
        }
    }

    fn test_group_key(seed: u8) -> Arc<GroupKey> {
        Arc::new(GroupKey::from_bytes([seed; GROUP_KEY_LENGTH]))
    }

    fn test_runtime_config_toml(local_endpoint: std::net::SocketAddr) -> String {
        format!(
            r#"
            [flotsync.io]
            bind-reuse-address = true

            [flotsync.replication.runtime]
            local-endpoint-bind-addr = "{local_endpoint}"
            "#,
        )
    }

    fn add_trusted_public_key(config: &mut ChecklistAppConfig, member: MemberIdentity) {
        let key_dir = config
            .local_private_jwks_path
            .parent()
            .expect("test local key path should have parent");
        let generated =
            generate_member_key_files(member).expect("trusted test key files should generate");
        let path = key_dir.join(format!(
            "trusted-extra-{}.jwks",
            config.trusted_public_jwks_paths.len()
        ));
        std::fs::write(&path, &generated.public_jwks)
            .expect("trusted public JWKS should be written");
        config.trusted_public_jwks_paths.push(path);
    }

    fn sync_working_set(replication: &dyn ReplicationApi, working_set: &mut ChecklistWorkingSet) {
        let plan = working_set
            .prepare_sync()
            .expect("sync plan should build")
            .expect("dirty checklist row should produce a sync plan");
        let read_token = working_set
            .read_token()
            .expect("working set should have a read token");
        let receipt = block_on(replication.publish_changes(PublishChangesRequest {
            read_token,
            changes: plan.mutations.clone(),
        }))
        .expect("checklist sync publish should succeed");
        working_set.set_read_token(receipt.read_token);
        working_set.finish_successful_sync(Some(plan));
    }

    #[test]
    fn checklist_rename_with_two_insert_hunks_syncs() {
        kompact::test_support::init_test_logger();

        let endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
        let group_id = GroupId(Uuid::from_u128(0x45d0));
        let mut config = test_config(group_id, PathBuf::from("unused.sqlite"));
        config.runtime_config_toml = test_runtime_config_toml(endpoint_lease.addr(0));
        let store = Arc::new(
            SqliteReplicationStore::in_memory_with_schema_sources(
                config.local_member.clone(),
                [(checklist_dataset_id(), &*CHECKLIST_SCHEMA)],
            )
            .expect("store should build"),
        );
        block_on(ensure_configured_group(store.as_ref(), &config))
            .expect("static group should be prepared");
        let (listener, _listener_receiver) = ChecklistListener::pair();
        let replication = block_on(load_replication_runtime_with_runtime_config_toml(
            Identifier::from_array(["flotsync", "examples", "replicated-checklist"]),
            store,
            listener,
            ReplicationConfig::default(),
            config.replication_security.clone(),
            &config.runtime_config_toml,
        ))
        .expect("runtime should load");
        let mut working_set = load_checklist_working_set(replication.as_ref(), group_id)
            .expect("working set should load");
        let row_key = RowKey(Uuid::from_u128(1));

        working_set.add_item_with_key(row_key, "a");
        sync_working_set(replication.as_ref(), &mut working_set);
        // This rename produces two insert hunks around the existing character.
        working_set
            .rename_item(ItemSelector::RowKey(row_key), "xay")
            .expect("rename should apply locally");

        sync_working_set(replication.as_ref(), &mut working_set);
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
        validate_initial_group_security_material(
            group_id,
            &config.replication_security,
            config.group_key.as_ref(),
            &groups[0].security_material,
        )
        .expect("stored group key should match config");
    }

    #[test]
    fn ensure_configured_group_rejects_missing_trusted_public_keys() {
        let group_id = GroupId(Uuid::from_u128(102));
        let mut config = test_config(group_id, PathBuf::from("unused.sqlite"));
        config
            .ordered_members
            .push(MemberIdentity::from_array(["bob"]));
        let store = SqliteReplicationStore::in_memory(config.local_member.clone())
            .expect("store should build");

        let result = block_on(ensure_configured_group(&store, &config));

        assert!(matches!(
            result,
            Err(StaticGroupError::MissingTrustedPublicKeys { .. })
        ));
    }

    #[test]
    fn ensure_configured_group_provisions_security_that_runtime_loads() {
        let endpoint_lease = reserve_sockets(&[ReservedSocketKind::UdpSocket]);
        let group_id = GroupId(Uuid::from_u128(103));
        let mut config = test_config_with_members(
            group_id,
            PathBuf::from("unused.sqlite"),
            vec![
                MemberIdentity::from_array(["alice"]),
                MemberIdentity::from_array(["bob"]),
            ],
        );
        config.runtime_config_toml = test_runtime_config_toml(endpoint_lease.addr(0));
        let store = Arc::new(
            SqliteReplicationStore::in_memory(config.local_member.clone())
                .expect("store should build"),
        );

        block_on(ensure_configured_group(store.as_ref(), &config))
            .expect("group should be prepared");
        let (listener, _listener_receiver) = ChecklistListener::pair();
        let runtime = block_on(load_replication_runtime_with_runtime_config_toml(
            Identifier::from_array(["flotsync", "examples", "replicated-checklist"]),
            store,
            listener,
            ReplicationConfig::default(),
            config.replication_security.clone(),
            &config.runtime_config_toml,
        ))
        .expect("runtime should load provisioned security");

        drop(runtime);
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
        add_trusted_public_key(&mut existing, MemberIdentity::from_array(["carol"]));
        block_on(ensure_configured_group(&store, &existing))
            .expect("existing group should be inserted");

        let result = block_on(ensure_configured_group(&store, &config));

        assert!(matches!(
            result,
            Err(StaticGroupError::MemberMismatch { .. })
        ));
    }

    #[test]
    fn ensure_configured_group_rejects_existing_group_secret_mismatch() {
        let group_id = GroupId(Uuid::from_u128(104));
        let insert_config = test_config(group_id, PathBuf::from("unused.sqlite"));
        let store = SqliteReplicationStore::in_memory(insert_config.local_member.clone())
            .expect("store should build");
        block_on(ensure_configured_group(&store, &insert_config))
            .expect("existing group should be inserted");
        let mut mismatched_config = insert_config.clone();
        mismatched_config.group_key = test_group_key(2);

        let result = block_on(ensure_configured_group(&store, &mismatched_config));

        assert!(matches!(
            result,
            Err(StaticGroupError::ExistingGroupSecurity {
                source: ProvisionSecurityError::GroupSecretMismatch { .. },
                ..
            })
        ));
    }

    #[test]
    fn generate_checklist_key_files_writes_round_trippable_jwks() {
        let out_dir =
            std::env::temp_dir().join(format!("flotsync-checklist-keys-{}", Uuid::new_v4()));
        let member = MemberIdentity::from_array(["alice"]);

        generate_checklist_key_files(member.clone(), &out_dir, false)
            .expect("key generation should succeed");

        let private_path = out_dir.join(GENERATED_PRIVATE_JWKS_FILE_NAME);
        let private_jwks =
            std::fs::read_to_string(&private_path).expect("private JWKS should be written");
        let public_jwks = std::fs::read_to_string(out_dir.join(GENERATED_PUBLIC_JWKS_FILE_NAME))
            .expect("public JWKS should be written");
        let local_keys = local_member_keys_from_jwks(&private_jwks, Some(&member))
            .expect("private JWKS should parse");
        let public_keys = public_member_keys_from_jwks(&public_jwks, Some(&member))
            .expect("public JWKS should parse");

        assert_eq!(local_keys.member_id(), &member);
        assert_eq!(public_keys.member_id(), &member);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let permissions = std::fs::metadata(private_path)
                .expect("private JWKS metadata should load")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(permissions, 0o600);
        }
    }

    #[test]
    fn generate_checklist_key_files_refuses_overwrite_without_partial_private_file() {
        let out_dir =
            std::env::temp_dir().join(format!("flotsync-checklist-keys-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&out_dir).expect("key dir should be created");
        let private_path = out_dir.join(GENERATED_PRIVATE_JWKS_FILE_NAME);
        let public_path = out_dir.join(GENERATED_PUBLIC_JWKS_FILE_NAME);
        std::fs::write(&public_path, "existing-public").expect("existing public key should write");

        let result =
            generate_checklist_key_files(MemberIdentity::from_array(["alice"]), &out_dir, false);

        assert!(matches!(
            result,
            Err(ReplicatedChecklistError::KeyFileExists { .. })
        ));
        assert!(!private_path.exists());
        assert_eq!(
            std::fs::read_to_string(public_path).expect("existing public key should remain"),
            "existing-public"
        );
    }
}

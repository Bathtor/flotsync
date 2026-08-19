//! Command-surface helpers for the replicated checklist example.
//!
//! This module deliberately stops at the example command contract: parsing,
//! item addressing, and the checklist schema. The runnable process and runtime
//! wiring live in the example CLI task.

mod config;
mod runner;

pub use config::ChecklistConfigError;
pub use runner::{ReplicatedChecklistArgs, ReplicatedChecklistError, run};

use clap::{CommandFactory, Parser, Subcommand};
use flotsync_core::GroupId;
use flotsync_data_types::{
    Decode,
    DecodeValueError,
    Field,
    PrimitiveType,
    RowOperations,
    RowValueRead,
    Schema,
    schema::{BasicDataType, NullableBasicDataType, datamodel::NullableBasicValue},
};
use flotsync_replication::{
    DatasetId,
    ReadToken,
    RowChange,
    RowId,
    RowKey,
    RowMutation,
    RowValuesPatch,
    SnapshotValueRow,
};
use itertools::Itertools;
use snafu::prelude::*;
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    num::NonZeroUsize,
    str::FromStr,
    sync::LazyLock,
    time::SystemTime,
};
use uuid::Uuid;

/// Logical dataset id segments for the checklist example.
///
/// The current replication API only supports flat `DatasetId` values, so the
/// concrete dataset id below uses `checklist_items` until flotsync-hnu adds
/// structural dataset identifiers.
pub const CHECKLIST_DATASET_ID_SEGMENTS: [&str; 2] = ["checklist", "items"];
pub const CHECKLIST_DATASET_ID: &str = "checklist_items";
pub const FIELD_TEXT: &str = "text";
pub const FIELD_NOTE: &str = "note";
pub const FIELD_TAGS: &str = "tags";
pub const FIELD_STATUS: &str = "status";
pub const FIELD_PRIORITY: &str = "priority";
pub const FIELD_EDIT_COUNT: &str = "edit_count";
pub static CHECKLIST_SCHEMA: LazyLock<Schema> = LazyLock::new(build_checklist_schema);

/// # Panics
///
/// Panics if the static checklist dataset identifier is not a valid [`DatasetId`].
#[must_use]
pub fn checklist_dataset_id() -> DatasetId {
    DatasetId::try_from_static(CHECKLIST_DATASET_ID)
        .expect("checklist dataset id must be a valid dataset identifier")
}

fn build_checklist_schema() -> Schema {
    Schema::from_fields([
        Field::linear_string(FIELD_TEXT)
            .with_default("")
            .expect("text default must match LINEAR_STRING"),
        Field::linear_string(FIELD_NOTE)
            .with_default("")
            .expect("note default must match LINEAR_STRING"),
        Field::linear_list(FIELD_TAGS, PrimitiveType::String)
            .with_default(Vec::<String>::new())
            .expect("tags default must match LINEAR_LIST<STRING>"),
        Field::finite_state_register(FIELD_STATUS, ChecklistStatus::SCHEMA_STATES)
            .expect("status states must form a valid finite-state register")
            .with_default(ChecklistStatus::Open.as_str())
            .expect("status default must match the finite-state register"),
        Field::latest_value_wins(
            FIELD_PRIORITY,
            NullableBasicDataType::NonNull(BasicDataType::Primitive(PrimitiveType::Byte)),
        )
        .with_default(0u8)
        .expect("priority default must match LATEST_VALUE_WINS U8"),
        Field::monotonic_counter(FIELD_EDIT_COUNT),
    ])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChecklistStatus {
    Open,
    InProgress,
    Done,
}

impl ChecklistStatus {
    pub const OPEN: &'static str = "open";
    pub const IN_PROGRESS: &'static str = "in_progress";
    pub const DONE: &'static str = "done";
    pub const SCHEMA_STATES: [&'static str; 3] = [Self::OPEN, Self::IN_PROGRESS, Self::DONE];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => Self::OPEN,
            Self::InProgress => Self::IN_PROGRESS,
            Self::Done => Self::DONE,
        }
    }

    #[must_use]
    pub fn from_schema_value(value: &str) -> Option<Self> {
        match value {
            Self::OPEN => Some(Self::Open),
            Self::IN_PROGRESS => Some(Self::InProgress),
            Self::DONE => Some(Self::Done),
            _ => None,
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::Open => 0,
            Self::InProgress => 1,
            Self::Done => 2,
        }
    }
}

impl std::fmt::Display for ChecklistStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub enum ChecklistCommand {
    /// Add one new checklist item.
    Add {
        /// Item text.
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        text: Vec<String>,
    },
    /// Replace an item's text.
    Rename {
        /// Item list position, UUID, or qualified reference.
        item: ItemSelector,
        /// Replacement item text.
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        text: Vec<String>,
    },
    /// Edit longer item fields.
    Edit {
        /// Item list position, UUID, or qualified item reference.
        item: ItemSelector,
        #[command(subcommand)]
        command: EditCommand,
    },
    /// Add or remove item tags.
    Tag {
        #[command(subcommand)]
        command: TagCommand,
    },
    /// Mark an item as in progress.
    Claim {
        /// Item list position, UUID, or qualified reference.
        item: ItemSelector,
    },
    /// Mark an item as done.
    Complete {
        /// Item list position, UUID, or qualified reference.
        item: ItemSelector,
    },
    /// Set an item's priority.
    Priority {
        /// Item list position, UUID, or qualified reference.
        item: ItemSelector,
        /// Priority value to store on the item.
        priority: u8,
    },
    /// Delete an item.
    Delete {
        /// Item list position, UUID, or qualified reference.
        item: ItemSelector,
    },
    /// Print visible checklist items.
    List,
    /// Print all fields for one item.
    Show {
        /// Item list position, UUID, or qualified reference.
        item: ItemSelector,
    },
    /// Print queued replication events.
    Events {
        /// Maximum number of latest events to print.
        limit: Option<usize>,
    },
    /// Publish local changes and apply received updates.
    Sync,
    /// Print active group members.
    Members,
    /// Ask each active group member for its current group summary.
    Check,
    /// Print current peer-route diagnostics.
    Peers,
    /// Inspect groups and manage the session default.
    Group {
        #[command(subcommand)]
        command: ChecklistGroupCommand,
    },
    /// Manage public identity keys through the running replication runtime.
    Keys {
        #[command(subcommand)]
        command: ChecklistKeyCommand,
    },
    /// Print local member and store details.
    Me,
    /// Print command help.
    Help,
    /// Exit the REPL.
    #[command(alias = "exit")]
    Quit,
}

/// Runtime-backed public-key commands available inside the checklist REPL.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub enum ChecklistKeyCommand {
    /// Print this peer's copyable public key bundle.
    ExportLocal,
    /// Inspect a pasted public key bundle without changing local security state.
    Inspect {
        /// Pasteable public key bundle text.
        public_bundle: String,
    },
    /// Trust a pasted public key bundle for one exact member identity.
    Trust {
        /// Exact member identity to trust for the bundle.
        member_id: flotsync_core::MemberIdentity,
        /// Pasteable public key bundle text.
        public_bundle: String,
    },
    /// Block the fingerprint derived from a pasted public key bundle.
    Block {
        /// Pasteable public key bundle text.
        public_bundle: String,
    },
}

/// Multi-group workspace commands available inside the checklist REPL.
#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub enum ChecklistGroupCommand {
    /// List every readable group known to this workspace.
    List,
    /// Create a named replication group through a short interactive wizard.
    Create {
        /// Group name. Remaining words form one name.
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        name: Vec<String>,
    },
    /// List pending group invitations awaiting a local decision.
    Invitations,
    /// Accept one pending invitation by its displayed position.
    Accept {
        /// Positive invitation position from `group invitations`.
        invitation: NonZeroUsize,
    },
    /// Reject one pending invitation by its displayed position.
    Reject {
        /// Positive invitation position from `group invitations`.
        invitation: NonZeroUsize,
    },
    /// Select the writable group used by add, members, and check.
    Default {
        /// Exact group UUID or name. Remaining words form one name.
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        group: Vec<String>,
    },
    /// Clear the session-only default so new items are process-local.
    ClearDefault,
}

impl ChecklistCommand {
    #[must_use]
    pub const fn status_target(&self) -> Option<ChecklistStatus> {
        match self {
            Self::Claim { .. } => Some(ChecklistStatus::InProgress),
            Self::Complete { .. } => Some(ChecklistStatus::Done),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub enum EditCommand {
    /// Replace an item's note text.
    Note,
    /// Copy an item into another group while preserving its row UUID.
    Copy {
        /// Exact target group UUID or name. Remaining words form one name.
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        group: Vec<String>,
    },
    /// Move an item into another group while preserving its row UUID.
    Move {
        /// Exact target group UUID or name. Remaining words form one name.
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        group: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub enum TagCommand {
    /// Add one tag to an item.
    Add {
        /// Item list position, UUID, or qualified reference.
        item: ItemSelector,
        /// Tag text to add.
        tag: String,
    },
    /// Remove one tag from an item.
    Rm {
        /// Item list position, UUID, or qualified reference.
        item: ItemSelector,
        /// Tag text to remove.
        tag: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ItemSelector {
    /// Transient one-based position from the current checklist display.
    ListIndex(NonZeroUsize),
    /// Bare row UUID, accepted only when it is unique across associations.
    RowKey(RowKey),
    /// Association-qualified row UUID.
    Qualified {
        /// Process-local marker or real-group selector.
        association: ItemAssociationSelector,
        /// Row UUID within the selected association.
        row_key: RowKey,
    },
}

/// Association portion of one qualified checklist item reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ItemAssociationSelector {
    /// The process-local working-set association.
    Local,
    /// Exact real-group UUID or unique display name.
    Group(String),
}

impl FromStr for ItemSelector {
    type Err = ChecklistCommandParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_item_selector(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
pub enum ChecklistCommandParseError {
    #[snafu(display("{message}"))]
    Command { message: String },
    #[snafu(display(
        "Item reference '{value}' is not a positive list position, full item UUID, or qualified association/item UUID."
    ))]
    InvalidItemReference { value: String },
}

/// # Errors
///
/// See `ChecklistCommandParseError` for failure conditions.
pub fn parse_checklist_command(
    line: &str,
) -> Result<Option<ChecklistCommand>, ChecklistCommandParseError> {
    let args = split_repl_args(line);
    if args.is_empty() {
        return Ok(None);
    }

    let parsed = ChecklistLine::try_parse_from(std::iter::once("replicated-checklist").chain(args))
        .map_err(|source| ChecklistCommandParseError::Command {
            message: source.to_string(),
        })?;
    Ok(Some(parsed.command))
}

/// # Errors
///
/// See `ChecklistCommandParseError` for failure conditions.
pub fn parse_item_selector(value: &str) -> Result<ItemSelector, ChecklistCommandParseError> {
    if !value.is_empty() && value.chars().all(|character| character.is_ascii_digit()) {
        let Ok(index) = value.parse::<usize>() else {
            return Err(ChecklistCommandParseError::InvalidItemReference {
                value: value.to_owned(),
            });
        };
        let Some(index) = NonZeroUsize::new(index) else {
            return Err(ChecklistCommandParseError::InvalidItemReference {
                value: value.to_owned(),
            });
        };
        return Ok(ItemSelector::ListIndex(index));
    }

    if let Some((association, row_key)) = value.rsplit_once('/') {
        let Ok(row_key) = Uuid::parse_str(row_key) else {
            return Err(ChecklistCommandParseError::InvalidItemReference {
                value: value.to_owned(),
            });
        };
        if association.is_empty() {
            return Err(ChecklistCommandParseError::InvalidItemReference {
                value: value.to_owned(),
            });
        }
        let association = if association == "local" {
            ItemAssociationSelector::Local
        } else {
            ItemAssociationSelector::Group(association.to_owned())
        };
        return Ok(ItemSelector::Qualified {
            association,
            row_key: RowKey(row_key),
        });
    }

    let Ok(row_key) = Uuid::parse_str(value) else {
        return Err(ChecklistCommandParseError::InvalidItemReference {
            value: value.to_owned(),
        });
    };
    Ok(ItemSelector::RowKey(RowKey(row_key)))
}

#[must_use]
pub fn checklist_help() -> String {
    ChecklistLine::command().render_long_help().to_string()
}

/// Checklist row state decoded into example-specific Rust values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChecklistItem {
    /// User-facing checklist text.
    pub text: String,
    /// Longer free-form note text edited separately from the title.
    pub note: String,
    /// Tags are stored as a set locally so display order converges before sync.
    pub tags: BTreeSet<String>,
    /// Monotonic checklist workflow state.
    pub status: ChecklistStatus,
    /// User-selected priority, stored as a byte in the replicated row.
    pub priority: u8,
    /// Local edit counter used by the example to make edits visible in the row.
    pub edit_count: u64,
}

impl ChecklistItem {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            note: String::new(),
            tags: BTreeSet::new(),
            status: ChecklistStatus::Open,
            priority: 0,
            edit_count: 1,
        }
    }

    #[must_use]
    pub fn formatted_tags(&self) -> String {
        self.tags
            .iter()
            .format_with(" ", |tag, formatter| formatter(&format_args!("#{tag}")))
            .to_string()
    }

    fn from_row(
        row_key: RowKey,
        row: &(impl RowValueRead + ?Sized),
    ) -> Result<Self, ChecklistWorkingSetError> {
        let text = decode_row_field::<String>(row_key, row, FIELD_TEXT)?.into_owned();
        let note = decode_row_field::<String>(row_key, row, FIELD_NOTE)?.into_owned();
        let tags = decode_row_field::<Vec<String>>(row_key, row, FIELD_TAGS)?
            .iter()
            .cloned()
            .collect();
        let status = decode_row_field::<String>(row_key, row, FIELD_STATUS)?;
        let status = ChecklistStatus::from_schema_value(status.as_ref()).ok_or_else(|| {
            ChecklistWorkingSetError::InvalidStatus {
                row_key,
                value: status.into_owned(),
            }
        })?;
        let priority = decode_row_field::<u8>(row_key, row, FIELD_PRIORITY)?.into_owned();
        let edit_count = decode_row_field::<u64>(row_key, row, FIELD_EDIT_COUNT)?.into_owned();

        Ok(Self {
            text,
            note,
            tags,
            status,
            priority,
            edit_count,
        })
    }

    fn to_row_values_patch(&self) -> RowValuesPatch {
        let mut fields = HashMap::new();
        insert_field(&mut fields, FIELD_TEXT, self.text.clone());
        insert_field(&mut fields, FIELD_NOTE, self.note.clone());
        insert_field(
            &mut fields,
            FIELD_TAGS,
            self.tags.iter().cloned().collect::<Vec<_>>(),
        );
        insert_field(&mut fields, FIELD_STATUS, self.status.as_str().to_owned());
        insert_field(&mut fields, FIELD_PRIORITY, self.priority);
        insert_field(&mut fields, FIELD_EDIT_COUNT, self.edit_count);
        RowValuesPatch::new(fields)
    }

    fn changed_fields_since(&self, original: &Self) -> RowValuesPatch {
        let mut fields = HashMap::new();
        if self.text != original.text {
            insert_field(&mut fields, FIELD_TEXT, self.text.clone());
        }
        if self.note != original.note {
            insert_field(&mut fields, FIELD_NOTE, self.note.clone());
        }
        if self.tags != original.tags {
            insert_field(
                &mut fields,
                FIELD_TAGS,
                self.tags.iter().cloned().collect::<Vec<_>>(),
            );
        }
        if self.status != original.status {
            insert_field(&mut fields, FIELD_STATUS, self.status.as_str().to_owned());
        }
        if self.priority != original.priority {
            insert_field(&mut fields, FIELD_PRIORITY, self.priority);
        }
        if self.edit_count != original.edit_count {
            insert_field(&mut fields, FIELD_EDIT_COUNT, self.edit_count);
        }
        RowValuesPatch::new(fields)
    }

    fn increment_edit_count(&mut self) {
        self.edit_count = self
            .edit_count
            .checked_add(1)
            .expect("checklist edit_count must not overflow during an example run");
    }
}

/// Checklist-specific row change buffered from the replication listener.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChecklistRowChange {
    /// A listener-visible full-row upsert for this checklist dataset.
    Upsert {
        item_id: ChecklistItemId,
        item: ChecklistItem,
    },
    /// A listener-visible row deletion for this checklist dataset.
    Delete { item_id: ChecklistItemId },
}

impl ChecklistRowChange {
    /// Return the stable workspace identity affected by this listener change.
    #[must_use]
    pub const fn item_id(&self) -> ChecklistItemId {
        match self {
            Self::Upsert { item_id, .. } | Self::Delete { item_id } => *item_id,
        }
    }
}

/// Association between one checklist item and replicated storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ChecklistItemAssociation {
    /// Process-local item that is not published by synchronisation.
    Local,
    /// Item stored in one real replication group.
    Group(GroupId),
}

/// Stable checklist identity within the heterogeneous process workspace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChecklistItemId {
    /// Storage association that scopes the row UUID.
    pub association: ChecklistItemAssociation,
    /// Row UUID that is unique only within the association.
    pub row_key: RowKey,
}

impl ChecklistItemId {
    /// Build one process-local item identity.
    #[must_use]
    pub const fn local(row_key: RowKey) -> Self {
        Self {
            association: ChecklistItemAssociation::Local,
            row_key,
        }
    }

    /// Build one real-group item identity.
    #[must_use]
    pub const fn group(group_id: GroupId, row_key: RowKey) -> Self {
        Self {
            association: ChecklistItemAssociation::Group(group_id),
            row_key,
        }
    }
}

/// One queued listener batch in checklist terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChecklistEvent {
    /// Local receive timestamp for display in the example event log.
    pub timestamp: SystemTime,
    /// Decoded checklist changes carried by one listener batch.
    pub changes: Vec<ChecklistRowChange>,
}

/// Mutations prepared from dirty rows for one explicit sync command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChecklistSyncPlan {
    item_ids: HashSet<ChecklistItemId>,
    /// Row mutations to pass to `publish_changes`.
    pub mutations: Vec<RowMutation>,
}

impl ChecklistSyncPlan {
    /// Item identities whose dirty state is acknowledged by this plan.
    pub fn item_ids(&self) -> impl Iterator<Item = ChecklistItemId> + '_ {
        self.item_ids.iter().copied()
    }
}

/// Checklist item paired with its transient one-based display index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedChecklistItem<'a> {
    /// One-based list position accepted by REPL commands.
    pub index: NonZeroUsize,
    /// Stable workspace identity including local or real-group association.
    pub item_id: ChecklistItemId,
    /// Checklist item currently visible in the working set.
    pub item: &'a ChecklistItem,
}

/// Errors raised while decoding rows or mutating the checklist working set.
#[derive(Debug, Snafu)]
pub enum ChecklistWorkingSetError {
    #[snafu(display("Checklist item {item_id:?} is not visible in the working set."))]
    UnknownItem { item_id: ChecklistItemId },
    #[snafu(display(
        "Checklist item {source_id:?} is already associated with target {target_association:?}."
    ))]
    SameTransferAssociation {
        source_id: ChecklistItemId,
        target_association: ChecklistItemAssociation,
    },
    #[snafu(display(
        "Checklist transfer target {target_id:?} already exists with different contents."
    ))]
    TransferTargetCollision { target_id: ChecklistItemId },
    #[snafu(display("Replicated row {row} does not belong to checklist dataset {dataset}."))]
    UnexpectedRowDataset { row: RowId, dataset: DatasetId },
    #[snafu(display("Failed to decode field {field} from checklist row {row_key}: {source}"))]
    DecodeRowField {
        row_key: RowKey,
        field: &'static str,
        source: DecodeValueError,
    },
    #[snafu(display("Checklist row {row_key} has unsupported status value {value:?}."))]
    InvalidStatus { row_key: RowKey, value: String },
    #[snafu(display(
        "Checklist snapshot unexpectedly included deleted row {row_id}; startup only requests visible rows."
    ))]
    UnexpectedDeletedSnapshotRow { row_id: RowId },
    #[snafu(display("Dirty checklist item {item_id:?} is missing from the working set."))]
    MissingDirtyItem { item_id: ChecklistItemId },
    #[snafu(display(
        "Incoming replication change conflicts with dirty checklist item {item_id:?}. Restart the checklist to reload current snapshots before continuing."
    ))]
    IncomingChangeForDirtyItem { item_id: ChecklistItemId },
    #[snafu(display("Checklist working set does not have a replication read token."))]
    MissingReadToken,
}

/// In-memory REPL view of checklist rows between explicit `sync` commands.
///
/// Local commands update this working set and remember the original row state
/// for dirty rows. Listener changes are queued here and applied when `sync`
/// drains the queue. Listener batches that unexpectedly conflict with an
/// unsynchronised local change are rejected rather than rebased in the example.
pub struct ChecklistWorkingSet {
    dataset_id: DatasetId,
    rows: HashMap<ChecklistItemId, ChecklistItem>,
    display_order: Vec<ChecklistItemId>,
    dirty_rows: HashMap<ChecklistItemId, DirtyRowKind>,
    queued_events: VecDeque<ChecklistEvent>,
    event_history: Vec<ChecklistEvent>,
    read_token: Option<ReadToken>,
}

impl ChecklistWorkingSet {
    #[must_use]
    pub fn new() -> Self {
        Self {
            dataset_id: checklist_dataset_id(),
            rows: HashMap::new(),
            display_order: Vec::new(),
            dirty_rows: HashMap::new(),
            queued_events: VecDeque::new(),
            event_history: Vec::new(),
            read_token: None,
        }
    }

    #[must_use]
    pub fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    #[must_use]
    pub fn item(&self, item_id: ChecklistItemId) -> Option<&ChecklistItem> {
        self.rows.get(&item_id)
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn require_listed_item(
        &self,
        item_id: ChecklistItemId,
    ) -> Result<ListedChecklistItem<'_>, ChecklistWorkingSetError> {
        self.listed_item(item_id)
            .ok_or(ChecklistWorkingSetError::UnknownItem { item_id })
    }

    /// Return every visible identity with one row UUID in stable association order.
    #[must_use]
    pub fn item_ids_with_row_key(&self, row_key: RowKey) -> Vec<ChecklistItemId> {
        let mut item_ids = self
            .rows
            .keys()
            .filter(|item_id| item_id.row_key == row_key)
            .copied()
            .collect::<Vec<_>>();
        item_ids.sort();
        item_ids
    }

    /// Return one visible item by its already-resolved workspace identity.
    #[must_use]
    pub fn listed_item(&self, item_id: ChecklistItemId) -> Option<ListedChecklistItem<'_>> {
        self.listed_items()
            .into_iter()
            .find(|listed| listed.item_id == item_id)
    }

    #[must_use]
    pub fn dirty_row_count(&self) -> usize {
        self.dirty_rows.len()
    }

    /// Number of unsynchronised process-local items.
    #[must_use]
    pub fn dirty_local_item_count(&self) -> usize {
        self.dirty_rows
            .keys()
            .filter(|item_id| item_id.association == ChecklistItemAssociation::Local)
            .count()
    }

    /// Real groups that still contain unsynchronised items.
    #[must_use]
    pub fn dirty_group_ids(&self) -> HashSet<GroupId> {
        self.dirty_rows
            .keys()
            .filter_map(|item_id| match item_id.association {
                ChecklistItemAssociation::Local => None,
                ChecklistItemAssociation::Group(group_id) => Some(group_id),
            })
            .collect()
    }

    #[must_use]
    pub fn queued_event_count(&self) -> usize {
        self.queued_events.len()
    }

    #[must_use]
    pub fn events(&self) -> &[ChecklistEvent] {
        &self.event_history
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn read_token(&self) -> Result<ReadToken, ChecklistWorkingSetError> {
        self.read_token
            .clone()
            .ok_or(ChecklistWorkingSetError::MissingReadToken)
    }

    pub fn set_read_token(&mut self, read_token: ReadToken) {
        self.read_token = Some(read_token);
    }

    pub fn merge_read_token(&mut self, read_token: ReadToken) {
        if let Some(existing_token) = &mut self.read_token {
            existing_token.merge_applied(&read_token);
        } else {
            self.read_token = Some(read_token);
        }
    }

    /// # Panics
    ///
    /// Panics if the one-based display index overflows `usize`.
    #[must_use]
    pub fn listed_items(&self) -> Vec<ListedChecklistItem<'_>> {
        self.display_order
            .iter()
            .filter_map(|item_id| self.rows.get(item_id).map(|item| (*item_id, item)))
            .enumerate()
            .map(|(index, (item_id, item))| ListedChecklistItem {
                index: NonZeroUsize::new(index + 1)
                    .expect("enumerated list positions start at one"),
                item_id,
                item,
            })
            .collect()
    }

    pub fn add_item(
        &mut self,
        association: ChecklistItemAssociation,
        text: impl Into<String>,
    ) -> ChecklistItemId {
        let row_key = RowKey(Uuid::new_v4());
        let item_id = ChecklistItemId {
            association,
            row_key,
        };
        self.add_item_with_id(item_id, text);
        item_id
    }

    pub fn add_item_with_id(&mut self, item_id: ChecklistItemId, text: impl Into<String>) {
        self.rows.insert(item_id, ChecklistItem::new(text));
        push_display_item(&mut self.display_order, item_id);
        self.dirty_rows.insert(item_id, DirtyRowKind::Insert);
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn rename_item(
        &mut self,
        item_id: ChecklistItemId,
        text: impl Into<String>,
    ) -> Result<(), ChecklistWorkingSetError> {
        let text = text.into();
        self.modify_item(item_id, |item| {
            if item.text == text {
                return false;
            }
            item.text = text;
            item.increment_edit_count();
            true
        })
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn edit_note(
        &mut self,
        item_id: ChecklistItemId,
        note: impl Into<String>,
    ) -> Result<(), ChecklistWorkingSetError> {
        let note = note.into();
        self.modify_item(item_id, |item| {
            if item.note == note {
                return false;
            }
            item.note = note;
            item.increment_edit_count();
            true
        })
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn add_tag(
        &mut self,
        item_id: ChecklistItemId,
        tag: impl Into<String>,
    ) -> Result<(), ChecklistWorkingSetError> {
        let tag = tag.into();
        self.modify_item(item_id, |item| {
            if item.tags.insert(tag) {
                item.increment_edit_count();
                return true;
            }
            false
        })
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn remove_tag(
        &mut self,
        item_id: ChecklistItemId,
        tag: &str,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.modify_item(item_id, |item| {
            if item.tags.remove(tag) {
                item.increment_edit_count();
                return true;
            }
            false
        })
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn claim_item(&mut self, item_id: ChecklistItemId) -> Result<(), ChecklistWorkingSetError> {
        self.advance_status(item_id, ChecklistStatus::InProgress)
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn complete_item(
        &mut self,
        item_id: ChecklistItemId,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.advance_status(item_id, ChecklistStatus::Done)
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn set_priority(
        &mut self,
        item_id: ChecklistItemId,
        priority: u8,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.modify_item(item_id, |item| {
            if item.priority == priority {
                return false;
            }
            item.priority = priority;
            item.increment_edit_count();
            true
        })
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn delete_item(
        &mut self,
        item_id: ChecklistItemId,
    ) -> Result<(), ChecklistWorkingSetError> {
        let Some(_item) = self.rows.remove(&item_id) else {
            return Err(ChecklistWorkingSetError::UnknownItem { item_id });
        };
        self.display_order.retain(|candidate| *candidate != item_id);
        match self.dirty_rows.remove(&item_id) {
            Some(DirtyRowKind::Insert) => {}
            _ => {
                self.dirty_rows.insert(item_id, DirtyRowKind::Delete);
            }
        }
        Ok(())
    }

    /// Stage a complete UUID-preserving copy in another association.
    ///
    /// An identical existing target is treated as an idempotent success. A
    /// divergent target is never overwritten.
    ///
    /// # Errors
    ///
    /// Returns an error when the source is absent, the target association is
    /// unchanged, or the target identity already has different contents.
    pub fn copy_item(
        &mut self,
        source_id: ChecklistItemId,
        target_association: ChecklistItemAssociation,
    ) -> Result<ChecklistItemId, ChecklistWorkingSetError> {
        if source_id.association == target_association {
            return Err(ChecklistWorkingSetError::SameTransferAssociation {
                source_id,
                target_association,
            });
        }
        let source = self
            .rows
            .get(&source_id)
            .cloned()
            .ok_or(ChecklistWorkingSetError::UnknownItem { item_id: source_id })?;
        let target_id = ChecklistItemId {
            association: target_association,
            row_key: source_id.row_key,
        };
        match self.rows.get(&target_id) {
            Some(target) if *target == source => Ok(target_id),
            Some(_) => Err(ChecklistWorkingSetError::TransferTargetCollision { target_id }),
            None => {
                self.rows.insert(target_id, source);
                push_display_item(&mut self.display_order, target_id);
                self.dirty_rows.insert(target_id, DirtyRowKind::Insert);
                Ok(target_id)
            }
        }
    }

    /// Stage a complete UUID-preserving target copy and remove the source locally.
    ///
    /// Existing replicated sources become tombstones through the ordinary dirty
    /// row path. Process-local and never-published sources simply disappear.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as [`Self::copy_item`], or an error if
    /// the source unexpectedly disappears before its local removal.
    pub fn move_item(
        &mut self,
        source_id: ChecklistItemId,
        target_association: ChecklistItemAssociation,
    ) -> Result<ChecklistItemId, ChecklistWorkingSetError> {
        let target_id = self.copy_item(source_id, target_association)?;
        self.delete_item(source_id)?;
        Ok(target_id)
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn enqueue_row_changes(
        &mut self,
        changes: Vec<RowChange>,
    ) -> Result<(), ChecklistWorkingSetError> {
        let mut checklist_changes = Vec::with_capacity(changes.len());
        for change in changes {
            checklist_changes.push(self.checklist_change_from_row_change(change)?);
        }
        self.enqueue_checklist_changes(checklist_changes)
    }

    /// Queue one decoded listener batch after checking for dirty-row conflicts.
    ///
    /// # Errors
    ///
    /// Returns [`ChecklistWorkingSetError::IncomingChangeForDirtyItem`] without
    /// changing the working set if any change targets a dirty item.
    pub fn enqueue_checklist_changes(
        &mut self,
        changes: Vec<ChecklistRowChange>,
    ) -> Result<(), ChecklistWorkingSetError> {
        if changes.is_empty() {
            return Ok(());
        }
        if let Some(item_id) = changes
            .iter()
            .map(ChecklistRowChange::item_id)
            .find(|item_id| self.dirty_rows.contains_key(item_id))
        {
            return Err(ChecklistWorkingSetError::IncomingChangeForDirtyItem { item_id });
        }
        let event = ChecklistEvent {
            timestamp: SystemTime::now(),
            changes,
        };
        self.event_history.push(event.clone());
        self.queued_events.push_back(event);
        Ok(())
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn apply_snapshot_rows<'a, I>(&mut self, rows: I) -> Result<(), ChecklistWorkingSetError>
    where
        I: IntoIterator<Item = SnapshotValueRow<'a>>,
    {
        for row in rows {
            let change = self.checklist_change_from_snapshot_row(&row)?;
            self.apply_checklist_change(change);
        }
        Ok(())
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn prepare_group_sync(
        &self,
        group_id: GroupId,
    ) -> Result<Option<ChecklistSyncPlan>, ChecklistWorkingSetError> {
        let mut item_ids = HashSet::with_capacity(self.dirty_rows.len());
        let mut mutations = Vec::with_capacity(self.dirty_rows.len());
        for (&item_id, dirty_row) in &self.dirty_rows {
            if item_id.association != ChecklistItemAssociation::Group(group_id) {
                continue;
            }
            item_ids.insert(item_id);
            let row_id = RowId {
                group_id,
                dataset_id: self.dataset_id.clone(),
                row_key: item_id.row_key,
            };
            match dirty_row {
                DirtyRowKind::Insert => {
                    let item = self
                        .rows
                        .get(&item_id)
                        .ok_or(ChecklistWorkingSetError::MissingDirtyItem { item_id })?;
                    mutations.push(RowMutation::Upsert {
                        row_id,
                        row: item.to_row_values_patch(),
                    });
                }
                DirtyRowKind::Update { original } => {
                    let item = self
                        .rows
                        .get(&item_id)
                        .ok_or(ChecklistWorkingSetError::MissingDirtyItem { item_id })?;
                    mutations.push(RowMutation::Upsert {
                        row_id,
                        row: item.changed_fields_since(original),
                    });
                }
                DirtyRowKind::Delete => {
                    mutations.push(RowMutation::Delete { row_id });
                }
            }
        }

        if item_ids.is_empty() {
            return Ok(None);
        }
        Ok(Some(ChecklistSyncPlan {
            item_ids,
            mutations,
        }))
    }

    /// Mark every item in a successfully published group plan as clean.
    ///
    /// Listener events remain queued separately and must be drained only after
    /// this method has cleared the published rows.
    pub fn finish_successful_group_sync(&mut self, plan: Option<ChecklistSyncPlan>) {
        if let Some(plan) = plan {
            for item_id in plan.item_ids {
                self.dirty_rows.remove(&item_id);
            }
        }
    }

    pub fn drain_queued_events(&mut self) -> usize {
        let mut applied_events = 0;
        while let Some(event) = self.queued_events.pop_front() {
            for change in event.changes {
                self.apply_checklist_change(change);
            }
            applied_events += 1;
        }
        applied_events
    }

    fn advance_status(
        &mut self,
        item_id: ChecklistItemId,
        target: ChecklistStatus,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.modify_item(item_id, |item| {
            if item.status.rank() >= target.rank() {
                return false;
            }
            item.status = target;
            item.increment_edit_count();
            true
        })
    }

    fn modify_item(
        &mut self,
        item_id: ChecklistItemId,
        update: impl FnOnce(&mut ChecklistItem) -> bool,
    ) -> Result<(), ChecklistWorkingSetError> {
        let Some(original) = self.rows.get(&item_id).cloned() else {
            return Err(ChecklistWorkingSetError::UnknownItem { item_id });
        };
        let item = self
            .rows
            .get_mut(&item_id)
            .expect("item checked above must remain visible");
        if update(item) {
            self.mark_dirty(item_id, original);
        }
        Ok(())
    }

    fn mark_dirty(&mut self, item_id: ChecklistItemId, original: ChecklistItem) {
        self.dirty_rows
            .entry(item_id)
            .or_insert(DirtyRowKind::Update { original });
    }

    fn checklist_change_from_row_change(
        &self,
        change: RowChange,
    ) -> Result<ChecklistRowChange, ChecklistWorkingSetError> {
        match change {
            RowChange::Upsert { row_id, row } => {
                self.checklist_upsert_change_from_row(&row_id, row.as_ref())
            }
            RowChange::Delete { row_id } => {
                self.validate_row_dataset(&row_id)?;
                Ok(ChecklistRowChange::Delete {
                    item_id: ChecklistItemId::group(row_id.group_id, row_id.row_key),
                })
            }
        }
    }

    fn checklist_change_from_snapshot_row(
        &self,
        row: &SnapshotValueRow<'_>,
    ) -> Result<ChecklistRowChange, ChecklistWorkingSetError> {
        let row_id = row.row_id().clone();
        if row.is_tombstoned() {
            self.validate_row_dataset(&row_id)?;
            return Err(ChecklistWorkingSetError::UnexpectedDeletedSnapshotRow { row_id });
        }
        self.checklist_upsert_change_from_row(&row_id, row)
    }

    fn checklist_upsert_change_from_row(
        &self,
        row_id: &RowId,
        row: &(impl RowValueRead + ?Sized),
    ) -> Result<ChecklistRowChange, ChecklistWorkingSetError> {
        self.validate_row_dataset(row_id)?;
        let row_key = row_id.row_key;
        let item = ChecklistItem::from_row(row_key, row)?;
        Ok(ChecklistRowChange::Upsert {
            item_id: ChecklistItemId::group(row_id.group_id, row_key),
            item,
        })
    }

    fn validate_row_dataset(&self, row_id: &RowId) -> Result<(), ChecklistWorkingSetError> {
        ensure!(
            row_id.dataset_id == self.dataset_id,
            UnexpectedRowDatasetSnafu {
                row: row_id.clone(),
                dataset: self.dataset_id.clone(),
            }
        );
        Ok(())
    }

    fn apply_checklist_change(&mut self, change: ChecklistRowChange) {
        match change {
            ChecklistRowChange::Upsert { item_id, item } => {
                self.rows.insert(item_id, item);
                push_display_item(&mut self.display_order, item_id);
            }
            ChecklistRowChange::Delete { item_id } => {
                self.rows.remove(&item_id);
                self.display_order.retain(|candidate| *candidate != item_id);
            }
        }
    }
}

impl Default for ChecklistWorkingSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Dirty state for a row in the local working set.
#[derive(Clone, Debug, PartialEq, Eq)]
enum DirtyRowKind {
    /// Row was created locally and has not been published yet.
    Insert,
    /// Existing row was modified locally; `original` is the snapshot visible
    /// before the first unsynchronised local edit.
    Update { original: ChecklistItem },
    /// Existing row was deleted locally and needs a delete mutation.
    Delete,
}

fn insert_field(
    fields: &mut HashMap<String, NullableBasicValue>,
    name: &'static str,
    value: impl Into<NullableBasicValue>,
) {
    fields.insert(name.to_owned(), value.into());
}

fn push_display_item(display_order: &mut Vec<ChecklistItemId>, item_id: ChecklistItemId) {
    if display_order.iter().all(|candidate| *candidate != item_id) {
        display_order.push(item_id);
    }
}

fn decode_row_field<'a, Value>(
    row_key: RowKey,
    row: &'a (impl RowValueRead + ?Sized),
    field: &'static str,
) -> Result<Cow<'a, Value>, ChecklistWorkingSetError>
where
    Value: ?Sized + Decode,
{
    row.get_field_value::<Value>(field)
        .context(DecodeRowFieldSnafu { row_key, field })
}

#[derive(Debug, Parser)]
#[command(
    name = "replicated-checklist",
    disable_help_subcommand = true,
    about = "Replicated checklist REPL commands"
)]
struct ChecklistLine {
    #[command(subcommand)]
    command: ChecklistCommand,
}

fn split_repl_args(line: &str) -> Vec<&str> {
    line.split_whitespace().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_data_types::{InMemoryValueData, ReplicatedDataType, RowValues};

    #[test]
    fn checklist_schema_uses_agreed_replication_semantics() {
        let schema = &*CHECKLIST_SCHEMA;
        let expected_status =
            Field::finite_state_register(FIELD_STATUS, ChecklistStatus::SCHEMA_STATES)
                .expect("test status field should build")
                .data_type;

        assert_eq!(CHECKLIST_DATASET_ID_SEGMENTS, ["checklist", "items"]);
        assert_eq!(checklist_dataset_id().as_str(), CHECKLIST_DATASET_ID);
        assert_eq!(schema.columns.len(), 6);
        assert_eq!(
            schema[FIELD_TEXT].data_type,
            ReplicatedDataType::LinearString
        );
        assert_eq!(
            schema[FIELD_NOTE].data_type,
            ReplicatedDataType::LinearString
        );
        assert_eq!(
            schema[FIELD_TAGS].data_type,
            ReplicatedDataType::LinearList {
                value_type: PrimitiveType::String
            }
        );
        assert_eq!(schema[FIELD_STATUS].data_type, expected_status);
        assert_eq!(
            schema[FIELD_PRIORITY].data_type,
            ReplicatedDataType::LatestValueWins {
                value_type: NullableBasicDataType::NonNull(BasicDataType::Primitive(
                    PrimitiveType::Byte
                )),
            }
        );
        assert_eq!(
            schema[FIELD_EDIT_COUNT].data_type,
            ReplicatedDataType::MonotonicCounter { small_range: false }
        );
    }

    #[test]
    fn checklist_status_maps_to_readable_schema_values() {
        assert_eq!(ChecklistStatus::Open.as_str(), ChecklistStatus::OPEN);
        assert_eq!(
            ChecklistStatus::InProgress.as_str(),
            ChecklistStatus::IN_PROGRESS
        );
        assert_eq!(ChecklistStatus::Done.as_str(), ChecklistStatus::DONE);
        assert_eq!(
            ChecklistStatus::SCHEMA_STATES,
            [
                ChecklistStatus::OPEN,
                ChecklistStatus::IN_PROGRESS,
                ChecklistStatus::DONE
            ]
        );
    }

    #[test]
    fn parses_text_commands_with_tail_arguments() {
        assert_eq!(
            parse_checklist_command("add buy oat milk").expect("command should parse"),
            Some(ChecklistCommand::Add {
                text: words(["buy", "oat", "milk"]),
            })
        );
        assert_eq!(
            parse_checklist_command("rename 3 buy bread").expect("command should parse"),
            Some(ChecklistCommand::Rename {
                item: ItemSelector::ListIndex(NonZeroUsize::new(3).unwrap()),
                text: words(["buy", "bread"]),
            })
        );
    }

    #[test]
    fn parses_item_uuid_references() {
        let row_key = Uuid::from_u128(42);
        let command = format!("complete {row_key}");

        assert_eq!(
            parse_checklist_command(&command).expect("command should parse"),
            Some(ChecklistCommand::Complete {
                item: ItemSelector::RowKey(RowKey(row_key)),
            })
        );
    }

    #[test]
    fn parses_qualified_item_references_and_item_first_edits() {
        let row_key = Uuid::from_u128(43);
        let local_reference = format!("local/{row_key}");
        let named_reference = format!("shared/errands/{row_key}");

        assert_eq!(
            parse_item_selector(&local_reference).expect("local reference should parse"),
            ItemSelector::Qualified {
                association: ItemAssociationSelector::Local,
                row_key: RowKey(row_key),
            }
        );
        assert_eq!(
            parse_item_selector(&named_reference).expect("final slash should delimit the UUID"),
            ItemSelector::Qualified {
                association: ItemAssociationSelector::Group("shared/errands".to_owned()),
                row_key: RowKey(row_key),
            }
        );
        assert_eq!(
            parse_checklist_command(&format!("edit shared/{row_key} copy family errands"))
                .expect("copy command should parse"),
            Some(ChecklistCommand::Edit {
                item: ItemSelector::Qualified {
                    association: ItemAssociationSelector::Group("shared".to_owned()),
                    row_key: RowKey(row_key),
                },
                command: EditCommand::Copy {
                    group: words(["family", "errands"]),
                },
            })
        );
        assert_eq!(
            parse_checklist_command(&format!("edit local/{row_key} move shared"))
                .expect("move command should parse"),
            Some(ChecklistCommand::Edit {
                item: ItemSelector::Qualified {
                    association: ItemAssociationSelector::Local,
                    row_key: RowKey(row_key),
                },
                command: EditCommand::Move {
                    group: words(["shared"]),
                },
            })
        );
    }

    #[test]
    fn parses_status_tag_priority_and_observation_commands() {
        let first = ItemSelector::ListIndex(NonZeroUsize::new(1).unwrap());

        assert_eq!(
            parse_checklist_command("edit 1 note").expect("command should parse"),
            Some(ChecklistCommand::Edit {
                item: first.clone(),
                command: EditCommand::Note,
            })
        );
        assert_eq!(
            parse_checklist_command("tag add 1 sillytag").expect("command should parse"),
            Some(ChecklistCommand::Tag {
                command: TagCommand::Add {
                    item: first.clone(),
                    tag: "sillytag".to_owned(),
                },
            })
        );
        assert_eq!(
            parse_checklist_command("tag rm 1 sillytag").expect("command should parse"),
            Some(ChecklistCommand::Tag {
                command: TagCommand::Rm {
                    item: first.clone(),
                    tag: "sillytag".to_owned(),
                },
            })
        );
        let claim = parse_checklist_command("claim 1")
            .expect("command should parse")
            .expect("line is not empty");
        assert_eq!(
            claim,
            ChecklistCommand::Claim {
                item: first.clone()
            }
        );
        assert_eq!(claim.status_target(), Some(ChecklistStatus::InProgress));

        let complete = parse_checklist_command("complete 1")
            .expect("command should parse")
            .expect("line is not empty");
        assert_eq!(
            complete,
            ChecklistCommand::Complete {
                item: first.clone()
            }
        );
        assert_eq!(complete.status_target(), Some(ChecklistStatus::Done));

        assert_eq!(
            parse_checklist_command("priority 1 255").expect("command should parse"),
            Some(ChecklistCommand::Priority {
                item: first.clone(),
                priority: 255,
            })
        );
        assert_eq!(
            parse_checklist_command("events 20").expect("command should parse"),
            Some(ChecklistCommand::Events { limit: Some(20) })
        );
        assert_eq!(
            parse_checklist_command("events").expect("command should parse"),
            Some(ChecklistCommand::Events { limit: None })
        );
        assert_eq!(
            parse_checklist_command("list").expect("command should parse"),
            Some(ChecklistCommand::List)
        );
        assert_eq!(
            parse_checklist_command("show 1").expect("command should parse"),
            Some(ChecklistCommand::Show {
                item: first.clone()
            })
        );
        assert_eq!(
            parse_checklist_command("delete 1").expect("command should parse"),
            Some(ChecklistCommand::Delete { item: first })
        );
        assert_eq!(
            parse_checklist_command("members").expect("command should parse"),
            Some(ChecklistCommand::Members)
        );
        assert_eq!(
            parse_checklist_command("check").expect("command should parse"),
            Some(ChecklistCommand::Check)
        );
        assert_eq!(
            parse_checklist_command("me").expect("command should parse"),
            Some(ChecklistCommand::Me)
        );
        assert_eq!(
            parse_checklist_command("sync").expect("command should parse"),
            Some(ChecklistCommand::Sync)
        );
        assert_eq!(
            parse_checklist_command("help").expect("command should parse"),
            Some(ChecklistCommand::Help)
        );
        assert_eq!(
            parse_checklist_command("exit").expect("command should parse"),
            Some(ChecklistCommand::Quit)
        );
    }

    #[test]
    fn parses_peer_diagnostics_command() {
        assert_eq!(
            parse_checklist_command("peers").expect("command should parse"),
            Some(ChecklistCommand::Peers)
        );
    }

    #[test]
    fn empty_repl_line_has_no_command() {
        assert_eq!(
            parse_checklist_command("").expect("empty line should parse"),
            None
        );
    }

    #[test]
    fn parses_group_commands() {
        assert_eq!(
            parse_checklist_command("group list").expect("command should parse"),
            Some(ChecklistCommand::Group {
                command: ChecklistGroupCommand::List,
            })
        );
        assert_eq!(
            parse_checklist_command("group create shared errands").expect("command should parse"),
            Some(ChecklistCommand::Group {
                command: ChecklistGroupCommand::Create {
                    name: words(["shared", "errands"]),
                },
            })
        );
        assert_eq!(
            parse_checklist_command("group invitations").expect("command should parse"),
            Some(ChecklistCommand::Group {
                command: ChecklistGroupCommand::Invitations,
            })
        );
        assert_eq!(
            parse_checklist_command("group accept 2").expect("command should parse"),
            Some(ChecklistCommand::Group {
                command: ChecklistGroupCommand::Accept {
                    invitation: NonZeroUsize::new(2).expect("two is non-zero"),
                },
            })
        );
        assert_eq!(
            parse_checklist_command("group reject 1").expect("command should parse"),
            Some(ChecklistCommand::Group {
                command: ChecklistGroupCommand::Reject {
                    invitation: NonZeroUsize::new(1).expect("one is non-zero"),
                },
            })
        );
        assert_eq!(
            parse_checklist_command("group default shared errands").expect("command should parse"),
            Some(ChecklistCommand::Group {
                command: ChecklistGroupCommand::Default {
                    group: words(["shared", "errands"]),
                },
            })
        );
        assert_eq!(
            parse_checklist_command("group clear-default").expect("command should parse"),
            Some(ChecklistCommand::Group {
                command: ChecklistGroupCommand::ClearDefault,
            })
        );
    }

    #[test]
    fn parses_runtime_key_commands() {
        assert_eq!(
            parse_checklist_command("keys export-local").expect("command should parse"),
            Some(ChecklistCommand::Keys {
                command: ChecklistKeyCommand::ExportLocal,
            })
        );
        assert_eq!(
            parse_checklist_command("keys inspect bundle-text").expect("command should parse"),
            Some(ChecklistCommand::Keys {
                command: ChecklistKeyCommand::Inspect {
                    public_bundle: "bundle-text".to_owned(),
                },
            })
        );
        assert_eq!(
            parse_checklist_command("keys trust bob bundle-text").expect("command should parse"),
            Some(ChecklistCommand::Keys {
                command: ChecklistKeyCommand::Trust {
                    member_id: flotsync_core::MemberIdentity::from_array(["bob"]),
                    public_bundle: "bundle-text".to_owned(),
                },
            })
        );
        assert_eq!(
            parse_checklist_command("keys block bundle-text").expect("command should parse"),
            Some(ChecklistCommand::Keys {
                command: ChecklistKeyCommand::Block {
                    public_bundle: "bundle-text".to_owned(),
                },
            })
        );
    }

    #[test]
    fn rejects_undo_style_status_commands() {
        assert_command_error_contains("undone 1", "unrecognized subcommand");
        assert_command_error_contains("status 1 open", "unrecognized subcommand");
    }

    #[test]
    fn rejects_invalid_item_references_and_values() {
        assert!(matches!(
            parse_item_selector("0"),
            Err(ChecklistCommandParseError::InvalidItemReference { value })
                if value == "0"
        ));
        assert!(matches!(
            parse_item_selector("not-a-uuid"),
            Err(ChecklistCommandParseError::InvalidItemReference { value })
                if value == "not-a-uuid"
        ));
        assert_command_error_contains("claim 0", "positive list position");
        assert_command_error_contains("complete not-a-uuid", "positive list position");
        assert_command_error_contains("priority 1 256", "invalid value");
        assert_command_error_contains("events no", "invalid digit");
    }

    #[test]
    fn generated_help_mentions_the_repl_commands() {
        let help = checklist_help();

        for command in [
            "add",
            "rename",
            "edit",
            "tag",
            "claim",
            "complete",
            "priority",
            "delete",
            "list",
            "show",
            "events",
            "sync",
            "members",
            "check",
            "peers",
            "group",
            "keys",
            "me",
            "help",
            "quit",
            "Add one new checklist item",
            "Publish local changes and apply received updates",
            "Print active group members",
            "Ask each active group member for its current group summary",
            "Manage public identity keys through the running replication runtime",
            "Exit the REPL",
        ] {
            assert!(
                help.contains(command),
                "generated help should contain {command:?}: {help}"
            );
        }
    }

    #[test]
    fn snapshot_reload_rejects_unrequested_deleted_rows() {
        let mut checklist = test_working_set();
        let row_id = test_row_id(RowKey(Uuid::from_u128(61)));
        let row_values = RowValues::try_from_fields(
            &CHECKLIST_SCHEMA,
            HashMap::from([
                (FIELD_TEXT.to_owned(), String::new().into()),
                (FIELD_NOTE.to_owned(), String::new().into()),
                (FIELD_TAGS.to_owned(), Vec::<String>::new().into()),
                (
                    FIELD_STATUS.to_owned(),
                    ChecklistStatus::Open.as_str().into(),
                ),
                (FIELD_PRIORITY.to_owned(), 0u8.into()),
                (FIELD_EDIT_COUNT.to_owned(), 0u64.into()),
            ]),
        )
        .expect("test snapshot row should match checklist schema");
        let mut value_data = InMemoryValueData::new(CHECKLIST_SCHEMA.clone());
        value_data
            .push_row(row_id.clone(), true, &row_values)
            .expect("test snapshot row should insert");

        let result = checklist.apply_snapshot_rows(value_data.rows());

        assert!(matches!(
            result,
            Err(ChecklistWorkingSetError::UnexpectedDeletedSnapshotRow { row_id: actual })
                if actual == row_id
        ));
    }

    #[test]
    fn working_set_group_insert_edits_prepare_one_mutation_without_queueing_events() {
        let row_key = RowKey(Uuid::from_u128(1));
        let item_id = test_item_id(row_key);
        let mut checklist = test_working_set();

        checklist.add_item_with_id(item_id, "buy milk");
        checklist
            .rename_item(item_id, "buy oat milk")
            .expect("rename should apply");
        checklist
            .add_tag(item_id, "errand")
            .expect("tag should apply");
        checklist.claim_item(item_id).expect("claim should apply");

        assert_eq!(checklist.dirty_row_count(), 1);
        assert_eq!(checklist.queued_event_count(), 0);
        assert_eq!(
            checklist
                .item(item_id)
                .expect("row should exist")
                .edit_count,
            4
        );

        let plan = checklist
            .prepare_group_sync(test_group_id())
            .expect("plan should build")
            .expect("dirty row should produce a sync plan");
        assert_eq!(plan.mutations.len(), 1);
        assert_eq!(plan.item_ids().count(), 1);
        assert!(
            plan.item_ids()
                .any(|dirty_item_id| dirty_item_id == item_id)
        );
        assert!(matches!(
            &plan.mutations[0],
            RowMutation::Upsert { row_id, .. }
                if row_id.group_id == test_group_id()
                    && row_id.dataset_id == *checklist.dataset_id()
                    && row_id.row_key == row_key
        ));
    }

    #[test]
    fn working_set_listener_events_queue_until_explicit_drain() {
        let row_key = RowKey(Uuid::from_u128(2));
        let item_id = test_item_id(row_key);
        let mut checklist = test_working_set();

        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: test_item("remote item", 7),
            }])
            .expect("clean listener change should enqueue");

        assert!(checklist.item(item_id).is_none());
        assert_eq!(checklist.queued_event_count(), 1);
        assert_eq!(checklist.events().len(), 1);

        assert_eq!(checklist.drain_queued_events(), 1);
        assert_eq!(
            checklist.item(item_id).expect("row should apply").text,
            "remote item"
        );
        assert_eq!(checklist.queued_event_count(), 0);
    }

    #[test]
    fn working_set_tags_have_deterministic_display_order() {
        let row_key = RowKey(Uuid::from_u128(6));
        let item_id = test_item_id(row_key);
        let mut checklist = test_working_set();

        checklist.add_item_with_id(item_id, "tagged");
        checklist
            .add_tag(item_id, "zeta")
            .expect("tag should apply");
        checklist
            .add_tag(item_id, "alpha")
            .expect("tag should apply");
        checklist
            .add_tag(item_id, "zeta")
            .expect("duplicate tag should be a no-op");

        let item = checklist.item(item_id).expect("row should exist");
        assert_eq!(
            item.tags.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
        assert_eq!(item.edit_count, 3);
    }

    #[test]
    fn dirty_update_publishes_only_local_field_diff_so_remote_fields_survive_sync() {
        let row_key = RowKey(Uuid::from_u128(8));
        let item_id = test_item_id(row_key);
        let mut checklist = test_working_set();
        let base = test_item("base", 1);
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: base.clone(),
            }])
            .expect("initial listener change should enqueue");
        checklist.drain_queued_events();

        checklist
            .set_priority(item_id, 7)
            .expect("priority edit should apply");

        let plan = checklist
            .prepare_group_sync(test_group_id())
            .expect("plan should build")
            .expect("dirty update should produce a sync plan");
        assert_eq!(plan.mutations.len(), 1);
        let RowMutation::Upsert { row, .. } = &plan.mutations[0] else {
            panic!("dirty update should publish an upsert");
        };
        let changed_fields = row
            .fields
            .keys()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        assert_eq!(
            changed_fields,
            HashSet::from([FIELD_PRIORITY, FIELD_EDIT_COUNT])
        );

        let mut remote = base;
        remote.text = "remote title".to_owned();
        remote.increment_edit_count();
        let mut merged = remote.clone();
        merged.priority = 7;
        checklist.finish_successful_group_sync(Some(plan));
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: remote,
            }])
            .expect("clean remote listener change should enqueue");
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: merged,
            }])
            .expect("local listener echo should enqueue after successful sync");

        assert_eq!(checklist.drain_queued_events(), 2);
        let item = checklist.item(item_id).expect("row should remain visible");
        assert_eq!(item.text, "remote title");
        assert_eq!(item.priority, 7);
        assert_eq!(checklist.dirty_row_count(), 0);
    }

    #[test]
    fn successful_sync_clears_dirty_rows_before_applying_listener_events_in_order() {
        let row_key = RowKey(Uuid::from_u128(3));
        let item_id = test_item_id(row_key);
        let mut checklist = test_working_set();
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: test_item("base", 1),
            }])
            .expect("initial listener change should enqueue");
        checklist.drain_queued_events();
        checklist
            .rename_item(item_id, "local")
            .expect("local rename should apply");

        let plan = checklist
            .prepare_group_sync(test_group_id())
            .expect("plan should build")
            .expect("dirty row should produce a sync plan");
        checklist.finish_successful_group_sync(Some(plan));
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: test_item("remote before echo", 2),
            }])
            .expect("remote listener change should enqueue after successful sync");
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: test_item("local", 3),
            }])
            .expect("local listener echo should enqueue after successful sync");

        assert_eq!(checklist.drain_queued_events(), 2);

        let item = checklist.item(item_id).expect("row should remain visible");
        assert_eq!(item.text, "local");
        assert_eq!(item.edit_count, 3);
        assert_eq!(checklist.dirty_row_count(), 0);
        assert_eq!(checklist.queued_event_count(), 0);
    }

    #[test]
    fn incoming_change_for_dirty_row_is_rejected_atomically() {
        let row_key = RowKey(Uuid::from_u128(4));
        let item_id = test_item_id(row_key);
        let clean_item_id = test_item_id(RowKey(Uuid::from_u128(5)));
        let mut checklist = test_working_set();
        checklist.add_item_with_id(item_id, "local");
        let error = checklist
            .enqueue_checklist_changes(vec![
                ChecklistRowChange::Upsert {
                    item_id: clean_item_id,
                    item: test_item("clean remote", 1),
                },
                ChecklistRowChange::Upsert {
                    item_id,
                    item: test_item("conflicting remote", 1),
                },
            ])
            .expect_err("dirty listener change should be rejected");

        let plan = checklist
            .prepare_group_sync(test_group_id())
            .expect("plan should build")
            .expect("dirty row should produce a sync plan");

        assert_eq!(plan.mutations.len(), 1);
        assert!(matches!(
            error,
            ChecklistWorkingSetError::IncomingChangeForDirtyItem {
                item_id: conflicting_item_id
            } if conflicting_item_id == item_id
        ));
        assert_eq!(checklist.dirty_row_count(), 1);
        assert_eq!(checklist.queued_event_count(), 0);
        assert!(checklist.events().is_empty());
        assert!(checklist.item(clean_item_id).is_none());
        assert!(matches!(
            checklist.read_token(),
            Err(ChecklistWorkingSetError::MissingReadToken)
        ));
        assert_eq!(
            checklist.item(item_id).expect("row should exist").text,
            "local"
        );
    }

    #[test]
    fn deleting_new_dirty_row_suppresses_publish() {
        let row_key = RowKey(Uuid::from_u128(5));
        let item_id = test_item_id(row_key);
        let mut checklist = test_working_set();

        checklist.add_item_with_id(item_id, "transient");
        checklist.delete_item(item_id).expect("delete should apply");

        assert!(checklist.item(item_id).is_none());
        assert_eq!(checklist.dirty_row_count(), 0);
        assert!(
            checklist
                .prepare_group_sync(test_group_id())
                .expect("plan should build")
                .is_none()
        );
    }

    #[test]
    fn deleting_existing_row_prepares_delete_mutation() {
        let row_key = RowKey(Uuid::from_u128(7));
        let item_id = test_item_id(row_key);
        let mut checklist = test_working_set();

        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id,
                item: test_item("remote", 1),
            }])
            .expect("initial listener change should enqueue");
        checklist.drain_queued_events();
        checklist.delete_item(item_id).expect("delete should apply");

        let plan = checklist
            .prepare_group_sync(test_group_id())
            .expect("plan should build")
            .expect("dirty delete should produce a sync plan");

        assert_eq!(plan.mutations.len(), 1);
        assert!(matches!(
            &plan.mutations[0],
            RowMutation::Delete { row_id }
                if row_id.group_id == test_group_id()
                    && row_id.dataset_id == *checklist.dataset_id()
                    && row_id.row_key == row_key
        ));
    }

    #[test]
    fn copy_preserves_uuid_and_complete_contents_without_overwriting_collisions() {
        let source_group = test_group_id();
        let target_group = GroupId(Uuid::from_u128(21));
        let row_key = RowKey(Uuid::from_u128(22));
        let source_id = ChecklistItemId::group(source_group, row_key);
        let target_id = ChecklistItemId::group(target_group, row_key);
        let mut source = test_item("source", 4);
        source.note = "complete note".to_owned();
        source.tags.insert("copied".to_owned());
        source.priority = 9;
        let mut checklist = test_working_set();
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id: source_id,
                item: source.clone(),
            }])
            .expect("source listener change should enqueue");
        checklist.drain_queued_events();

        assert_eq!(
            checklist
                .copy_item(source_id, ChecklistItemAssociation::Group(target_group))
                .expect("copy should stage"),
            target_id
        );
        assert_eq!(checklist.item(target_id), Some(&source));
        assert_eq!(
            checklist
                .copy_item(source_id, ChecklistItemAssociation::Group(target_group))
                .expect("identical retry should be idempotent"),
            target_id
        );
        let plan = checklist
            .prepare_group_sync(target_group)
            .expect("target plan should build")
            .expect("target should be dirty");
        assert_eq!(plan.mutations.len(), 1);
        let RowMutation::Upsert { row_id, row } = &plan.mutations[0] else {
            panic!("copy should produce one target upsert");
        };
        assert_eq!(row_id.row_key, row_key);
        assert_eq!(row.fields.len(), CHECKLIST_SCHEMA.columns.len());

        let divergent_group = GroupId(Uuid::from_u128(23));
        let divergent_id = ChecklistItemId::group(divergent_group, row_key);
        checklist.add_item_with_id(divergent_id, "different");
        assert!(matches!(
            checklist.copy_item(source_id, ChecklistItemAssociation::Group(divergent_group)),
            Err(ChecklistWorkingSetError::TransferTargetCollision { target_id })
                if target_id == divergent_id
        ));
        assert!(matches!(
            checklist.copy_item(source_id, ChecklistItemAssociation::Group(source_group)),
            Err(ChecklistWorkingSetError::SameTransferAssociation {
                source_id: actual_source,
                target_association: ChecklistItemAssociation::Group(actual_group),
            }) if actual_source == source_id && actual_group == source_group
        ));
    }

    #[test]
    fn move_stages_target_upsert_and_existing_source_tombstone_in_one_pass() {
        let source_group = test_group_id();
        let target_group = GroupId(Uuid::from_u128(24));
        let row_key = RowKey(Uuid::from_u128(25));
        let source_id = ChecklistItemId::group(source_group, row_key);
        let target_id = ChecklistItemId::group(target_group, row_key);
        let mut checklist = test_working_set();
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id: source_id,
                item: test_item("base", 1),
            }])
            .expect("source listener change should enqueue");
        checklist.drain_queued_events();
        checklist
            .rename_item(source_id, "latest source")
            .expect("dirty source edit should apply");

        assert_eq!(
            checklist
                .move_item(source_id, ChecklistItemAssociation::Group(target_group))
                .expect("move should stage"),
            target_id
        );
        assert!(checklist.item(source_id).is_none());
        assert_eq!(
            checklist
                .item(target_id)
                .expect("target should remain visible")
                .text,
            "latest source"
        );
        let source_plan = checklist
            .prepare_group_sync(source_group)
            .expect("source plan should build")
            .expect("existing source should stage a tombstone");
        assert!(matches!(
            source_plan.mutations.as_slice(),
            [RowMutation::Delete { row_id }] if row_id.row_key == row_key
        ));
        let target_plan = checklist
            .prepare_group_sync(target_group)
            .expect("target plan should build")
            .expect("target should stage an upsert");
        assert!(matches!(
            target_plan.mutations.as_slice(),
            [RowMutation::Upsert { row_id, .. }] if row_id.row_key == row_key
        ));
    }

    #[test]
    fn move_removes_never_published_and_local_sources_without_tombstones() {
        let source_group = test_group_id();
        let target_group = GroupId(Uuid::from_u128(26));
        let local_target_group = GroupId(Uuid::from_u128(27));
        let final_group = GroupId(Uuid::from_u128(30));
        let group_row = RowKey(Uuid::from_u128(28));
        let local_row = RowKey(Uuid::from_u128(29));
        let group_source = ChecklistItemId::group(source_group, group_row);
        let local_source = ChecklistItemId::local(local_row);
        let mut checklist = test_working_set();
        checklist.add_item_with_id(group_source, "new group item");
        checklist.add_item_with_id(local_source, "local item");

        let intermediate_id = checklist
            .move_item(group_source, ChecklistItemAssociation::Group(target_group))
            .expect("new group source should move");
        checklist
            .move_item(
                intermediate_id,
                ChecklistItemAssociation::Group(final_group),
            )
            .expect("never-published target should support a chained move");
        checklist
            .move_item(
                local_source,
                ChecklistItemAssociation::Group(local_target_group),
            )
            .expect("local source should move");

        assert!(
            checklist
                .prepare_group_sync(source_group)
                .expect("source plan should build")
                .is_none()
        );
        assert_eq!(checklist.dirty_local_item_count(), 0);
        assert_eq!(
            checklist.dirty_group_ids(),
            HashSet::from([final_group, local_target_group])
        );
        assert!(checklist.item(group_source).is_none());
        assert!(checklist.item(intermediate_id).is_none());
        assert!(checklist.item(local_source).is_none());
    }

    #[test]
    fn composite_item_identity_keeps_duplicate_row_uuids_separately_addressable() {
        let row_key = RowKey(Uuid::from_u128(80));
        let first_group = test_group_id();
        let second_group = GroupId(Uuid::from_u128(11));
        let local_id = ChecklistItemId::local(row_key);
        let first_group_id = ChecklistItemId::group(first_group, row_key);
        let second_group_id = ChecklistItemId::group(second_group, row_key);
        let mut checklist = test_working_set();

        checklist.add_item_with_id(local_id, "local");
        checklist.add_item_with_id(first_group_id, "first group");
        checklist.add_item_with_id(second_group_id, "second group");

        assert_eq!(
            checklist.item_ids_with_row_key(row_key),
            vec![local_id, first_group_id, second_group_id]
        );

        checklist
            .rename_item(first_group_id, "updated first group")
            .expect("global list index should resolve exactly");
        assert_eq!(
            checklist
                .item(first_group_id)
                .expect("item should exist")
                .text,
            "updated first group"
        );
        assert_eq!(
            checklist.item(local_id).expect("item should exist").text,
            "local"
        );
        assert_eq!(
            checklist
                .item(second_group_id)
                .expect("item should exist")
                .text,
            "second group"
        );
    }

    #[test]
    fn group_sync_partitions_dirty_items_and_leaves_local_items_unsynchronised() {
        let first_group = test_group_id();
        let second_group = GroupId(Uuid::from_u128(12));
        let local_id = ChecklistItemId::local(RowKey(Uuid::from_u128(81)));
        let first_group_id = ChecklistItemId::group(first_group, RowKey(Uuid::from_u128(82)));
        let second_group_id = ChecklistItemId::group(second_group, RowKey(Uuid::from_u128(83)));
        let mut checklist = test_working_set();

        checklist.add_item_with_id(local_id, "local");
        checklist.add_item_with_id(first_group_id, "first group");
        checklist.add_item_with_id(second_group_id, "second group");

        let first_plan = checklist
            .prepare_group_sync(first_group)
            .expect("first group plan should build")
            .expect("first group should be dirty");
        assert_eq!(first_plan.item_ids().collect::<Vec<_>>(), [first_group_id]);
        assert!(
            checklist
                .prepare_group_sync(GroupId(Uuid::from_u128(999)))
                .expect("unknown clean group plan should build")
                .is_none()
        );

        checklist.finish_successful_group_sync(Some(first_plan));
        assert_eq!(checklist.dirty_local_item_count(), 1);
        assert_eq!(checklist.dirty_group_ids(), HashSet::from([second_group]));
        assert_eq!(checklist.dirty_row_count(), 2);
    }

    #[test]
    fn syncing_one_group_rejects_events_for_another_dirty_group() {
        let synced_group = test_group_id();
        let dirty_group = GroupId(Uuid::from_u128(13));
        let synced_id = ChecklistItemId::group(synced_group, RowKey(Uuid::from_u128(84)));
        let dirty_id = ChecklistItemId::group(dirty_group, RowKey(Uuid::from_u128(85)));
        let mut checklist = test_working_set();
        checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id: dirty_id,
                item: test_item("base", 1),
            }])
            .expect("initial listener change should enqueue");
        checklist.drain_queued_events();
        checklist
            .rename_item(dirty_id, "local title")
            .expect("other-group edit should apply");
        checklist.add_item_with_id(synced_id, "publish me");

        let plan = checklist
            .prepare_group_sync(synced_group)
            .expect("sync plan should build")
            .expect("selected group should be dirty");
        checklist.finish_successful_group_sync(Some(plan));

        let mut remote = test_item("base", 2);
        remote.note = "remote note".to_owned();
        let error = checklist
            .enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
                item_id: dirty_id,
                item: remote,
            }])
            .expect_err("other dirty group listener change should be rejected");

        assert!(matches!(
            error,
            ChecklistWorkingSetError::IncomingChangeForDirtyItem {
                item_id: conflicting_item_id
            } if conflicting_item_id == dirty_id
        ));
        let dirty_item = checklist.item(dirty_id).expect("dirty item should remain");
        assert_eq!(dirty_item.text, "local title");
        assert!(dirty_item.note.is_empty());
        assert_eq!(dirty_item.edit_count, 2);
        assert_eq!(checklist.queued_event_count(), 0);
        assert_eq!(checklist.events().len(), 1);
        assert_eq!(checklist.dirty_group_ids(), HashSet::from([dirty_group]));
    }

    fn assert_command_error_contains(line: &str, expected: &str) {
        let error = parse_checklist_command(line).expect_err("command should fail");
        let message = error.to_string();
        assert!(
            message.contains(expected),
            "expected error for {line:?} to contain {expected:?}, got {message:?}"
        );
    }

    fn words<const N: usize>(values: [&str; N]) -> Vec<String> {
        values.into_iter().map(str::to_owned).collect()
    }

    fn test_working_set() -> ChecklistWorkingSet {
        ChecklistWorkingSet::new()
    }

    fn test_group_id() -> GroupId {
        GroupId(Uuid::from_u128(10))
    }

    fn test_item_id(row_key: RowKey) -> ChecklistItemId {
        ChecklistItemId::group(test_group_id(), row_key)
    }

    fn test_row_id(row_key: RowKey) -> RowId {
        RowId {
            group_id: test_group_id(),
            dataset_id: checklist_dataset_id(),
            row_key,
        }
    }

    fn test_item(text: &str, edit_count: u64) -> ChecklistItem {
        ChecklistItem {
            text: text.to_owned(),
            note: String::new(),
            tags: BTreeSet::new(),
            status: ChecklistStatus::Open,
            priority: 0,
            edit_count,
        }
    }
}

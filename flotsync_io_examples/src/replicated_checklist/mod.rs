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
    DatasetId::try_new(CHECKLIST_DATASET_ID)
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
        /// Item list index or row UUID.
        item: ItemSelector,
        /// Replacement item text.
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        text: Vec<String>,
    },
    /// Edit longer item fields.
    Edit {
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
        /// Item list index or row UUID.
        item: ItemSelector,
    },
    /// Mark an item as done.
    Complete {
        /// Item list index or row UUID.
        item: ItemSelector,
    },
    /// Set an item's priority.
    Priority {
        /// Item list index or row UUID.
        item: ItemSelector,
        /// Priority value to store on the item.
        priority: u8,
    },
    /// Delete an item.
    Delete {
        /// Item list index or row UUID.
        item: ItemSelector,
    },
    /// Print visible checklist items.
    List,
    /// Print all fields for one item.
    Show {
        /// Item list index or row UUID.
        item: ItemSelector,
    },
    /// Print queued replication events.
    Events {
        /// Maximum number of latest events to print.
        limit: Option<usize>,
    },
    /// Publish local changes and apply received updates.
    Sync,
    /// Print configured group members.
    Members,
    /// Ask each configured member for its current group summary.
    Check,
    /// Print local member and store details.
    Me,
    /// Print command help.
    Help,
    /// Exit the REPL.
    #[command(alias = "exit")]
    Quit,
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
    Note {
        /// Item list index or row UUID.
        item: ItemSelector,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub enum TagCommand {
    /// Add one tag to an item.
    Add {
        /// Item list index or row UUID.
        item: ItemSelector,
        /// Tag text to add.
        tag: String,
    },
    /// Remove one tag from an item.
    Rm {
        /// Item list index or row UUID.
        item: ItemSelector,
        /// Tag text to remove.
        tag: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ItemSelector {
    ListIndex(NonZeroUsize),
    RowKey(RowKey),
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
    #[snafu(display("Item reference '{value}' is not a positive list index or full item UUID."))]
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
        row_key: RowKey,
        item: ChecklistItem,
    },
    /// A listener-visible row deletion for this checklist dataset.
    Delete { row_key: RowKey },
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
    row_keys: HashSet<RowKey>,
    /// Row mutations to pass to `publish_changes`.
    pub mutations: Vec<RowMutation>,
}

impl ChecklistSyncPlan {
    pub fn row_keys(&self) -> impl Iterator<Item = RowKey> + '_ {
        self.row_keys.iter().copied()
    }
}

/// Checklist item paired with its transient one-based display index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedChecklistItem<'a> {
    /// One-based list position accepted by REPL commands.
    pub index: NonZeroUsize,
    /// Stable replicated row key for this item.
    pub row_key: RowKey,
    /// Checklist item currently visible in the working set.
    pub item: &'a ChecklistItem,
}

/// Errors raised while decoding rows or mutating the checklist working set.
#[derive(Debug, Snafu)]
pub enum ChecklistWorkingSetError {
    #[snafu(display("Checklist item reference {selector:?} does not resolve to a visible row."))]
    UnknownItem { selector: ItemSelector },
    #[snafu(display(
        "Replicated row {row} does not belong to checklist group {group} dataset {dataset}."
    ))]
    UnexpectedRowScope {
        row: RowId,
        group: GroupId,
        dataset: DatasetId,
    },
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
    #[snafu(display("Dirty checklist row {row_key} is missing from the working set."))]
    MissingDirtyRow { row_key: RowKey },
    #[snafu(display("Checklist working set does not have a replication read token."))]
    MissingReadToken,
}

/// In-memory REPL view of checklist rows between explicit `sync` commands.
///
/// Local commands update this working set and remember the original row state
/// for dirty rows. Listener changes are queued here and only applied when
/// `sync` drains the queue after any local dirty rows have been published.
pub struct ChecklistWorkingSet {
    group_id: GroupId,
    dataset_id: DatasetId,
    rows: HashMap<RowKey, ChecklistItem>,
    display_order: Vec<RowKey>,
    dirty_rows: HashMap<RowKey, DirtyRowKind>,
    queued_events: VecDeque<ChecklistEvent>,
    event_history: Vec<ChecklistEvent>,
    read_token: Option<ReadToken>,
}

impl ChecklistWorkingSet {
    #[must_use]
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
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
    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    #[must_use]
    pub fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    #[must_use]
    pub fn item(&self, row_key: RowKey) -> Option<&ChecklistItem> {
        self.rows.get(&row_key)
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn selected_item(
        &self,
        selector: ItemSelector,
    ) -> Result<ListedChecklistItem<'_>, ChecklistWorkingSetError> {
        let row_key = self.resolve_selector(selector)?;
        self.listed_items()
            .into_iter()
            .find(|listed| listed.row_key == row_key)
            .ok_or(ChecklistWorkingSetError::UnknownItem { selector })
    }

    #[must_use]
    pub fn dirty_row_count(&self) -> usize {
        self.dirty_rows.len()
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
            .filter_map(|row_key| self.rows.get(row_key).map(|item| (*row_key, item)))
            .enumerate()
            .map(|(index, (row_key, item))| ListedChecklistItem {
                index: NonZeroUsize::new(index + 1)
                    .expect("enumerated list positions start at one"),
                row_key,
                item,
            })
            .collect()
    }

    pub fn add_item(&mut self, text: impl Into<String>) -> RowKey {
        let row_key = RowKey(Uuid::new_v4());
        self.add_item_with_key(row_key, text);
        row_key
    }

    pub fn add_item_with_key(&mut self, row_key: RowKey, text: impl Into<String>) {
        self.rows.insert(row_key, ChecklistItem::new(text));
        push_display_row(&mut self.display_order, row_key);
        self.dirty_rows.insert(row_key, DirtyRowKind::Insert);
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn rename_item(
        &mut self,
        selector: ItemSelector,
        text: impl Into<String>,
    ) -> Result<(), ChecklistWorkingSetError> {
        let text = text.into();
        self.modify_item(selector, |item| {
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
        selector: ItemSelector,
        note: impl Into<String>,
    ) -> Result<(), ChecklistWorkingSetError> {
        let note = note.into();
        self.modify_item(selector, |item| {
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
        selector: ItemSelector,
        tag: impl Into<String>,
    ) -> Result<(), ChecklistWorkingSetError> {
        let tag = tag.into();
        self.modify_item(selector, |item| {
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
        selector: ItemSelector,
        tag: &str,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.modify_item(selector, |item| {
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
    pub fn claim_item(&mut self, selector: ItemSelector) -> Result<(), ChecklistWorkingSetError> {
        self.advance_status(selector, ChecklistStatus::InProgress)
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn complete_item(
        &mut self,
        selector: ItemSelector,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.advance_status(selector, ChecklistStatus::Done)
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn set_priority(
        &mut self,
        selector: ItemSelector,
        priority: u8,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.modify_item(selector, |item| {
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
    pub fn delete_item(&mut self, selector: ItemSelector) -> Result<(), ChecklistWorkingSetError> {
        let row_key = self.resolve_selector(selector)?;
        self.rows.remove(&row_key);
        self.display_order.retain(|candidate| *candidate != row_key);
        match self.dirty_rows.remove(&row_key) {
            Some(DirtyRowKind::Insert) => {}
            _ => {
                self.dirty_rows.insert(row_key, DirtyRowKind::Delete);
            }
        }
        Ok(())
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
        self.enqueue_checklist_changes(checklist_changes);
        Ok(())
    }

    pub fn enqueue_checklist_changes(&mut self, changes: Vec<ChecklistRowChange>) {
        if changes.is_empty() {
            return;
        }
        let event = ChecklistEvent {
            timestamp: SystemTime::now(),
            changes,
        };
        self.event_history.push(event.clone());
        self.queued_events.push_back(event);
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn apply_snapshot_rows<'a, I>(&mut self, rows: I) -> Result<(), ChecklistWorkingSetError>
    where
        I: IntoIterator<Item = SnapshotValueRow<'a>>,
    {
        for row in rows {
            let change = self.checklist_change_from_snapshot_row(row)?;
            self.apply_checklist_change(change);
        }
        Ok(())
    }

    /// # Errors
    ///
    /// See `ChecklistWorkingSetError` for failure conditions.
    pub fn prepare_sync(&self) -> Result<Option<ChecklistSyncPlan>, ChecklistWorkingSetError> {
        if self.dirty_rows.is_empty() {
            return Ok(None);
        }

        let mut row_keys = HashSet::with_capacity(self.dirty_rows.len());
        let mut mutations = Vec::with_capacity(self.dirty_rows.len());
        for (&row_key, dirty_row) in &self.dirty_rows {
            row_keys.insert(row_key);
            match dirty_row {
                DirtyRowKind::Insert => {
                    let item = self
                        .rows
                        .get(&row_key)
                        .ok_or(ChecklistWorkingSetError::MissingDirtyRow { row_key })?;
                    mutations.push(RowMutation::Upsert {
                        row_id: self.row_id(row_key),
                        row: item.to_row_values_patch(),
                    });
                }
                DirtyRowKind::Update { original } => {
                    let item = self
                        .rows
                        .get(&row_key)
                        .ok_or(ChecklistWorkingSetError::MissingDirtyRow { row_key })?;
                    mutations.push(RowMutation::Upsert {
                        row_id: self.row_id(row_key),
                        row: item.changed_fields_since(original),
                    });
                }
                DirtyRowKind::Delete => {
                    mutations.push(RowMutation::Delete {
                        row_id: self.row_id(row_key),
                    });
                }
            }
        }

        Ok(Some(ChecklistSyncPlan {
            row_keys,
            mutations,
        }))
    }

    pub fn finish_successful_sync(&mut self, plan: Option<ChecklistSyncPlan>) -> usize {
        if let Some(plan) = plan {
            for row_key in plan.row_keys {
                self.dirty_rows.remove(&row_key);
            }
        }
        self.drain_queued_events()
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
        selector: ItemSelector,
        target: ChecklistStatus,
    ) -> Result<(), ChecklistWorkingSetError> {
        self.modify_item(selector, |item| {
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
        selector: ItemSelector,
        update: impl FnOnce(&mut ChecklistItem) -> bool,
    ) -> Result<(), ChecklistWorkingSetError> {
        let row_key = self.resolve_selector(selector)?;
        let original = self
            .rows
            .get(&row_key)
            .expect("resolved selector must point to a visible row")
            .clone();
        let item = self
            .rows
            .get_mut(&row_key)
            .expect("resolved selector must point to a visible row");
        if update(item) {
            self.mark_dirty(row_key, original);
        }
        Ok(())
    }

    fn mark_dirty(&mut self, row_key: RowKey, original: ChecklistItem) {
        self.dirty_rows
            .entry(row_key)
            .or_insert(DirtyRowKind::Update { original });
    }

    fn resolve_selector(&self, selector: ItemSelector) -> Result<RowKey, ChecklistWorkingSetError> {
        match selector {
            ItemSelector::RowKey(row_key) if self.rows.contains_key(&row_key) => Ok(row_key),
            ItemSelector::RowKey(_) => Err(ChecklistWorkingSetError::UnknownItem { selector }),
            ItemSelector::ListIndex(index) => self
                .listed_items()
                .get(index.get() - 1)
                .map(|listed| listed.row_key)
                .ok_or(ChecklistWorkingSetError::UnknownItem { selector }),
        }
    }

    fn row_id(&self, row_key: RowKey) -> RowId {
        RowId {
            group_id: self.group_id,
            dataset_id: self.dataset_id.clone(),
            row_key,
        }
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
                self.validate_row_scope(&row_id)?;
                Ok(ChecklistRowChange::Delete {
                    row_key: row_id.row_key,
                })
            }
        }
    }

    fn checklist_change_from_snapshot_row(
        &self,
        row: SnapshotValueRow<'_>,
    ) -> Result<ChecklistRowChange, ChecklistWorkingSetError> {
        let row_id = row.row_id().clone();
        if row.is_tombstoned() {
            self.validate_row_scope(&row_id)?;
            return Err(ChecklistWorkingSetError::UnexpectedDeletedSnapshotRow { row_id });
        }
        self.checklist_upsert_change_from_row(&row_id, &row)
    }

    fn checklist_upsert_change_from_row(
        &self,
        row_id: &RowId,
        row: &(impl RowValueRead + ?Sized),
    ) -> Result<ChecklistRowChange, ChecklistWorkingSetError> {
        self.validate_row_scope(row_id)?;
        let row_key = row_id.row_key;
        let item = ChecklistItem::from_row(row_key, row)?;
        Ok(ChecklistRowChange::Upsert { row_key, item })
    }

    fn validate_row_scope(&self, row_id: &RowId) -> Result<(), ChecklistWorkingSetError> {
        ensure!(
            row_id.group_id == self.group_id && row_id.dataset_id == self.dataset_id,
            UnexpectedRowScopeSnafu {
                row: row_id.clone(),
                group: self.group_id,
                dataset: self.dataset_id.clone(),
            }
        );
        Ok(())
    }

    fn apply_checklist_change(&mut self, change: ChecklistRowChange) {
        match change {
            ChecklistRowChange::Upsert { row_key, item } => {
                self.rows.insert(row_key, item);
                push_display_row(&mut self.display_order, row_key);
            }
            ChecklistRowChange::Delete { row_key } => {
                self.rows.remove(&row_key);
                self.display_order.retain(|candidate| *candidate != row_key);
            }
        }
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

fn push_display_row(display_order: &mut Vec<RowKey>, row_key: RowKey) {
    if display_order.iter().all(|candidate| *candidate != row_key) {
        display_order.push(row_key);
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
    fn parses_status_tag_priority_and_observation_commands() {
        let first = ItemSelector::ListIndex(NonZeroUsize::new(1).unwrap());

        assert_eq!(
            parse_checklist_command("").expect("empty line should parse"),
            None
        );
        assert_eq!(
            parse_checklist_command("edit note 1").expect("command should parse"),
            Some(ChecklistCommand::Edit {
                command: EditCommand::Note { item: first },
            })
        );
        assert_eq!(
            parse_checklist_command("tag add 1 sillytag").expect("command should parse"),
            Some(ChecklistCommand::Tag {
                command: TagCommand::Add {
                    item: first,
                    tag: "sillytag".to_owned(),
                },
            })
        );
        assert_eq!(
            parse_checklist_command("tag rm 1 sillytag").expect("command should parse"),
            Some(ChecklistCommand::Tag {
                command: TagCommand::Rm {
                    item: first,
                    tag: "sillytag".to_owned(),
                },
            })
        );
        let claim = parse_checklist_command("claim 1")
            .expect("command should parse")
            .expect("line is not empty");
        assert_eq!(claim, ChecklistCommand::Claim { item: first });
        assert_eq!(claim.status_target(), Some(ChecklistStatus::InProgress));

        let complete = parse_checklist_command("complete 1")
            .expect("command should parse")
            .expect("line is not empty");
        assert_eq!(complete, ChecklistCommand::Complete { item: first });
        assert_eq!(complete.status_target(), Some(ChecklistStatus::Done));

        assert_eq!(
            parse_checklist_command("priority 1 255").expect("command should parse"),
            Some(ChecklistCommand::Priority {
                item: first,
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
            Some(ChecklistCommand::Show { item: first })
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
        assert_command_error_contains("claim 0", "positive list index or full item UUID");
        assert_command_error_contains(
            "complete not-a-uuid",
            "positive list index or full item UUID",
        );
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
            "me",
            "help",
            "quit",
            "Add one new checklist item",
            "Publish local changes and apply received updates",
            "Print configured group members",
            "Ask each configured member for its current group summary",
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
        let row_id = checklist.row_id(RowKey(Uuid::from_u128(61)));
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
    fn working_set_local_insert_edits_prepare_one_mutation_without_queueing_events() {
        let row_key = RowKey(Uuid::from_u128(1));
        let mut checklist = test_working_set();

        checklist.add_item_with_key(row_key, "buy milk");
        checklist
            .rename_item(ItemSelector::RowKey(row_key), "buy oat milk")
            .expect("rename should apply");
        checklist
            .add_tag(ItemSelector::RowKey(row_key), "errand")
            .expect("tag should apply");
        checklist
            .claim_item(ItemSelector::RowKey(row_key))
            .expect("claim should apply");

        assert_eq!(checklist.dirty_row_count(), 1);
        assert_eq!(checklist.queued_event_count(), 0);
        assert_eq!(
            checklist
                .item(row_key)
                .expect("row should exist")
                .edit_count,
            4
        );

        let plan = checklist
            .prepare_sync()
            .expect("plan should build")
            .expect("dirty row should produce a sync plan");
        assert_eq!(plan.mutations.len(), 1);
        assert_eq!(plan.row_keys().count(), 1);
        assert!(
            plan.row_keys()
                .any(|dirty_row_key| dirty_row_key == row_key)
        );
        assert!(matches!(
            &plan.mutations[0],
            RowMutation::Upsert { row_id, .. }
                if row_id.group_id == checklist.group_id()
                    && row_id.dataset_id == *checklist.dataset_id()
                    && row_id.row_key == row_key
        ));
    }

    #[test]
    fn working_set_listener_events_queue_until_explicit_drain() {
        let row_key = RowKey(Uuid::from_u128(2));
        let mut checklist = test_working_set();

        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: test_item("remote item", 7),
        }]);

        assert!(checklist.item(row_key).is_none());
        assert_eq!(checklist.queued_event_count(), 1);
        assert_eq!(checklist.events().len(), 1);

        assert_eq!(checklist.drain_queued_events(), 1);
        assert_eq!(
            checklist.item(row_key).expect("row should apply").text,
            "remote item"
        );
        assert_eq!(checklist.queued_event_count(), 0);
    }

    #[test]
    fn working_set_tags_have_deterministic_display_order() {
        let row_key = RowKey(Uuid::from_u128(6));
        let mut checklist = test_working_set();

        checklist.add_item_with_key(row_key, "tagged");
        checklist
            .add_tag(ItemSelector::RowKey(row_key), "zeta")
            .expect("tag should apply");
        checklist
            .add_tag(ItemSelector::RowKey(row_key), "alpha")
            .expect("tag should apply");
        checklist
            .add_tag(ItemSelector::RowKey(row_key), "zeta")
            .expect("duplicate tag should be a no-op");

        let item = checklist.item(row_key).expect("row should exist");
        assert_eq!(
            item.tags.iter().map(String::as_str).collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
        assert_eq!(item.edit_count, 3);
    }

    #[test]
    fn dirty_update_publishes_only_local_field_diff_so_remote_fields_survive_sync() {
        let row_key = RowKey(Uuid::from_u128(8));
        let mut checklist = test_working_set();
        let base = test_item("base", 1);
        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: base.clone(),
        }]);
        checklist.drain_queued_events();

        checklist
            .set_priority(ItemSelector::RowKey(row_key), 7)
            .expect("priority edit should apply");

        let plan = checklist
            .prepare_sync()
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
        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: remote,
        }]);
        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: merged,
        }]);

        assert_eq!(checklist.finish_successful_sync(Some(plan)), 2);
        let item = checklist.item(row_key).expect("row should remain visible");
        assert_eq!(item.text, "remote title");
        assert_eq!(item.priority, 7);
        assert_eq!(checklist.dirty_row_count(), 0);
    }

    #[test]
    fn successful_sync_clears_dirty_rows_then_applies_queued_events_in_order() {
        let row_key = RowKey(Uuid::from_u128(3));
        let mut checklist = test_working_set();
        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: test_item("base", 1),
        }]);
        checklist.drain_queued_events();
        checklist
            .rename_item(ItemSelector::RowKey(row_key), "local")
            .expect("local rename should apply");

        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: test_item("remote before sync", 2),
        }]);
        let plan = checklist
            .prepare_sync()
            .expect("plan should build")
            .expect("dirty row should produce a sync plan");
        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: test_item("local", 3),
        }]);

        assert_eq!(checklist.finish_successful_sync(Some(plan)), 2);

        let item = checklist.item(row_key).expect("row should remain visible");
        assert_eq!(item.text, "local");
        assert_eq!(item.edit_count, 3);
        assert_eq!(checklist.dirty_row_count(), 0);
        assert_eq!(checklist.queued_event_count(), 0);
    }

    #[test]
    fn failed_sync_can_leave_dirty_rows_and_queued_events_untouched() {
        let row_key = RowKey(Uuid::from_u128(4));
        let mut checklist = test_working_set();
        checklist.add_item_with_key(row_key, "local");
        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: test_item("remote", 1),
        }]);

        let plan = checklist
            .prepare_sync()
            .expect("plan should build")
            .expect("dirty row should produce a sync plan");

        assert_eq!(plan.mutations.len(), 1);
        assert_eq!(checklist.dirty_row_count(), 1);
        assert_eq!(checklist.queued_event_count(), 1);
        assert_eq!(
            checklist.item(row_key).expect("row should exist").text,
            "local"
        );
    }

    #[test]
    fn deleting_new_dirty_row_suppresses_publish() {
        let row_key = RowKey(Uuid::from_u128(5));
        let mut checklist = test_working_set();

        checklist.add_item_with_key(row_key, "transient");
        checklist
            .delete_item(ItemSelector::RowKey(row_key))
            .expect("delete should apply");

        assert!(checklist.item(row_key).is_none());
        assert_eq!(checklist.dirty_row_count(), 0);
        assert!(
            checklist
                .prepare_sync()
                .expect("plan should build")
                .is_none()
        );
    }

    #[test]
    fn deleting_existing_row_prepares_delete_mutation() {
        let row_key = RowKey(Uuid::from_u128(7));
        let mut checklist = test_working_set();

        checklist.enqueue_checklist_changes(vec![ChecklistRowChange::Upsert {
            row_key,
            item: test_item("remote", 1),
        }]);
        checklist.drain_queued_events();
        checklist
            .delete_item(ItemSelector::RowKey(row_key))
            .expect("delete should apply");

        let plan = checklist
            .prepare_sync()
            .expect("plan should build")
            .expect("dirty delete should produce a sync plan");

        assert_eq!(plan.mutations.len(), 1);
        assert!(matches!(
            &plan.mutations[0],
            RowMutation::Delete { row_id }
                if row_id.group_id == checklist.group_id()
                    && row_id.dataset_id == *checklist.dataset_id()
                    && row_id.row_key == row_key
        ));
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
        ChecklistWorkingSet::new(GroupId(Uuid::from_u128(10)))
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

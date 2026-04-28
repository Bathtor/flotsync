//! Command-surface helpers for the replicated checklist example.
//!
//! This module deliberately stops at the example command contract: parsing,
//! item addressing, and the checklist schema. The runnable process and runtime
//! wiring live in the example CLI task.

use clap::{CommandFactory, Parser, Subcommand};
use flotsync_data_types::{
    Field,
    PrimitiveType,
    Schema,
    schema::{BasicDataType, NullableBasicDataType},
};
use flotsync_replication::{DatasetId, RowKey};
use snafu::Snafu;
use std::{num::NonZeroUsize, str::FromStr};
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

pub fn checklist_dataset_id() -> DatasetId {
    DatasetId::try_new(CHECKLIST_DATASET_ID)
        .expect("checklist dataset id must be a valid dataset identifier")
}

pub fn checklist_schema() -> Schema {
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

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => Self::OPEN,
            Self::InProgress => Self::IN_PROGRESS,
            Self::Done => Self::DONE,
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
    Add {
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        text: Vec<String>,
    },
    Rename {
        item: ItemSelector,
        #[arg(required = true, num_args = 1.., trailing_var_arg = true)]
        text: Vec<String>,
    },
    Edit {
        #[command(subcommand)]
        command: EditCommand,
    },
    Tag {
        #[command(subcommand)]
        command: TagCommand,
    },
    Claim {
        item: ItemSelector,
    },
    Complete {
        item: ItemSelector,
    },
    Priority {
        item: ItemSelector,
        priority: u8,
    },
    Delete {
        item: ItemSelector,
    },
    List,
    Show {
        item: ItemSelector,
    },
    Events {
        limit: Option<usize>,
    },
    Sync,
    Members,
    Me,
    Help,
    #[command(alias = "exit")]
    Quit,
}

impl ChecklistCommand {
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
    Note { item: ItemSelector },
}

#[derive(Clone, Debug, PartialEq, Eq, Subcommand)]
pub enum TagCommand {
    Add { item: ItemSelector, tag: String },
    Rm { item: ItemSelector, tag: String },
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

pub fn checklist_help() -> String {
    ChecklistLine::command().render_long_help().to_string()
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
    use flotsync_data_types::ReplicatedDataType;

    #[test]
    fn checklist_schema_uses_agreed_replication_semantics() {
        let schema = checklist_schema();
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
            "add", "rename", "edit", "tag", "claim", "complete", "priority", "delete", "list",
            "show", "events", "sync", "members", "me", "help", "quit",
        ] {
            assert!(
                help.contains(command),
                "generated help should contain {command:?}: {help}"
            );
        }
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
}

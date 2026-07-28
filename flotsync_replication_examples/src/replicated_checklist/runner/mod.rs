use super::{
    CHECKLIST_SCHEMA,
    ChecklistCommand,
    ChecklistGroupCommand,
    ChecklistItemAssociation,
    ChecklistItemId,
    ChecklistKeyCommand,
    ChecklistWorkingSet,
    ChecklistWorkingSetError,
    EditCommand,
    ItemAssociationSelector,
    ItemSelector,
    ListedChecklistItem,
    TagCommand,
    checklist_dataset_id,
    checklist_help,
    config::{ChecklistAppConfig, ChecklistConfigError, checklist_application_id},
    parse_checklist_command,
};
use chrono::{DateTime, Local};
use clap::{Parser, Subcommand};
use flotsync_core::{GroupId, MemberIdentity, member::IdentifierParseError};
use flotsync_replication::{
    ApiError,
    CreateGroupRequest,
    GroupInvitation,
    GroupInvitationResponder,
    GroupSchema,
    ListenerError,
    LoadError,
    LoadSecurityError,
    MemberPublicKeysRecord,
    ProvisionSecurityError,
    PublishChangesRequest,
    ReadToken,
    RejectionReason,
    ReplicationApi,
    ReplicationConfig,
    ReplicationEvent,
    ReplicationEventListener,
    ReplicationGroupView,
    ReplicationSecuritySecrets,
    ReplicationStore,
    RowChange,
    RowProviderError,
    SnapshotRowsRequest,
    SqliteReplicationStore,
    StoreError,
    StoreSecretKeyId,
    SummaryRequest,
    load_local_public_key_bundle,
    load_replication_runtime_with_runtime_config_toml,
    provision_replication_security,
    security::{
        AssessPublicKeyBundleRequest,
        KnownMemberKeysReport,
        PublicKeyBundleAssessmentStorage,
        PublicKeyBundleFeedback,
        PublicKeyBundleReport,
        RecordPublicKeyBundleFeedbackRequest,
    },
};
use flotsync_security::{
    KeyFingerprint,
    PublicKeyBundle,
    STORE_SECRET_KEY_LENGTH,
    SecurityError,
    StoreSecretKey,
    generate_member_key_bundles,
};
use futures_util::{FutureExt, future::join_all};
use itertools::Itertools;
use kompact::prelude::block_on;
use sha2::{Digest, Sha256};
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    fs,
    future::Future,
    io::{self, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        LazyLock,
        mpsc::{self, Receiver, Sender, TryRecvError},
    },
    time::SystemTime,
};
use uuid::Uuid;

mod diagnostics;
mod groups;
mod keys;
mod repl;
mod setup;

const CHECKLIST_SNAPSHOT_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(128).unwrap();
/// Immutable dataset schema assigned to every group created by the checklist application.
static CHECKLIST_GROUP_SCHEMA: LazyLock<GroupSchema> = LazyLock::new(|| {
    GroupSchema::new(HashMap::from([(
        checklist_dataset_id(),
        CHECKLIST_SCHEMA.clone().into(),
    )]))
});
// TODO(flotsync-lsi8): Remove this unsafe profile escape hatch once headless
// local store-secret backends are implemented.
const UNSAFE_STORE_SECRET_PROFILE_PREFIX: &str = "unsafe:";
const UNSAFE_STORE_SECRET_KEY_ID_DOMAIN: &[u8] =
    b"flotsync/examples/replicated-checklist/unsafe-store-secret-key-id/v1";
const UNSAFE_STORE_SECRET_KEY_DOMAIN: &[u8] =
    b"flotsync/examples/replicated-checklist/unsafe-store-secret-key/v1";

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
    /// Initialise store-native checklist identity keys before runtime startup.
    Keys {
        #[command(subcommand)]
        command: ReplicatedChecklistKeyCommand,
    },
}

/// Pre-runtime local identity commands.
#[derive(Clone, Debug, Subcommand)]
pub enum ReplicatedChecklistKeyCommand {
    /// Create or reuse this peer's local identity keys.
    InitLocal {
        /// Path to the node-specific checklist TOML config.
        config: PathBuf,
    },
}

/// Run one configured replicated checklist REPL.
///
/// # Errors
///
/// See [`ReplicatedChecklistError`] for failure conditions.
#[allow(
    clippy::needless_pass_by_value,
    reason = "Example entry points consume parsed CLI argument structs."
)]
pub fn run(args: ReplicatedChecklistArgs) -> Result<(), ReplicatedChecklistError> {
    match args.command {
        ReplicatedChecklistCommand::Run { config } => block_on(repl::run_configured_peer(&config)),
        ReplicatedChecklistCommand::Keys { command } => block_on(keys::run_key_command(command)),
    }
}

/// Errors from the replicated checklist binary.
#[derive(Debug, Snafu)]
#[snafu(module(repl_error))]
pub enum ReplicatedChecklistError {
    #[snafu(display("{source}"))]
    Config { source: ChecklistConfigError },
    #[snafu(display("Failed to load checklist local store secret: {source}"))]
    LocalStoreSecret { source: LoadSecurityError },
    #[snafu(display("Security operation failed while {action}: {source}"))]
    Security {
        action: &'static str,
        source: SecurityError,
    },
    #[snafu(display("Security provisioning failed while {action}: {source}"))]
    ProvisionSecurity {
        action: &'static str,
        source: ProvisionSecurityError,
    },
    #[snafu(display("Failed to prepare checklist store directory {}: {source}", path.display()))]
    CreateStoreDirectory { path: PathBuf, source: io::Error },
    #[snafu(display("Failed to open checklist replication store: {source}"))]
    Store { source: StoreError },
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
    #[snafu(display("No readable checklist group matches {selector:?}."))]
    UnknownGroup { selector: String },
    #[snafu(display(
        "Checklist group name {name:?} is ambiguous; matching group UUIDs: {candidate_ids:?}."
    ))]
    AmbiguousGroupName {
        name: String,
        candidate_ids: Vec<GroupId>,
    },
    #[snafu(display("Checklist group {group_id} is not writable and cannot become default."))]
    NonWritableDefaultGroup { group_id: GroupId },
    #[snafu(display("Checklist group {group_id} is not writable and cannot receive items."))]
    NonWritableTargetGroup { group_id: GroupId },
    #[snafu(display("Checklist item reference {selector:?} does not resolve to a visible row."))]
    UnknownItemReference { selector: ItemSelector },
    #[snafu(display(
        "Checklist item UUID {row_key} is ambiguous; use one of: {}.",
        candidates.join(", ")
    ))]
    AmbiguousItemReference {
        row_key: flotsync_replication::RowKey,
        candidates: Vec<String>,
    },
    #[snafu(display(
        "No default checklist group is selected. Use 'group default <name-or-uuid>' first."
    ))]
    NoDefaultGroup,
    #[snafu(display("Listener reported changes for unknown checklist group {group_id}."))]
    UnknownListenerGroup { group_id: GroupId },
    #[snafu(display("Proposed group member identity is invalid: {source}"))]
    InvalidGroupMemberIdentity { source: IdentifierParseError },
    #[snafu(display("Member {member_id} occurs more than once in the proposed group."))]
    DuplicateGroupMember { member_id: MemberIdentity },
    #[snafu(display(
        "The local creator {member_id} is added automatically and must not be repeated."
    ))]
    RepeatedGroupCreator { member_id: MemberIdentity },
    #[snafu(display(
        "No pending group invitation exists at position {position}; {available} invitation(s) are pending."
    ))]
    UnknownGroupInvitation {
        position: NonZeroUsize,
        available: usize,
    },
    #[snafu(display(
        "Unexpected confirmation response {response:?}; enter y/yes to continue or n/no to cancel."
    ))]
    InvalidConfirmationResponse { response: String },
}

/// Read one fail-closed confirmation from standard input.
fn confirm(prompt: &str) -> Result<bool, ReplicatedChecklistError> {
    print!("{prompt} [y/N] ");
    io::stdout().flush().context(repl_error::IoSnafu {
        action: "flushing confirmation prompt",
    })?;
    let mut answer = String::new();
    let bytes_read = io::stdin()
        .read_line(&mut answer)
        .context(repl_error::IoSnafu {
            action: "reading confirmation",
        })?;
    if bytes_read == 0 {
        return Ok(false);
    }
    parse_confirmation(&answer)
}

/// Parse one confirmation answer while rejecting unrecognised input.
fn parse_confirmation(answer: &str) -> Result<bool, ReplicatedChecklistError> {
    let answer = answer.trim();
    if answer.is_empty() || answer.eq_ignore_ascii_case("n") || answer.eq_ignore_ascii_case("no") {
        return Ok(false);
    }
    if answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes") {
        return Ok(true);
    }
    Err(ReplicatedChecklistError::InvalidConfirmationResponse {
        response: answer.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn top_level_retains_only_local_key_initialisation() {
        let init = ReplicatedChecklistArgs::try_parse_from([
            "replicated-checklist",
            "keys",
            "init-local",
            "alice.toml",
        ])
        .expect("init command should parse");
        assert!(matches!(
            init.command,
            ReplicatedChecklistCommand::Keys {
                command: ReplicatedChecklistKeyCommand::InitLocal { .. }
            }
        ));

        let former_export = ReplicatedChecklistArgs::try_parse_from([
            "replicated-checklist",
            "keys",
            "export-local",
            "alice.toml",
        ]);
        assert!(former_export.is_err());
    }

    #[test]
    fn confirmation_distinguishes_yes_no_and_unexpected_answers() {
        assert!(parse_confirmation("y").expect("y should confirm"));
        assert!(parse_confirmation(" YES ").expect("yes should confirm"));
        assert!(!parse_confirmation("").expect("empty input should cancel"));
        assert!(!parse_confirmation("N").expect("n should cancel"));
        assert!(!parse_confirmation("no").expect("no should cancel"));

        let error = parse_confirmation("ABSOLUTE GASBEHVBEWVBWEKC")
            .expect_err("unexpected confirmation should fail");
        assert!(matches!(
            &error,
            ReplicatedChecklistError::InvalidConfirmationResponse { response }
                if response == "ABSOLUTE GASBEHVBEWVBWEKC"
        ));
        assert_eq!(
            error.to_string(),
            "Unexpected confirmation response \"ABSOLUTE GASBEHVBEWVBWEKC\"; enter y/yes to continue or n/no to cancel."
        );
    }
}

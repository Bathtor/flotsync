use super::{
    CHECKLIST_SCHEMA,
    ChecklistCommand,
    ChecklistKeyCommand,
    ChecklistWorkingSet,
    ChecklistWorkingSetError,
    EditCommand,
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
use flotsync_core::{GroupId, MemberIdentity};
use flotsync_replication::{
    ApiError,
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
    ReplicationGroupRecord,
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
use kompact::prelude::block_on;
use sha2::{Digest, Sha256};
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

mod keys;
mod repl;
mod setup;

const CHECKLIST_SNAPSHOT_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(128).unwrap();
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
    #[snafu(display(
        "Checklist currently supports at most one active group; the store contains {actual_count}. Multi-group workspace support lands in flotsync-git-d03.3."
    ))]
    MultipleActiveGroups { actual_count: usize },
    #[snafu(display(
        "No active checklist group is available. Key commands remain available; group creation lands in flotsync-git-d03.3."
    ))]
    NoActiveGroup,
    #[snafu(display(
        "Unexpected confirmation response {response:?}; enter y/yes to continue or n/no to cancel."
    ))]
    InvalidConfirmationResponse { response: String },
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
}

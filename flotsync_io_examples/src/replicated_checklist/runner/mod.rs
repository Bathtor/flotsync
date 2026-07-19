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
    config::{ChecklistAppConfig, ChecklistConfigError, checklist_application_id},
    parse_checklist_command,
};
use chrono::{DateTime, Local};
use clap::{Parser, Subcommand};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    membership::{GroupMembers, GroupMembersError},
    versions::VersionVector,
};
use flotsync_replication::{
    ApiError,
    AuthorityScope,
    GroupMemberKeys,
    GroupSchema,
    ListenerError,
    LoadError,
    LoadSecurityError,
    MemberKeyId,
    MemberKeyTrustEvidenceKind,
    MemberKeyTrustEvidenceRecord,
    MemberKeyTrustRequirement,
    MemberPublicKeysRecord,
    PermissionDecision,
    PermissionDenialReason,
    ProvisionSecurityError,
    PublishChangesRequest,
    ReadToken,
    RejectionReason,
    ReplicationApi,
    ReplicationConfig,
    ReplicationEvent,
    ReplicationEventListener,
    ReplicationGroupLifecycle,
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
    TrustPolicy,
    load_local_public_key_bundle,
    load_replication_runtime_with_runtime_config_toml,
    prepare_initial_group_security_material,
    provision_replication_security,
    validate_initial_group_security_material,
};
use flotsync_security::{
    KeyFingerprint,
    PublicKeyBundle,
    PublicMemberKeys,
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
    collections::{HashMap, HashSet},
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
mod static_group;

pub use static_group::StaticGroupError;

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
    /// Manage store-native checklist identity keys.
    Keys {
        #[command(subcommand)]
        command: ReplicatedChecklistKeyCommand,
    },
}

/// Store-native key setup and trust commands.
#[derive(Clone, Debug, Subcommand)]
pub enum ReplicatedChecklistKeyCommand {
    /// Create or reuse this peer's local identity keys.
    InitLocal {
        /// Path to the node-specific checklist TOML config.
        config: PathBuf,
    },
    /// Print this peer's copyable public key bundle.
    ExportLocal {
        /// Path to the node-specific checklist TOML config.
        config: PathBuf,
    },
    /// Inspect a pasted public key bundle without trusting it.
    Inspect {
        /// Path to the node-specific checklist TOML config.
        config: PathBuf,
        /// Pasteable public key bundle text.
        public_bundle: String,
    },
    /// Trust a pasted public key bundle for one exact member identity.
    Trust {
        /// Path to the node-specific checklist TOML config.
        config: PathBuf,
        /// Exact member identity to trust for the bundle.
        member_id: MemberIdentity,
        /// Pasteable public key bundle text.
        public_bundle: String,
    },
    /// Block a key fingerprint globally in this local store.
    Block {
        /// Path to the node-specific checklist TOML config.
        config: PathBuf,
        /// Canonical padded base64url key fingerprint.
        fingerprint: KeyFingerprint,
    },
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
    #[snafu(display(
        "Local identity keys for member {member_id} are missing. Run keys init-local <config> before this command."
    ))]
    MissingLocalIdentityKeys { member_id: MemberIdentity },
    #[snafu(display(
        "Public key bundle fingerprint {fingerprint} is globally blocked and cannot be trusted for member {member_id}."
    ))]
    BlockedKeyTrust {
        member_id: MemberIdentity,
        fingerprint: KeyFingerprint,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        keys::{
            public_bundle_inspection_report,
            store_observed_public_key_binding,
            store_trusted_public_key_binding,
        },
        setup::load_checklist_replication_security,
        static_group::{
            StaticGroupSetupStatus,
            ensure_configured_group,
            initialise_configured_group_if_ready,
        },
        *,
    };
    use flotsync_core::versions::VersionVector;
    use flotsync_replication::{
        GroupMemberKeys,
        MemberKeyId,
        prepare_initial_group_security_material,
        test_support::{test_public_member_keys, test_replication_security_secrets},
    };
    use flotsync_security::{
        GROUP_KEY_LENGTH,
        GeneratedMemberKeyBundles,
        GroupKey,
        test_support::{TEST_MEMBER_KEY_SEED_LENGTH, member_key_bundles_from_seed},
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
        ChecklistAppConfig {
            source_path: PathBuf::from("test.toml"),
            runtime_config_toml: String::new(),
            local_member,
            store_path,
            store_secret_profile: flotsync_replication::LocalStoreSecretProfile::new(
                "test-profile",
            )
            .expect("test profile should build"),
            group_key: test_group_key(1),
            group_id,
            ordered_members,
        }
    }

    fn test_group_key(seed: u8) -> Arc<GroupKey> {
        Arc::new(GroupKey::from_bytes([seed; GROUP_KEY_LENGTH]))
    }

    fn generated_test_bundle(
        member_id: MemberIdentity,
        seed: u8,
    ) -> (GeneratedMemberKeyBundles, PublicKeyBundle) {
        let seed = [seed; TEST_MEMBER_KEY_SEED_LENGTH];
        let generated = member_key_bundles_from_seed(member_id, &seed);
        let bundle = PublicKeyBundle::from_bytes(&generated.public_bundle)
            .expect("generated public bundle should decode");
        (generated, bundle)
    }

    fn test_store(local_member: MemberIdentity) -> SqliteReplicationStore {
        block_on(SqliteReplicationStore::in_memory(local_member)).expect("store should build")
    }

    fn provision_local_test_keys(
        store: &SqliteReplicationStore,
        config: &ChecklistAppConfig,
        replication_security: &ReplicationSecuritySecrets,
        seed: u8,
    ) -> PublicKeyBundle {
        let (generated, bundle) = generated_test_bundle(config.local_member.clone(), seed);
        block_on(async {
            provision_replication_security(
                store,
                &config.local_member,
                replication_security,
                generated.local_private_bundle.as_bytes(),
                std::iter::empty(),
            )
            .await
            .expect("local keys should provision");
            store_observed_public_key_binding(store, config.local_member.clone(), &bundle, false)
                .await
                .expect("local public binding should store");
        });
        bundle
    }

    fn block_test_fingerprint(store: &SqliteReplicationStore, fingerprint: KeyFingerprint) {
        block_on(async {
            let mut transaction = store
                .begin_transaction()
                .await
                .expect("transaction should start");
            transaction
                .ensure_blocked_key_fingerprint(fingerprint)
                .await
                .expect("blocked fingerprint should store");
            transaction
                .commit()
                .await
                .expect("transaction should commit");
        });
    }

    fn load_test_groups(store: &SqliteReplicationStore) -> Vec<ReplicationGroupRecord> {
        block_on(async {
            let mut transaction = store
                .begin_read_transaction()
                .await
                .expect("transaction should start");
            let groups = transaction
                .load_replication_groups()
                .await
                .expect("groups should load");
            transaction
                .release()
                .await
                .expect("transaction should release");
            groups
        })
    }

    /// Build exact member-key groups for tests that manually insert static-group records.
    fn test_group_member_keys(ordered_members: &[MemberIdentity]) -> GroupMemberKeys {
        let member_keys = ordered_members
            .iter()
            .cloned()
            .map(|member_id| MemberKeyId {
                fingerprint: test_public_member_keys(&member_id).fingerprint(),
                member_id,
            })
            .collect::<Vec<_>>();
        GroupMemberKeys::from_ordered_member_keys(member_keys)
            .expect("test group member keys should build")
    }

    /// Insert an existing static-group record for reload-path tests while setup is deferred.
    fn insert_configured_group(
        store: &SqliteReplicationStore,
        config: &ChecklistAppConfig,
        replication_security: &ReplicationSecuritySecrets,
    ) {
        let members = GroupMembers::from_ordered_members(config.ordered_members.clone())
            .expect("test members should build");
        let local_member_index = members
            .member_index(&config.local_member)
            .expect("test local member should belong to group");
        let member_count = NonZeroUsize::new(config.ordered_members.len())
            .expect("test group should not be empty");
        let security_material = prepare_initial_group_security_material(
            config.group_id,
            replication_security,
            config.group_key.as_ref(),
        )
        .expect("test group security should prepare");
        block_on(async {
            let mut transaction = store
                .begin_transaction()
                .await
                .expect("transaction should start");
            transaction
                .insert_replication_group(ReplicationGroupRecord {
                    group_id: config.group_id,
                    member_keys: test_group_member_keys(&config.ordered_members),
                    local_member_index,
                    version_vector: VersionVector::initial(member_count),
                    lifecycle: ReplicationGroupLifecycle::Open,
                    security_material,
                    group_schema: GroupSchema::new(HashMap::from([(
                        checklist_dataset_id(),
                        CHECKLIST_SCHEMA.clone().into(),
                    )])),
                })
                .await
                .expect("test group should insert");
            transaction
                .commit()
                .await
                .expect("transaction should commit");
        });
    }

    #[test]
    fn parses_store_native_key_commands() {
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

        let bob_bundle = "bundle-text";
        let trust = ReplicatedChecklistArgs::try_parse_from([
            "replicated-checklist",
            "keys",
            "trust",
            "alice.toml",
            "bob",
            bob_bundle,
        ])
        .expect("trust command should parse");
        assert!(matches!(
            trust.command,
            ReplicatedChecklistCommand::Keys {
                command: ReplicatedChecklistKeyCommand::Trust { .. }
            }
        ));

        let fingerprint = KeyFingerprint::from_bytes([3; 32]).to_canonical_base64url();
        let block = ReplicatedChecklistArgs::try_parse_from([
            "replicated-checklist",
            "keys",
            "block",
            "alice.toml",
            &fingerprint,
        ])
        .expect("block command should parse");
        assert!(matches!(
            block.command,
            ReplicatedChecklistCommand::Keys {
                command: ReplicatedChecklistKeyCommand::Block { .. }
            }
        ));
    }

    #[test]
    fn local_key_setup_remains_pending_until_remote_trust_exists() {
        let group_id = GroupId(Uuid::from_u128(99));
        let config = test_config_with_members(
            group_id,
            PathBuf::from("unused.sqlite"),
            vec![
                MemberIdentity::from_array(["alice"]),
                MemberIdentity::from_array(["bob"]),
            ],
        );
        let replication_security = test_replication_security_secrets();
        let store = test_store(config.local_member.clone());
        let local_bundle = provision_local_test_keys(&store, &config, &replication_security, 1);

        let loaded = block_on(load_local_public_key_bundle(
            &store,
            &config.local_member,
            &replication_security,
        ))
        .expect("local public bundle should load")
        .expect("local public bundle should exist");
        let status = block_on(initialise_configured_group_if_ready(
            &store,
            &config,
            &replication_security,
        ))
        .expect("static group setup should not fail");

        assert_eq!(loaded, local_bundle);
        assert!(matches!(status, StaticGroupSetupStatus::Pending(_)));
        assert!(load_test_groups(&store).is_empty());
    }

    #[test]
    fn trusted_remote_key_initialises_static_group_when_ready() {
        let group_id = GroupId(Uuid::from_u128(100));
        let bob = MemberIdentity::from_array(["bob"]);
        let config = test_config_with_members(
            group_id,
            PathBuf::from("unused.sqlite"),
            vec![MemberIdentity::from_array(["alice"]), bob.clone()],
        );
        let replication_security = test_replication_security_secrets();
        let store = test_store(config.local_member.clone());
        let local_bundle = provision_local_test_keys(&store, &config, &replication_security, 2);
        let (_, bob_bundle) = generated_test_bundle(bob.clone(), 3);

        let warnings = block_on(store_trusted_public_key_binding(&store, &bob, &bob_bundle))
            .expect("trusted binding should store");
        let status = block_on(initialise_configured_group_if_ready(
            &store,
            &config,
            &replication_security,
        ))
        .expect("static group setup should succeed");

        assert!(warnings.is_empty());
        assert!(matches!(status, StaticGroupSetupStatus::Initialised));
        let groups = load_test_groups(&store);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].member_keys.ordered_member_keys(),
            &[
                MemberKeyId {
                    member_id: config.local_member.clone(),
                    fingerprint: local_bundle.fingerprint(),
                },
                MemberKeyId {
                    member_id: bob,
                    fingerprint: bob_bundle.fingerprint(),
                },
            ]
        );
    }

    #[test]
    fn blocked_local_key_keeps_static_group_pending() {
        let group_id = GroupId(Uuid::from_u128(103));
        let config = test_config(group_id, PathBuf::from("unused.sqlite"));
        let replication_security = test_replication_security_secrets();
        let store = test_store(config.local_member.clone());
        let local_bundle = provision_local_test_keys(&store, &config, &replication_security, 6);
        block_test_fingerprint(&store, local_bundle.fingerprint());

        let status = block_on(initialise_configured_group_if_ready(
            &store,
            &config,
            &replication_security,
        ))
        .expect("static group setup should not fail");

        let StaticGroupSetupStatus::Pending(reasons) = status else {
            panic!("blocked local key should keep setup pending");
        };
        assert_eq!(
            reasons,
            vec![format!(
                "local member {} fingerprint is globally blocked: {}",
                config.local_member,
                local_bundle.fingerprint()
            )]
        );
        assert!(load_test_groups(&store).is_empty());
    }

    #[test]
    fn blocked_trusted_remote_key_keeps_static_group_pending() {
        let group_id = GroupId(Uuid::from_u128(104));
        let bob = MemberIdentity::from_array(["bob"]);
        let carol = MemberIdentity::from_array(["carol"]);
        let config = test_config_with_members(
            group_id,
            PathBuf::from("unused.sqlite"),
            vec![
                MemberIdentity::from_array(["alice"]),
                bob.clone(),
                carol.clone(),
            ],
        );
        let replication_security = test_replication_security_secrets();
        let store = test_store(config.local_member.clone());
        let _local_bundle = provision_local_test_keys(&store, &config, &replication_security, 7);
        let (_, bob_bundle) = generated_test_bundle(bob.clone(), 8);
        let (_, carol_bundle) = generated_test_bundle(carol.clone(), 9);
        let bob_warnings = block_on(store_trusted_public_key_binding(&store, &bob, &bob_bundle))
            .expect("trusted bob binding should store");
        block_test_fingerprint(&store, bob_bundle.fingerprint());
        let carol_warnings = block_on(store_trusted_public_key_binding(
            &store,
            &carol,
            &carol_bundle,
        ))
        .expect("trusted carol binding should store");

        let status = block_on(initialise_configured_group_if_ready(
            &store,
            &config,
            &replication_security,
        ))
        .expect("static group setup should not fail");

        assert!(bob_warnings.is_empty());
        assert!(carol_warnings.is_empty());
        let StaticGroupSetupStatus::Pending(reasons) = status else {
            panic!("blocked trusted remote key should keep setup pending");
        };
        assert_eq!(
            reasons,
            vec![format!(
                "member {bob} has no unblocked trusted public key bundle; blocked trusted fingerprints: {}",
                bob_bundle.fingerprint()
            )]
        );
        assert!(load_test_groups(&store).is_empty());
    }

    #[test]
    fn inspect_reports_observed_untrusted_bundle_without_granting_trust() {
        let group_id = GroupId(Uuid::from_u128(101));
        let bob = MemberIdentity::from_array(["bob"]);
        let config = test_config_with_members(
            group_id,
            PathBuf::from("unused.sqlite"),
            vec![MemberIdentity::from_array(["alice"]), bob.clone()],
        );
        let store = test_store(config.local_member.clone());
        let (_, bob_bundle) = generated_test_bundle(bob.clone(), 4);
        block_on(store_observed_public_key_binding(
            &store,
            bob.clone(),
            &bob_bundle,
            false,
        ))
        .expect("observed binding should store");

        let report = block_on(public_bundle_inspection_report(
            &store,
            &config,
            &bob_bundle,
        ))
        .expect("inspection should report");

        assert!(!report.globally_blocked);
        assert_eq!(report.bindings.len(), 1);
        assert_eq!(report.bindings[0].key_id.member_id, bob);
        assert!(!report.bindings[0].has_local_explicit_trust);
        assert!(
            report.bindings[0]
                .authority
                .iter()
                .any(|(scope, decision)| {
                    *scope == AuthorityScope::ReplicationRuntime
                        && *decision
                            == PermissionDecision::Deny(
                                PermissionDenialReason::MissingTrustEvidence,
                            )
                })
        );
    }

    #[test]
    fn blocked_fingerprint_is_visible_in_inspection() {
        let group_id = GroupId(Uuid::from_u128(102));
        let bob = MemberIdentity::from_array(["bob"]);
        let config = test_config_with_members(
            group_id,
            PathBuf::from("unused.sqlite"),
            vec![MemberIdentity::from_array(["alice"]), bob.clone()],
        );
        let store = test_store(config.local_member.clone());
        let (_, bob_bundle) = generated_test_bundle(bob.clone(), 5);
        block_on(store_observed_public_key_binding(
            &store,
            bob,
            &bob_bundle,
            true,
        ))
        .expect("trusted binding should store");
        block_test_fingerprint(&store, bob_bundle.fingerprint());

        let report = block_on(public_bundle_inspection_report(
            &store,
            &config,
            &bob_bundle,
        ))
        .expect("inspection should report");

        assert!(report.globally_blocked);
        assert!(report.bindings[0].authority.iter().all(|(_, decision)| {
            *decision == PermissionDecision::Deny(PermissionDenialReason::FingerprintBlocked)
        }));
    }

    // TODO(flotsync-lsi8): Remove these unsafe profile tests with the temporary
    // derivation path they cover.
    #[test]
    fn unsafe_store_secret_profile_derives_stable_security_without_keyring() {
        let mut config = test_config(GroupId(Uuid::from_u128(0x51afe)), PathBuf::from("unused"));
        config.store_secret_profile =
            flotsync_replication::LocalStoreSecretProfile::new("unsafe:raspberrypi")
                .expect("unsafe test profile should build");

        let first =
            load_checklist_replication_security(&config).expect("unsafe security should derive");
        let second =
            load_checklist_replication_security(&config).expect("unsafe security should derive");

        assert_eq!(first.store_secret_key_id(), second.store_secret_key_id());
        assert_ne!(
            first.store_secret_key_id().to_string(),
            "00000000-0000-0000-0000-000000000000"
        );
    }

    #[test]
    fn unsafe_store_secret_profile_changes_derived_key_id() {
        let mut first_config =
            test_config(GroupId(Uuid::from_u128(0x51aff)), PathBuf::from("unused-a"));
        first_config.store_secret_profile =
            flotsync_replication::LocalStoreSecretProfile::new("unsafe:first-pi")
                .expect("first unsafe test profile should build");
        let mut second_config =
            test_config(GroupId(Uuid::from_u128(0x51b00)), PathBuf::from("unused-b"));
        second_config.store_secret_profile =
            flotsync_replication::LocalStoreSecretProfile::new("unsafe:second-pi")
                .expect("second unsafe test profile should build");

        let first = load_checklist_replication_security(&first_config)
            .expect("first unsafe security should derive");
        let second = load_checklist_replication_security(&second_config)
            .expect("second unsafe security should derive");

        assert_ne!(first.store_secret_key_id(), second.store_secret_key_id());
    }

    #[test]
    fn ensure_configured_group_rejects_missing_key_setup() {
        let group_id = GroupId(Uuid::from_u128(103));
        let config = test_config(group_id, PathBuf::from("unused.sqlite"));
        let replication_security = test_replication_security_secrets();
        let store = test_store(config.local_member.clone());

        let result = block_on(ensure_configured_group(
            &store,
            &config,
            &replication_security,
        ));

        assert!(matches!(result, Err(StaticGroupError::KeySetupRequired)));
        assert!(load_test_groups(&store).is_empty());
    }

    #[test]
    fn ensure_configured_group_rejects_member_mismatch() {
        let group_id = GroupId(Uuid::from_u128(101));
        let config = test_config(group_id, PathBuf::from("unused.sqlite"));
        let replication_security = test_replication_security_secrets();
        let store = test_store(config.local_member.clone());
        provision_local_test_keys(&store, &config, &replication_security, 6);
        let mut existing = config.clone();
        existing.ordered_members = vec![
            MemberIdentity::from_array(["alice"]),
            MemberIdentity::from_array(["carol"]),
        ];
        insert_configured_group(&store, &existing, &replication_security);

        let result = block_on(ensure_configured_group(
            &store,
            &config,
            &replication_security,
        ));

        assert!(matches!(
            result,
            Err(StaticGroupError::MemberMismatch { .. })
        ));
    }

    #[test]
    fn ensure_configured_group_rejects_existing_group_secret_mismatch() {
        let group_id = GroupId(Uuid::from_u128(104));
        let insert_config = test_config(group_id, PathBuf::from("unused.sqlite"));
        let replication_security = test_replication_security_secrets();
        let store = test_store(insert_config.local_member.clone());
        provision_local_test_keys(&store, &insert_config, &replication_security, 7);
        insert_configured_group(&store, &insert_config, &replication_security);
        let mut mismatched_config = insert_config.clone();
        mismatched_config.group_key = test_group_key(2);

        let result = block_on(ensure_configured_group(
            &store,
            &mismatched_config,
            &replication_security,
        ));

        assert!(matches!(
            result,
            Err(StaticGroupError::ExistingGroupSecurity {
                source: ProvisionSecurityError::GroupSecretMismatch { .. },
                ..
            })
        ));
    }
}

//! Local identity initialisation and runtime-backed key commands.

use super::{setup::load_checklist_store_setup, *};

/// Run the sole pre-runtime key-management command.
pub(super) async fn run_key_command(
    command: ReplicatedChecklistKeyCommand,
) -> Result<(), ReplicatedChecklistError> {
    match command {
        ReplicatedChecklistKeyCommand::InitLocal { config } => init_local_keys(&config).await,
    }
}

/// Run one key command through the already-unlocked replication runtime.
pub(super) async fn run_runtime_key_command(
    replication: &dyn ReplicationApi,
    command: ChecklistKeyCommand,
) -> Result<(), ReplicatedChecklistError> {
    let mut confirm_operation = confirm;
    run_runtime_key_command_with_confirmation(replication, command, &mut confirm_operation).await
}

/// Run one runtime key command with an injected confirmation boundary.
async fn run_runtime_key_command_with_confirmation(
    replication: &dyn ReplicationApi,
    command: ChecklistKeyCommand,
    confirm_operation: &mut dyn FnMut(&str) -> Result<bool, ReplicatedChecklistError>,
) -> Result<(), ReplicatedChecklistError> {
    match command {
        ChecklistKeyCommand::ExportLocal => export_local_keys(replication).await,
        ChecklistKeyCommand::Inspect { public_bundle } => {
            inspect_public_bundle(replication, &public_bundle).await
        }
        ChecklistKeyCommand::Trust {
            member_id,
            public_bundle,
        } => trust_public_bundle(replication, member_id, &public_bundle, confirm_operation).await,
        ChecklistKeyCommand::Block { public_bundle } => {
            block_public_bundle(replication, &public_bundle, confirm_operation).await
        }
    }
}

/// Create local identity keys if absent and print the local public bundle.
async fn init_local_keys(config_path: &Path) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path).await?;
    let existing_bundle = load_local_public_key_bundle(
        setup.store.as_ref(),
        &setup.config.local_member,
        &setup.replication_security,
    )
    .await
    .context(repl_error::ProvisionSecuritySnafu {
        action: "loading local identity keys",
    })?;
    let (bundle, created) = if let Some(bundle) = existing_bundle {
        (bundle, false)
    } else {
        let generated = generate_member_key_bundles(setup.config.local_member.clone()).context(
            repl_error::SecuritySnafu {
                action: "generating local identity keys",
            },
        )?;
        provision_replication_security(
            setup.store.as_ref(),
            &setup.config.local_member,
            &setup.replication_security,
            generated.local_private_bundle.as_bytes(),
            std::iter::empty(),
        )
        .await
        .context(repl_error::ProvisionSecuritySnafu {
            action: "storing local identity keys",
        })?;
        let bundle = PublicKeyBundle::from_bytes(&generated.public_bundle).context(
            repl_error::SecuritySnafu {
                action: "decoding generated public key bundle",
            },
        )?;
        (bundle, true)
    };

    store_observed_public_key_binding(
        setup.store.as_ref(),
        setup.config.local_member.clone(),
        &bundle,
    )
    .await?;
    println!(
        "local identity keys: {}",
        if created { "created" } else { "existing" }
    );
    print_local_public_bundle(&setup.config.local_member, &bundle);
    Ok(())
}

/// Print the local public bundle from the running replication runtime.
async fn export_local_keys(
    replication: &dyn ReplicationApi,
) -> Result<(), ReplicatedChecklistError> {
    let bundle = replication
        .local_public_key_bundle()
        .await
        .context(repl_error::ReplicationSnafu)?;
    print_local_public_bundle_without_member(&bundle);
    Ok(())
}

/// Assess one public bundle without changing local security state.
async fn inspect_public_bundle(
    replication: &dyn ReplicationApi,
    public_bundle: &str,
) -> Result<(), ReplicatedChecklistError> {
    let bundle = decode_pasteable_public_bundle(public_bundle)?;
    let report = assess_public_bundle(replication, bundle, HashSet::new()).await?;
    println!("{report}");
    Ok(())
}

/// Assess and, after confirmation, trust one bundle for an exact member.
async fn trust_public_bundle(
    replication: &dyn ReplicationApi,
    member_id: MemberIdentity,
    public_bundle: &str,
    confirm_operation: &mut dyn FnMut(&str) -> Result<bool, ReplicatedChecklistError>,
) -> Result<(), ReplicatedChecklistError> {
    let bundle = decode_pasteable_public_bundle(public_bundle)?;
    let report = assess_public_bundle(
        replication,
        bundle.clone(),
        HashSet::from([member_id.clone()]),
    )
    .await?;
    println!("{report}");
    if !confirm_operation(&format!("Trust this bundle for member {member_id}?"))? {
        println!("trust cancelled");
        return Ok(());
    }
    let fingerprint = bundle.fingerprint();
    replication
        .record_public_key_bundle_feedback(RecordPublicKeyBundleFeedbackRequest {
            bundle,
            feedback: PublicKeyBundleFeedback::TrustMember {
                member_id: member_id.clone(),
            },
        })
        .await
        .context(repl_error::ReplicationSnafu)?;
    println!("trusted member: {member_id}");
    print_fingerprint("trusted fingerprint", fingerprint);
    Ok(())
}

/// Assess and, after confirmation, block the fingerprint derived from one bundle.
async fn block_public_bundle(
    replication: &dyn ReplicationApi,
    public_bundle: &str,
    confirm_operation: &mut dyn FnMut(&str) -> Result<bool, ReplicatedChecklistError>,
) -> Result<(), ReplicatedChecklistError> {
    let bundle = decode_pasteable_public_bundle(public_bundle)?;
    let report = assess_public_bundle(replication, bundle.clone(), HashSet::new()).await?;
    println!("{report}");
    if !confirm_operation("Block this bundle fingerprint globally?")? {
        println!("block cancelled");
        return Ok(());
    }
    let fingerprint = bundle.fingerprint();
    replication
        .record_public_key_bundle_feedback(RecordPublicKeyBundleFeedbackRequest {
            bundle,
            feedback: PublicKeyBundleFeedback::BlockFingerprint,
        })
        .await
        .context(repl_error::ReplicationSnafu)?;
    println!("blocked fingerprint recorded");
    print_fingerprint("blocked fingerprint", fingerprint);
    Ok(())
}

/// Ask the runtime for one read-only public-bundle assessment.
async fn assess_public_bundle(
    replication: &dyn ReplicationApi,
    bundle: PublicKeyBundle,
    candidate_member_ids: HashSet<MemberIdentity>,
) -> Result<PublicKeyBundleReport, ReplicatedChecklistError> {
    replication
        .assess_public_key_bundle(AssessPublicKeyBundleRequest {
            bundle,
            candidate_member_ids,
            material_storage: PublicKeyBundleAssessmentStorage::ReadOnly,
        })
        .await
        .context(repl_error::ReplicationSnafu)
}

/// Decode pasteable public bundle text for runtime command handlers.
fn decode_pasteable_public_bundle(
    input: &str,
) -> Result<PublicKeyBundle, ReplicatedChecklistError> {
    PublicKeyBundle::from_pasteable_string(input).context(repl_error::SecuritySnafu {
        action: "decoding public key bundle",
    })
}

/// Store the local public-key binding needed by later member enumeration.
async fn store_observed_public_key_binding(
    store: &dyn ReplicationStore,
    member_id: MemberIdentity,
    bundle: &PublicKeyBundle,
) -> Result<(), ReplicatedChecklistError> {
    let public_keys = bundle.clone().bind_member(member_id);
    let record = MemberPublicKeysRecord::from_public_keys(&public_keys);
    let mut transaction = store
        .begin_transaction()
        .await
        .context(repl_error::StoreSnafu)?;
    transaction
        .ensure_member_public_keys(record)
        .await
        .context(repl_error::StoreSnafu)?;
    transaction.commit().await.context(repl_error::StoreSnafu)
}

/// Print the local public bundle in copy/paste and verification forms.
fn print_local_public_bundle(member_id: &MemberIdentity, bundle: &PublicKeyBundle) {
    println!("member id: {member_id}");
    print_local_public_bundle_without_member(bundle);
}

/// Print identity-free local public material returned by the runtime.
fn print_local_public_bundle_without_member(bundle: &PublicKeyBundle) {
    println!("public bundle (copy this value):");
    println!("{}", bundle.to_pasteable_string());
    print_fingerprint("fingerprint", bundle.fingerprint());
}

/// Print one fingerprint in parseable and display forms.
fn print_fingerprint(label: &str, fingerprint: KeyFingerprint) {
    println!("{label}: {}", fingerprint.to_canonical_base64url());
    println!("{label} display: {fingerprint}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_replication::{
        ChangeGroupMembershipRequest,
        CreateGroupRequest,
        MigrationId,
        PublishReceipt,
        SnapshotValueRows,
        Summary,
        security::{KnownMemberKeysReport, PublicKeyBundleSchemeReport},
        test_support::test_public_member_keys,
    };
    use futures_util::future;
    use std::sync::Mutex;
    use uuid::Uuid;

    struct RecordingApi {
        local_bundle: PublicKeyBundle,
        assessments: Mutex<Vec<AssessPublicKeyBundleRequest>>,
        feedback: Mutex<Vec<RecordPublicKeyBundleFeedbackRequest>>,
    }

    impl RecordingApi {
        fn new(local_bundle: PublicKeyBundle) -> Self {
            Self {
                local_bundle,
                assessments: Mutex::new(Vec::new()),
                feedback: Mutex::new(Vec::new()),
            }
        }
    }

    impl ReplicationApi for RecordingApi {
        fn diagnostics(&self) -> Arc<dyn flotsync_replication::FlotsyncDiagnostics> {
            panic!("checklist key tests must not request diagnostics")
        }

        fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send + '_>> {
            future::ready(Ok(())).boxed()
        }

        fn local_public_key_bundle(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<PublicKeyBundle, ApiError>> + Send + '_>> {
            future::ready(Ok(self.local_bundle.clone())).boxed()
        }

        fn known_member_keys(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<KnownMemberKeysReport, ApiError>> + Send + '_>>
        {
            panic!("checklist key tests must not enumerate known members")
        }

        fn assess_public_key_bundle(
            &self,
            request: AssessPublicKeyBundleRequest,
        ) -> Pin<Box<dyn Future<Output = Result<PublicKeyBundleReport, ApiError>> + Send + '_>>
        {
            let report = PublicKeyBundleReport {
                fingerprint: request.bundle.fingerprint(),
                schemes: PublicKeyBundleSchemeReport::SUPPORTED,
                globally_blocked: false,
                known_bindings: Vec::new(),
                candidate_members: Vec::new(),
            };
            self.assessments
                .lock()
                .expect("assessment lock should be available")
                .push(request);
            future::ready(Ok(report)).boxed()
        }

        fn record_public_key_bundle_feedback(
            &self,
            request: RecordPublicKeyBundleFeedbackRequest,
        ) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send + '_>> {
            self.feedback
                .lock()
                .expect("feedback lock should be available")
                .push(request);
            future::ready(Ok(())).boxed()
        }

        fn publish_changes(
            &self,
            _request: PublishChangesRequest,
        ) -> Pin<Box<dyn Future<Output = Result<PublishReceipt, ApiError>> + Send + '_>> {
            panic!("checklist key tests must not publish changes")
        }

        fn snapshot_rows(
            &self,
            _request: SnapshotRowsRequest,
        ) -> Pin<Box<dyn Future<Output = Result<SnapshotValueRows, ApiError>> + Send + '_>>
        {
            panic!("checklist key tests must not load snapshots")
        }

        fn request_summary(
            &self,
            _request: SummaryRequest,
        ) -> Pin<Box<dyn Future<Output = Result<Summary, ApiError>> + Send + '_>> {
            panic!("checklist key tests must not request summaries")
        }

        fn create_group(
            &self,
            _request: CreateGroupRequest,
        ) -> Pin<Box<dyn Future<Output = Result<GroupId, ApiError>> + Send + '_>> {
            panic!("checklist key tests must not create groups")
        }

        fn change_group_membership(
            &self,
            _request: ChangeGroupMembershipRequest,
        ) -> Pin<Box<dyn Future<Output = Result<MigrationId, ApiError>> + Send + '_>> {
            panic!("checklist key tests must not change membership")
        }
    }

    #[test]
    fn trust_assesses_exact_member_before_recording_feedback() {
        let member_id = MemberIdentity::from_array(["bob"]);
        let bundle = test_public_member_keys(&member_id).public_key_bundle();
        let api = RecordingApi::new(bundle.clone());
        let command = ChecklistKeyCommand::Trust {
            member_id: member_id.clone(),
            public_bundle: bundle.to_pasteable_string(),
        };
        let mut confirm_after_assessment = |_: &str| {
            assert_eq!(
                api.assessments
                    .lock()
                    .expect("assessment lock should be available")
                    .len(),
                1
            );
            Ok(true)
        };

        block_on(run_runtime_key_command_with_confirmation(
            &api,
            command,
            &mut confirm_after_assessment,
        ))
        .expect("trust command should succeed");

        let assessments = api
            .assessments
            .lock()
            .expect("assessment lock should be available");
        assert_eq!(
            assessments[0].candidate_member_ids,
            HashSet::from([member_id.clone()])
        );
        assert_eq!(
            assessments[0].material_storage,
            PublicKeyBundleAssessmentStorage::ReadOnly
        );
        let feedback = api
            .feedback
            .lock()
            .expect("feedback lock should be available");
        assert_eq!(feedback.len(), 1);
        assert_eq!(feedback[0].bundle, bundle);
        assert_eq!(
            feedback[0].feedback,
            PublicKeyBundleFeedback::TrustMember { member_id }
        );
    }

    #[test]
    fn declined_block_assesses_without_recording_feedback() {
        let member_id = MemberIdentity::from_array(["bob"]);
        let bundle = test_public_member_keys(&member_id).public_key_bundle();
        let api = RecordingApi::new(bundle.clone());
        let command = ChecklistKeyCommand::Block {
            public_bundle: bundle.to_pasteable_string(),
        };
        let mut decline_after_assessment = |_: &str| {
            assert_eq!(
                api.assessments
                    .lock()
                    .expect("assessment lock should be available")
                    .len(),
                1
            );
            Ok(false)
        };

        block_on(run_runtime_key_command_with_confirmation(
            &api,
            command,
            &mut decline_after_assessment,
        ))
        .expect("declined block should succeed without mutation");

        let assessments = api
            .assessments
            .lock()
            .expect("assessment lock should be available");
        assert!(assessments[0].candidate_member_ids.is_empty());
        assert!(
            api.feedback
                .lock()
                .expect("feedback lock should be available")
                .is_empty()
        );
    }

    #[test]
    fn local_initialisation_is_idempotent_and_does_not_create_a_group() {
        let test_id = Uuid::new_v4();
        let test_dir = std::env::temp_dir().join(format!("flotsync-checklist-keys-{test_id}"));
        std::fs::create_dir_all(&test_dir).expect("test directory should be created");
        let config_path = test_dir.join("alice.toml");
        let store_path = test_dir.join("alice.sqlite");
        std::fs::write(
            &config_path,
            format!(
                r#"
                [flotsync.examples.replicated-checklist]
                local-member = "alice"
                store-path = "alice.sqlite"
                store-secret-profile = "unsafe:test-{test_id}"
                "#
            ),
        )
        .expect("test config should be written");

        block_on(init_local_keys(&config_path)).expect("first initialisation should succeed");
        block_on(init_local_keys(&config_path)).expect("second initialisation should reuse keys");

        block_on(async {
            let setup = load_checklist_store_setup(&config_path)
                .await
                .expect("test store should reopen");
            let bundle = load_local_public_key_bundle(
                setup.store.as_ref(),
                &setup.config.local_member,
                &setup.replication_security,
            )
            .await
            .expect("local bundle should load");
            assert!(bundle.is_some());
            let mut transaction = setup
                .store
                .begin_read_transaction()
                .await
                .expect("read transaction should start");
            let bindings = transaction
                .load_member_public_keys_for_member(&setup.config.local_member)
                .await
                .expect("local bindings should load");
            let groups = transaction
                .load_replication_groups()
                .await
                .expect("groups should load");
            transaction
                .release()
                .await
                .expect("read transaction should release");
            assert_eq!(bindings.len(), 1);
            assert!(groups.is_empty());
        });

        std::fs::remove_file(store_path).expect("test store should be removed");
        std::fs::remove_file(config_path).expect("test config should be removed");
        std::fs::remove_dir(test_dir).expect("test directory should be removed");
    }
}

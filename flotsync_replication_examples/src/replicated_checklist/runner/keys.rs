//! Runtime-backed key commands.

use super::*;

/// Run one key command through the already-unlocked replication runtime.
pub(super) async fn run_runtime_key_command(
    replication: &dyn ReplicationApi,
    command: ChecklistKeyCommand,
) -> Result<(), ReplicatedChecklistError> {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut output = io::stdout();
    let mut confirmation = ConfirmationDialog::new(&mut input, &mut output);
    run_runtime_key_command_with_confirmation(replication, command, &mut confirmation).await
}

/// Run one runtime key command with an explicit confirmation dialog.
async fn run_runtime_key_command_with_confirmation(
    replication: &dyn ReplicationApi,
    command: ChecklistKeyCommand,
    confirmation: &mut ConfirmationDialog<'_>,
) -> Result<(), ReplicatedChecklistError> {
    match command {
        ChecklistKeyCommand::ExportLocal => export_local_keys(replication).await,
        ChecklistKeyCommand::Inspect { public_bundle } => {
            inspect_public_bundle(replication, &public_bundle).await
        }
        ChecklistKeyCommand::Trust {
            member_id,
            public_bundle,
        } => trust_public_bundle(replication, member_id, &public_bundle, confirmation).await,
        ChecklistKeyCommand::Block { public_bundle } => {
            block_public_bundle(replication, &public_bundle, confirmation).await
        }
    }
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
    confirmation: &mut ConfirmationDialog<'_>,
) -> Result<(), ReplicatedChecklistError> {
    let bundle = decode_pasteable_public_bundle(public_bundle)?;
    let report = assess_public_bundle(
        replication,
        bundle.clone(),
        HashSet::from([member_id.clone()]),
    )
    .await?;
    println!("{report}");
    if !confirmation.confirm(&format!("Trust this bundle for member {member_id}?"))? {
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
    confirmation: &mut ConfirmationDialog<'_>,
) -> Result<(), ReplicatedChecklistError> {
    let bundle = decode_pasteable_public_bundle(public_bundle)?;
    let report = assess_public_bundle(replication, bundle.clone(), HashSet::new()).await?;
    println!("{report}");
    if !confirmation.confirm("Block this bundle fingerprint globally?")? {
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
    use std::{io::Cursor, sync::Mutex};

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

        fn group_state(
            &self,
        ) -> Result<Arc<dyn flotsync_replication::ReplicationGroupSnapshot>, ApiError> {
            panic!("checklist key tests must not request group state")
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
        let mut input = Cursor::new(b"yes\n".as_slice());
        let mut output = Vec::new();
        let mut confirmation = ConfirmationDialog::new(&mut input, &mut output);

        block_on(run_runtime_key_command_with_confirmation(
            &api,
            command,
            &mut confirmation,
        ))
        .expect("trust command should succeed");

        assert_eq!(
            String::from_utf8(output).expect("confirmation prompt should be UTF-8"),
            "Trust this bundle for member bob? [y/N] "
        );

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
        let mut input = Cursor::new(b"no\n".as_slice());
        let mut output = Vec::new();
        let mut confirmation = ConfirmationDialog::new(&mut input, &mut output);

        block_on(run_runtime_key_command_with_confirmation(
            &api,
            command,
            &mut confirmation,
        ))
        .expect("declined block should succeed without mutation");

        assert_eq!(
            String::from_utf8(output).expect("confirmation prompt should be UTF-8"),
            "Block this bundle fingerprint globally? [y/N] "
        );

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
}

//! Store-native key setup commands for the replicated checklist runner.

use super::{
    setup::load_checklist_store_setup,
    static_group::{initialise_configured_group_if_ready, print_static_group_status},
    *,
};

/// Run one pre-runtime key management command.
pub(super) async fn run_key_command(
    command: ReplicatedChecklistKeyCommand,
) -> Result<(), ReplicatedChecklistError> {
    match command {
        ReplicatedChecklistKeyCommand::InitLocal { config } => init_local_keys(&config).await,
        ReplicatedChecklistKeyCommand::ExportLocal { config } => export_local_keys(&config).await,
        ReplicatedChecklistKeyCommand::Inspect {
            config,
            public_bundle,
        } => inspect_public_bundle(&config, &public_bundle).await,
        ReplicatedChecklistKeyCommand::Trust {
            config,
            member_id,
            public_bundle,
        } => trust_public_bundle(&config, &member_id, &public_bundle).await,
        ReplicatedChecklistKeyCommand::Block {
            config,
            fingerprint,
        } => block_fingerprint(&config, fingerprint).await,
    }
}

/// Create local identity keys if absent and print the local public bundle.
async fn init_local_keys(config_path: &Path) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path)?;
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
        false,
    )
    .await?;
    if created {
        println!("local identity keys: created");
    } else {
        println!("local identity keys: existing");
    }
    print_local_public_bundle(&setup.config.local_member, &bundle);
    let group_status = initialise_configured_group_if_ready(
        setup.store.as_ref(),
        &setup.config,
        &setup.replication_security,
    )
    .await
    .context(repl_error::StaticGroupSnafu)?;
    print_static_group_status(&group_status);
    Ok(())
}

/// Print the local public bundle from encrypted local-key storage.
async fn export_local_keys(config_path: &Path) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path)?;
    let bundle = load_local_public_key_bundle(
        setup.store.as_ref(),
        &setup.config.local_member,
        &setup.replication_security,
    )
    .await
    .context(repl_error::ProvisionSecuritySnafu {
        action: "loading local identity keys",
    })?
    .ok_or_else(|| ReplicatedChecklistError::MissingLocalIdentityKeys {
        member_id: setup.config.local_member.clone(),
    })?;
    print_local_public_bundle(&setup.config.local_member, &bundle);
    Ok(())
}

/// Print security state for one pasted public key bundle without writing trust.
async fn inspect_public_bundle(
    config_path: &Path,
    public_bundle: &str,
) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path)?;
    let bundle = decode_pasteable_public_bundle(public_bundle)?;
    let report =
        public_bundle_inspection_report(setup.store.as_ref(), &setup.config, &bundle).await?;
    print_public_bundle_inspection(&report);
    Ok(())
}

/// Trust one pasted public key bundle for one exact member identity.
async fn trust_public_bundle(
    config_path: &Path,
    member_id: &MemberIdentity,
    public_bundle: &str,
) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path)?;
    let bundle = decode_pasteable_public_bundle(public_bundle)?;
    let fingerprint = bundle.fingerprint();
    let warnings =
        store_trusted_public_key_binding(setup.store.as_ref(), member_id, &bundle).await?;
    println!("trusted member: {member_id}");
    print_fingerprint("trusted fingerprint", fingerprint);
    for warning in warnings {
        println!("warning: {warning}");
    }
    let group_status = initialise_configured_group_if_ready(
        setup.store.as_ref(),
        &setup.config,
        &setup.replication_security,
    )
    .await
    .context(repl_error::StaticGroupSnafu)?;
    print_static_group_status(&group_status);
    Ok(())
}

/// Record one globally blocked key fingerprint.
async fn block_fingerprint(
    config_path: &Path,
    fingerprint: KeyFingerprint,
) -> Result<(), ReplicatedChecklistError> {
    let setup = load_checklist_store_setup(config_path)?;
    let mut transaction = setup
        .store
        .begin_transaction()
        .await
        .context(repl_error::StoreSnafu)?;
    transaction
        .ensure_blocked_key_fingerprint(fingerprint)
        .await
        .context(repl_error::StoreSnafu)?;
    transaction.commit().await.context(repl_error::StoreSnafu)?;
    println!("blocked fingerprint recorded");
    print_fingerprint("blocked fingerprint", fingerprint);
    Ok(())
}

/// Decode pasteable public bundle text for command handlers.
fn decode_pasteable_public_bundle(
    input: &str,
) -> Result<PublicKeyBundle, ReplicatedChecklistError> {
    PublicKeyBundle::from_pasteable_string(input).context(repl_error::SecuritySnafu {
        action: "decoding public key bundle",
    })
}

/// Store one observed public-key binding, optionally with local explicit trust.
pub(super) async fn store_observed_public_key_binding(
    store: &dyn ReplicationStore,
    member_id: MemberIdentity,
    bundle: &PublicKeyBundle,
    trusted: bool,
) -> Result<(), ReplicatedChecklistError> {
    let public_keys = bundle.clone().bind_member(member_id);
    let record = MemberPublicKeysRecord::from_public_keys(&public_keys);
    let key_id = record.key_id.clone();
    let mut transaction = store
        .begin_transaction()
        .await
        .context(repl_error::StoreSnafu)?;
    transaction
        .ensure_member_public_keys(record)
        .await
        .context(repl_error::StoreSnafu)?;
    if trusted {
        transaction
            .ensure_member_key_trust_evidence(MemberKeyTrustEvidenceRecord {
                key_id,
                evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
            })
            .await
            .context(repl_error::StoreSnafu)?;
    }
    transaction.commit().await.context(repl_error::StoreSnafu)?;
    Ok(())
}

/// Trust one public bundle for an exact member and return local-state warnings.
pub(super) async fn store_trusted_public_key_binding(
    store: &dyn ReplicationStore,
    member_id: &MemberIdentity,
    bundle: &PublicKeyBundle,
) -> Result<Vec<String>, ReplicatedChecklistError> {
    let fingerprint = bundle.fingerprint();
    let mut transaction = store
        .begin_transaction()
        .await
        .context(repl_error::StoreSnafu)?;
    let fingerprint_blocked = transaction
        .is_key_fingerprint_blocked(&fingerprint)
        .await
        .context(repl_error::StoreSnafu)?;
    if fingerprint_blocked {
        return Err(ReplicatedChecklistError::BlockedKeyTrust {
            member_id: member_id.clone(),
            fingerprint,
        });
    }

    let mut warnings = Vec::new();
    let existing_for_fingerprint = transaction
        .load_member_public_keys_for_fingerprint(&fingerprint)
        .await
        .context(repl_error::StoreSnafu)?;
    for record in existing_for_fingerprint {
        if record.key_id.member_id != *member_id {
            warnings.push(format!(
                "fingerprint is already bound to member {}",
                record.key_id.member_id
            ));
        }
    }
    let existing_for_member = transaction
        .load_member_public_keys_for_member(member_id)
        .await
        .context(repl_error::StoreSnafu)?;
    for record in existing_for_member {
        if record.key_id.fingerprint != fingerprint {
            warnings.push(format!(
                "member {member_id} already has known fingerprint {}",
                record.key_id.fingerprint
            ));
        }
    }

    let public_keys = bundle.clone().bind_member(member_id.clone());
    let record = MemberPublicKeysRecord::from_public_keys(&public_keys);
    let key_id = record.key_id.clone();
    transaction
        .ensure_member_public_keys(record)
        .await
        .context(repl_error::StoreSnafu)?;
    transaction
        .ensure_member_key_trust_evidence(MemberKeyTrustEvidenceRecord {
            key_id,
            evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
        })
        .await
        .context(repl_error::StoreSnafu)?;
    transaction.commit().await.context(repl_error::StoreSnafu)?;
    Ok(warnings)
}

/// Local report printed by `keys inspect`.
pub(super) struct PublicBundleInspectionReport {
    pub(super) fingerprint: KeyFingerprint,
    pub(super) globally_blocked: bool,
    pub(super) bindings: Vec<KeyBindingInspection>,
    pub(super) configured_member_candidates: Vec<ConfiguredMemberCandidateState>,
}

/// Security state for one exact stored binding.
pub(super) struct KeyBindingInspection {
    pub(super) key_id: MemberKeyId,
    pub(super) has_local_explicit_trust: bool,
    pub(super) authority: Vec<(AuthorityScope, PermissionDecision)>,
}

/// Configured member state for fingerprints other than the inspected bundle.
pub(super) struct ConfiguredMemberCandidateState {
    pub(super) member_id: MemberIdentity,
    pub(super) other_fingerprints: Vec<KeyFingerprint>,
}

/// Build an inspect report for one public bundle.
pub(super) async fn public_bundle_inspection_report(
    store: &dyn ReplicationStore,
    config: &ChecklistAppConfig,
    bundle: &PublicKeyBundle,
) -> Result<PublicBundleInspectionReport, ReplicatedChecklistError> {
    let fingerprint = bundle.fingerprint();
    let trust_policy = TrustPolicy::default();
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(repl_error::StoreSnafu)?;
    let globally_blocked = transaction
        .is_key_fingerprint_blocked(&fingerprint)
        .await
        .context(repl_error::StoreSnafu)?;
    let records = transaction
        .load_member_public_keys_for_fingerprint(&fingerprint)
        .await
        .context(repl_error::StoreSnafu)?;
    let mut bindings = Vec::with_capacity(records.len());
    for record in records {
        decode_public_keys_record(&record)?;
        let evidence = transaction
            .load_member_key_trust_evidence(&record.key_id)
            .await
            .context(repl_error::StoreSnafu)?;
        let has_local_explicit_trust =
            evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust);
        let authority = AuthorityScope::VALUES
            .iter()
            .map(|scope| {
                (
                    *scope,
                    permission_decision(
                        &trust_policy,
                        *scope,
                        has_local_explicit_trust,
                        globally_blocked,
                    ),
                )
            })
            .collect();
        bindings.push(KeyBindingInspection {
            key_id: record.key_id,
            has_local_explicit_trust,
            authority,
        });
    }

    let mut configured_member_candidates = Vec::new();
    for member_id in &config.ordered_members {
        let records = transaction
            .load_member_public_keys_for_member(member_id)
            .await
            .context(repl_error::StoreSnafu)?;
        let other_fingerprints = records
            .into_iter()
            .filter_map(|record| {
                if record.key_id.fingerprint == fingerprint {
                    None
                } else {
                    Some(record.key_id.fingerprint)
                }
            })
            .collect::<Vec<_>>();
        if !other_fingerprints.is_empty() {
            configured_member_candidates.push(ConfiguredMemberCandidateState {
                member_id: member_id.clone(),
                other_fingerprints,
            });
        }
    }
    transaction
        .release()
        .await
        .context(repl_error::StoreSnafu)?;

    Ok(PublicBundleInspectionReport {
        fingerprint,
        globally_blocked,
        bindings,
        configured_member_candidates,
    })
}

/// Decode and validate a public-key store record.
fn decode_public_keys_record(
    record: &MemberPublicKeysRecord,
) -> Result<PublicMemberKeys, ReplicatedChecklistError> {
    PublicMemberKeys::from_key_bytes(
        record.key_id.member_id.clone(),
        record.signing_public_key.as_ref(),
        record.encryption_public_key.as_ref(),
    )
    .context(repl_error::SecuritySnafu {
        action: "decoding stored public keys",
    })
}

/// Evaluate permission with the current first-slice trust policy.
fn permission_decision(
    policy: &TrustPolicy,
    scope: AuthorityScope,
    has_local_explicit_trust: bool,
    fingerprint_blocked: bool,
) -> PermissionDecision {
    if fingerprint_blocked {
        return PermissionDecision::Deny(PermissionDenialReason::FingerprintBlocked);
    }
    let requirement = match scope {
        AuthorityScope::ReplicationRuntime => policy.replication_runtime,
        AuthorityScope::BootstrapActivation => policy.bootstrap_activation,
        AuthorityScope::MemberRoutePublication => policy.member_route_publication,
    };
    match requirement {
        MemberKeyTrustRequirement::DenyAll => {
            PermissionDecision::Deny(PermissionDenialReason::PolicyDenied)
        }
        MemberKeyTrustRequirement::StoredPublicKeyMaterial => PermissionDecision::Permit,
        MemberKeyTrustRequirement::LocalExplicitTrust => {
            if has_local_explicit_trust {
                PermissionDecision::Permit
            } else {
                PermissionDecision::Deny(PermissionDenialReason::MissingTrustEvidence)
            }
        }
    }
}

/// Print the local public bundle in copy/paste and verification forms.
fn print_local_public_bundle(member_id: &MemberIdentity, bundle: &PublicKeyBundle) {
    println!("member id: {member_id}");
    println!("public bundle (copy this value):");
    println!("{}", bundle.to_pasteable_string());
    println!(
        "canonical fingerprint: {}",
        bundle.fingerprint().to_canonical_base64url()
    );
    println!("display fingerprint: {}", bundle.fingerprint());
}

/// Print one fingerprint in parseable and display forms.
fn print_fingerprint(label: &str, fingerprint: KeyFingerprint) {
    println!("{label}: {}", fingerprint.to_canonical_base64url());
    println!("{label} display: {fingerprint}");
}

/// Print an inspect report.
fn print_public_bundle_inspection(report: &PublicBundleInspectionReport) {
    println!("public key bundle inspection");
    print_fingerprint("fingerprint", report.fingerprint);
    println!("schemes: signing: Ed25519, encryption: X25519 HPKE");
    println!("globally blocked: {}", yes_no(report.globally_blocked));
    if report.bindings.is_empty() {
        println!("known bindings: none");
    } else {
        println!("known bindings:");
        for binding in &report.bindings {
            println!(
                "  - member: {}, fingerprint: {}, local explicit trust: {}",
                binding.key_id.member_id,
                binding.key_id.fingerprint,
                yes_no(binding.has_local_explicit_trust)
            );
            for (scope, decision) in &binding.authority {
                println!("    authority {scope}: {decision}");
            }
        }
    }
    if report.configured_member_candidates.is_empty() {
        println!("configured member candidate state: no other known fingerprints");
    } else {
        println!("configured member candidate state:");
        for candidate in &report.configured_member_candidates {
            let fingerprints = joined_fingerprints(candidate.other_fingerprints.iter().copied());
            println!(
                "  - member {} has other known fingerprints: {fingerprints}",
                candidate.member_id
            );
        }
    }
}

/// Format fingerprints for compact command output.
fn joined_fingerprints(fingerprints: impl IntoIterator<Item = KeyFingerprint>) -> String {
    fingerprints
        .into_iter()
        .map(|fingerprint| fingerprint.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Human-friendly bool rendering for command output.
const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

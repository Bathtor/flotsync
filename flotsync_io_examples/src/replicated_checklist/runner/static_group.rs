//! Static-group setup bridge for the replicated checklist runner.

use super::*;

/// Result of trying to initialise the configured static group.
pub(super) enum StaticGroupSetupStatus {
    Existing,
    Initialised,
    Pending(Vec<String>),
}

/// Static group shape derived from checklist config.
struct ConfiguredStaticGroup {
    local_member_index: MemberIndex,
    member_count: NonZeroUsize,
}

/// Return the fixed dataset schema for configured checklist groups.
fn checklist_group_schema() -> GroupSchema {
    GroupSchema::new(HashMap::from([(
        checklist_dataset_id(),
        CHECKLIST_SCHEMA.clone().into(),
    )]))
}

/// Derive the configured static group shape shared by run and key setup.
fn configured_static_group(
    config: &ChecklistAppConfig,
) -> Result<ConfiguredStaticGroup, StaticGroupError> {
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
    Ok(ConfiguredStaticGroup {
        local_member_index,
        member_count,
    })
}

/// Insert the configured static group once all member keys are ready.
pub(super) async fn initialise_configured_group_if_ready(
    store: &dyn ReplicationStore,
    config: &ChecklistAppConfig,
    replication_security: &ReplicationSecuritySecrets,
) -> Result<StaticGroupSetupStatus, StaticGroupError> {
    let group_shape = configured_static_group(config)?;
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
        validate_existing_static_group(existing_group, config, group_shape.local_member_index)?;
        validate_existing_static_group_security(existing_group, config, replication_security)?;
        return Ok(StaticGroupSetupStatus::Existing);
    }

    let member_keys =
        match load_ready_configured_member_keys(store, config, replication_security).await? {
            Ok(member_keys) => member_keys,
            Err(pending) => return Ok(StaticGroupSetupStatus::Pending(pending)),
        };
    let security_material = prepare_initial_group_security_material(
        config.group_id,
        replication_security,
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
            member_keys,
            local_member_index: group_shape.local_member_index,
            version_vector: VersionVector::initial(group_shape.member_count),
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material,
            group_schema: checklist_group_schema(),
        })
        .await
        .context(static_group_error::StoreSnafu)?;
    transaction
        .commit()
        .await
        .context(static_group_error::StoreSnafu)?;
    Ok(StaticGroupSetupStatus::Initialised)
}

/// Load exact configured member keys when every configured member is ready.
async fn load_ready_configured_member_keys(
    store: &dyn ReplicationStore,
    config: &ChecklistAppConfig,
    replication_security: &ReplicationSecuritySecrets,
) -> Result<Result<GroupMemberKeys, Vec<String>>, StaticGroupError> {
    let local_bundle =
        load_local_public_key_bundle(store, &config.local_member, replication_security)
            .await
            .context(static_group_error::LocalIdentityKeysSnafu)?;
    let mut transaction = store
        .begin_read_transaction()
        .await
        .context(static_group_error::StoreSnafu)?;
    let mut ordered_member_keys = Vec::with_capacity(config.ordered_members.len());
    let mut pending = Vec::new();
    for member_id in &config.ordered_members {
        if member_id == &config.local_member {
            match &local_bundle {
                Some(bundle) => {
                    let fingerprint = bundle.fingerprint();
                    let fingerprint_blocked = transaction
                        .is_key_fingerprint_blocked(&fingerprint)
                        .await
                        .context(static_group_error::StoreSnafu)?;
                    if fingerprint_blocked {
                        pending.push(format!(
                            "local member {member_id} fingerprint is globally blocked: {fingerprint}"
                        ));
                    } else {
                        ordered_member_keys.push(MemberKeyId {
                            member_id: member_id.clone(),
                            fingerprint,
                        });
                    }
                }
                None => pending.push(format!(
                    "local member {member_id} has no local identity keys"
                )),
            }
            continue;
        }

        let records = transaction
            .load_member_public_keys_for_member(member_id)
            .await
            .context(static_group_error::StoreSnafu)?;
        let mut trusted = Vec::new();
        let mut blocked_trusted = Vec::new();
        for record in records {
            let evidence = transaction
                .load_member_key_trust_evidence(&record.key_id)
                .await
                .context(static_group_error::StoreSnafu)?;
            if evidence.contains(MemberKeyTrustEvidenceKind::LocalExplicitTrust) {
                let fingerprint_blocked = transaction
                    .is_key_fingerprint_blocked(&record.key_id.fingerprint)
                    .await
                    .context(static_group_error::StoreSnafu)?;
                if fingerprint_blocked {
                    blocked_trusted.push(record.key_id);
                } else {
                    trusted.push(record.key_id);
                }
            }
        }
        match trusted.as_slice() {
            [] if blocked_trusted.is_empty() => pending.push(format!(
                "member {member_id} has no trusted public key bundle"
            )),
            [] => {
                let fingerprints =
                    joined_fingerprints(blocked_trusted.iter().map(|key_id| key_id.fingerprint));
                pending.push(format!(
                    "member {member_id} has no unblocked trusted public key bundle; blocked trusted fingerprints: {fingerprints}"
                ));
            }
            [key_id] => ordered_member_keys.push(key_id.clone()),
            keys => {
                let fingerprints =
                    joined_fingerprints(keys.iter().map(|key_id| key_id.fingerprint));
                pending.push(format!(
                    "member {member_id} has multiple trusted fingerprints: {fingerprints}"
                ));
            }
        }
    }
    transaction
        .release()
        .await
        .context(static_group_error::StoreSnafu)?;
    if !pending.is_empty() {
        return Ok(Err(pending));
    }
    GroupMemberKeys::from_ordered_member_keys(ordered_member_keys)
        .context(static_group_error::InvalidMembersSnafu)
        .map(Ok)
}

/// Print the static-group setup result after a key command.
pub(super) fn print_static_group_status(status: &StaticGroupSetupStatus) {
    match status {
        StaticGroupSetupStatus::Existing => println!("static group: already initialised"),
        StaticGroupSetupStatus::Initialised => println!("static group: initialised"),
        StaticGroupSetupStatus::Pending(reasons) => {
            println!("static group: pending");
            for reason in reasons {
                println!("  - {reason}");
            }
        }
    }
}

/// Format fingerprints for compact static-group status messages.
fn joined_fingerprints(fingerprints: impl IntoIterator<Item = KeyFingerprint>) -> String {
    fingerprints
        .into_iter()
        .map(|fingerprint| fingerprint.to_string())
        .collect::<Vec<_>>()
        .join(", ")
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
    #[snafu(display("Failed to prepare initial static group security material: {source}"))]
    InitialGroupSecurity { source: ProvisionSecurityError },
    #[snafu(display("Failed to load local identity keys while preparing static group: {source}"))]
    LocalIdentityKeys { source: ProvisionSecurityError },
    #[snafu(display(
        "Stored static group {group_id} security material does not match checklist config: {source}"
    ))]
    ExistingGroupSecurity {
        group_id: GroupId,
        source: ProvisionSecurityError,
    },
    /// Key setup commands must provision missing static-group metadata before runtime starts.
    #[snafu(display(
        "Checklist key setup is incomplete. Run keys init-local <config>, then keys trust <config> <member-id> <public-bundle> for each configured remote member."
    ))]
    KeySetupRequired,
}

/// Ensure the store contains either no group or exactly the configured static group.
///
/// When no group exists this returns [`StaticGroupError::KeySetupRequired`].
/// When one group exists it must match the configured id, member order, and
/// local member index.
pub(super) async fn ensure_configured_group(
    store: &dyn ReplicationStore,
    config: &ChecklistAppConfig,
    replication_security: &ReplicationSecuritySecrets,
) -> Result<(), StaticGroupError> {
    let group_shape = configured_static_group(config)?;
    let local_bundle =
        load_local_public_key_bundle(store, &config.local_member, replication_security)
            .await
            .context(static_group_error::LocalIdentityKeysSnafu)?;
    if local_bundle.is_none() {
        return static_group_error::KeySetupRequiredSnafu.fail();
    }

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
        validate_existing_static_group(existing_group, config, group_shape.local_member_index)?;
        validate_existing_static_group_security(existing_group, config, replication_security)?;
        return Ok(());
    }

    static_group_error::KeySetupRequiredSnafu.fail()
}

/// Validate an existing static group's encrypted key against current config.
fn validate_existing_static_group_security(
    existing_group: &ReplicationGroupRecord,
    config: &ChecklistAppConfig,
    replication_security: &ReplicationSecuritySecrets,
) -> Result<(), StaticGroupError> {
    validate_initial_group_security_material(
        existing_group.group_id,
        replication_security,
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

/// Validate the existing static group before runtime loading checks member key records.
fn validate_existing_static_group(
    existing_group: &ReplicationGroupRecord,
    config: &ChecklistAppConfig,
    local_member_index: MemberIndex,
) -> Result<(), StaticGroupError> {
    let actual_members = existing_group.member_ids().cloned().collect::<Vec<_>>();
    ensure!(
        existing_group.group_id == config.group_id,
        static_group_error::UnexpectedGroupSnafu {
            configured_group_id: config.group_id,
            actual_group_id: existing_group.group_id,
        }
    );
    ensure!(
        actual_members.as_slice() == config.ordered_members.as_slice(),
        static_group_error::MemberMismatchSnafu {
            group_id: config.group_id,
            expected: config.ordered_members.clone(),
            actual: actual_members,
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

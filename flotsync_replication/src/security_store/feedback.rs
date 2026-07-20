//! Public-key bundle storage and feedback handling for the security store.

use super::*;

/// Store one observed public-key binding for each candidate member identity.
pub(super) async fn ensure_candidate_public_key_bindings(
    transaction: &mut dyn ReplicationStoreTransaction,
    bundle: &PublicKeyBundle,
    candidate_member_ids: &HashSet<MemberIdentity>,
) -> Result<(), SecurityStoreError> {
    for member_id in candidate_member_ids {
        ensure_candidate_public_key_binding(transaction, bundle, member_id.clone()).await?;
    }
    Ok(())
}

/// Store one observed public-key binding for a candidate member identity.
async fn ensure_candidate_public_key_binding(
    transaction: &mut dyn ReplicationStoreTransaction,
    bundle: &PublicKeyBundle,
    member_id: MemberIdentity,
) -> Result<(), SecurityStoreError> {
    let record = member_public_keys_record_from_bundle(bundle, member_id);
    transaction
        .ensure_member_public_keys(record)
        .await
        .context(StoreAccessSnafu)?;
    Ok(())
}

/// Ensure one directly discovered public-key binding when exact material is still missing.
pub(super) async fn ensure_discovery_public_key_binding(
    transaction: &mut dyn ReplicationStoreTransaction,
    bundle: &PublicKeyBundle,
    member_id: MemberIdentity,
) -> Result<(), SecurityStoreError> {
    let fingerprint = bundle.fingerprint();
    let key_id = MemberKeyId {
        member_id: member_id.clone(),
        fingerprint,
    };
    let existing = transaction
        .load_member_public_keys(&key_id)
        .await
        .context(StoreAccessSnafu)?;
    if existing.is_some() {
        return Ok(());
    }
    let fingerprint_blocked = transaction
        .is_key_fingerprint_blocked(&fingerprint)
        .await
        .context(StoreAccessSnafu)?;
    if fingerprint_blocked {
        return Err(SecurityStoreError::ExactMemberKeyPublicKeysUnavailable {
            key_id,
            denial_reasons: vec![PermissionDenialReason::FingerprintBlocked],
        });
    }
    ensure_candidate_public_key_binding(transaction, bundle, member_id).await
}

/// Persist one feedback action in an already-open write transaction.
pub(super) async fn record_public_key_bundle_feedback_in_transaction(
    transaction: &mut dyn ReplicationStoreTransaction,
    bundle: &PublicKeyBundle,
    feedback: PublicKeyBundleFeedback,
    fingerprint: KeyFingerprint,
) -> Result<(), SecurityStoreError> {
    match feedback {
        PublicKeyBundleFeedback::TrustMember { member_id } => {
            let fingerprint_blocked = transaction
                .is_key_fingerprint_blocked(&fingerprint)
                .await
                .context(StoreAccessSnafu)?;
            ensure!(
                !fingerprint_blocked,
                BlockedKeyTrustSnafu {
                    member_id: member_id.clone(),
                    fingerprint,
                }
            );
            let record = member_public_keys_record_from_bundle(bundle, member_id);
            let key_id = record.key_id.clone();
            transaction
                .ensure_member_public_keys(record)
                .await
                .context(StoreAccessSnafu)?;
            transaction
                .ensure_member_key_trust_evidence(MemberKeyTrustEvidenceRecord {
                    key_id: key_id.clone(),
                    evidence_kind: MemberKeyTrustEvidenceKind::LocalExplicitTrust,
                })
                .await
                .context(StoreAccessSnafu)?;
            Ok(())
        }
        PublicKeyBundleFeedback::BlockFingerprint => {
            transaction
                .ensure_blocked_key_fingerprint(fingerprint)
                .await
                .context(StoreAccessSnafu)?;
            Ok(())
        }
    }
}

/// Build a member-public-keys store record by binding one public bundle to a member.
fn member_public_keys_record_from_bundle(
    bundle: &PublicKeyBundle,
    member_id: MemberIdentity,
) -> MemberPublicKeysRecord {
    let public_keys = bundle.clone().bind_member(member_id);
    MemberPublicKeysRecord::from_public_keys(&public_keys)
}

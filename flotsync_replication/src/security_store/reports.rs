//! Public-key bundle assessment reports for the security store.

use super::*;
use flotsync_core::member::TrieMap;

/// Build the known-member trust report from an already-open store transaction.
pub(super) async fn known_member_keys_report_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
) -> Result<KnownMemberKeysReport, SecurityStoreError> {
    let key_ids = transaction
        .load_member_public_key_ids()
        .await
        .context(StoreAccessSnafu)?;
    let mut keys_by_member: TrieMap<Vec<KnownMemberKeyReport>> = TrieMap::new();
    for key_id in key_ids {
        let trust = member_key_trust_report_from_transaction(transaction, &key_id).await?;
        let key = KnownMemberKeyReport {
            fingerprint: key_id.fingerprint,
            trust,
        };
        if let Some(keys) = keys_by_member.get_mut(&key_id.member_id) {
            keys.push(key);
        } else {
            keys_by_member.insert(key_id.member_id, vec![key]);
        }
    }
    let members = keys_by_member
        .into_iter()
        .map(|(member_id, keys)| KnownMemberReport { member_id, keys })
        .collect();
    Ok(KnownMemberKeysReport { members })
}

/// Build a public bundle report from an already-open store transaction.
pub(super) async fn public_key_bundle_report_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    fingerprint: KeyFingerprint,
    candidate_member_ids: HashSet<MemberIdentity>,
) -> Result<PublicKeyBundleReport, SecurityStoreError> {
    let globally_blocked = transaction
        .is_key_fingerprint_blocked(&fingerprint)
        .await
        .context(StoreAccessSnafu)?;
    let known_records = transaction
        .load_member_public_keys_for_fingerprint(&fingerprint)
        .await
        .context(StoreAccessSnafu)?;
    let mut known_bindings = Vec::with_capacity(known_records.len());
    for record in known_records {
        public_keys_from_record(&record)?;
        let report = member_key_binding_report_from_transaction(
            transaction,
            policy,
            &record.key_id,
            globally_blocked,
        )
        .await?;
        known_bindings.push(report);
    }

    let mut candidate_member_ids = candidate_member_ids.into_iter().collect::<Vec<_>>();
    candidate_member_ids.sort();
    let mut candidate_members = Vec::with_capacity(candidate_member_ids.len());
    for member_id in candidate_member_ids {
        let candidate_report = candidate_member_key_report_from_transaction(
            transaction,
            policy,
            member_id,
            fingerprint,
            globally_blocked,
        )
        .await?;
        candidate_members.push(candidate_report);
    }

    Ok(PublicKeyBundleReport {
        fingerprint,
        schemes: PublicKeyBundleSchemeReport::SUPPORTED,
        globally_blocked,
        known_bindings,
        candidate_members,
    })
}

/// Build a report for one candidate identity supplied by the application.
async fn candidate_member_key_report_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    member_id: MemberIdentity,
    fingerprint: KeyFingerprint,
    globally_blocked: bool,
) -> Result<CandidateMemberKeyReport, SecurityStoreError> {
    let records = transaction
        .load_member_public_keys_for_member(&member_id)
        .await
        .context(StoreAccessSnafu)?;
    let mut binding_for_bundle = None;
    let mut other_known_fingerprints = Vec::new();
    for record in records {
        public_keys_from_record(&record)?;
        if record.key_id.fingerprint == fingerprint {
            let report = member_key_binding_report_from_transaction(
                transaction,
                policy,
                &record.key_id,
                globally_blocked,
            )
            .await?;
            binding_for_bundle = Some(report);
        } else {
            other_known_fingerprints.push(record.key_id.fingerprint);
        }
    }
    Ok(CandidateMemberKeyReport {
        member_id,
        binding_for_bundle,
        other_known_fingerprints,
    })
}

/// Build a local trust and authority report for one exact member-key binding.
async fn member_key_binding_report_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    policy: &TrustPolicy,
    key_id: &MemberKeyId,
    globally_blocked: bool,
) -> Result<MemberKeyBindingReport, SecurityStoreError> {
    let evidence = transaction
        .load_member_key_trust_evidence(key_id)
        .await
        .context(StoreAccessSnafu)?;
    let trust = evidence.into();
    let authority = AuthorityScope::VALUES
        .iter()
        .map(|scope| MemberKeyAuthorityReport {
            scope: *scope,
            decision: request_member_key_permission(
                policy,
                *scope,
                key_id,
                evidence,
                globally_blocked,
            ),
        })
        .collect();
    Ok(MemberKeyBindingReport {
        key_id: key_id.clone(),
        trust,
        authority,
    })
}

/// Load the local trust summary for one exact member-key binding.
async fn member_key_trust_report_from_transaction(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    key_id: &MemberKeyId,
) -> Result<MemberKeyTrustReport, SecurityStoreError> {
    let evidence = transaction
        .load_member_key_trust_evidence(key_id)
        .await
        .context(StoreAccessSnafu)?;
    Ok(evidence.into())
}

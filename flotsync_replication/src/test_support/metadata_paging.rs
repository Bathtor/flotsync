//! Reusable store-backend contract scenarios for metadata pagination.

use crate::api::{
    MemberKeyTrustEvidenceRecord,
    MemberPublicKeyPredicate,
    MemberPublicKeysRecord,
    PageCursor,
    PendingGroupActivationRecord,
    PendingGroupDecisionRecord,
    ReplicationGroupPredicate,
    ReplicationGroupRecord,
    ReplicationStore,
    ReplicationStoreReadTransaction,
    StoreError,
    VecPageBatch,
    WritableReplicationGroupVersionRecord,
};
use flotsync_core::{GroupId, MemberIdentity};
use flotsync_security::KeyFingerprint;
use flotsync_utils::testing::assert_unordered_eq;
use itertools::Itertools as _;
use std::{collections::HashSet, num::NonZeroUsize};

/// Expected contents of a freshly prepared store used for metadata paging checks.
///
/// Backends prepare these records through their public write transactions before
/// calling [`assert_metadata_paging_contract`]. The fixtures must contain at
/// least three groups and member-key ids, two pending decisions and activations,
/// one trust-evidence record, two public keys for one member, two public keys
/// sharing one fingerprint, and member identities whose segment ordering
/// differs from their canonical text ordering.
pub struct MetadataPagingFixtures {
    /// Every active replication group expected in the store.
    pub groups: Vec<ReplicationGroupRecord>,
    /// Writable group progress expected in the store.
    pub writable_group_versions: Vec<WritableReplicationGroupVersionRecord>,
    /// Every public-key record expected in the store, including provisioned keys.
    pub member_public_keys: Vec<MemberPublicKeysRecord>,
    /// Trust evidence inserted for exact member-key bindings.
    pub member_key_trust_evidence: Vec<MemberKeyTrustEvidenceRecord>,
    /// Every unresolved pending group decision.
    pub pending_group_decisions: Vec<PendingGroupDecisionRecord>,
    /// Every accepted pending group activation.
    pub pending_group_activations: Vec<PendingGroupActivationRecord>,
    /// Group id which is known to be absent from the prepared store.
    pub missing_group_id: GroupId,
}

/// Verify metadata paging through one backend's public store interface.
///
/// The scenarios cover bounded continuation, exact-size final pages followed by
/// an empty page, filters, missing selected group ids, composite member-key ties,
/// pending-work keys, interleaved cursors on one transaction, changed page
/// limits, and equivalence with complete-result conveniences.
///
/// # Errors
///
/// Returns the backend error raised while opening or releasing the read
/// transaction. Paging failures panic because this function is test support and
/// such a failure is a contract assertion.
///
/// # Panics
///
/// Panics when the fixtures do not provide the documented scenarios or the
/// backend returns results which violate the metadata paging contract.
pub async fn assert_metadata_paging_contract(
    store: &dyn ReplicationStore,
    fixtures: &MetadataPagingFixtures,
) -> Result<(), StoreError> {
    let validated = validate_fixtures(fixtures);
    let mut transaction = store.begin_read_transaction().await?;

    assert_interleaved_group_and_key_paging(transaction.as_mut(), fixtures).await;
    assert_group_predicates(transaction.as_mut(), fixtures).await;
    assert_writable_group_paging(transaction.as_mut(), fixtures).await;
    assert_public_key_predicates(transaction.as_mut(), fixtures, &validated).await;
    assert_trust_evidence_paging(transaction.as_mut(), validated.evidence).await;
    assert_pending_group_paging(transaction.as_mut(), fixtures).await;

    transaction.release().await
}

/// Single-record page used to exercise exact-boundary continuation.
const SINGLE_RECORD_PAGE: NonZeroUsize = NonZeroUsize::new(1).expect("one is non-zero");
/// Two-record page used before rotating to a smaller batch.
const TWO_RECORD_PAGE: NonZeroUsize = NonZeroUsize::new(2).expect("two is non-zero");
/// Maximum calls allowed in one contract scenario before paging is considered stuck.
const MAX_PAGE_CALLS: usize = 64;

/// Fixture values selected once for scenarios that require tied records.
struct ValidatedFixtures<'a> {
    /// One exact binding with recorded trust evidence.
    evidence: &'a MemberKeyTrustEvidenceRecord,
    /// Member identity shared by at least two public-key records.
    tied_member: &'a MemberIdentity,
    /// Fingerprint shared by at least two public-key records.
    tied_fingerprint: KeyFingerprint,
}

/// Validate fixture preconditions and select values used by focused scenarios.
fn validate_fixtures(fixtures: &MetadataPagingFixtures) -> ValidatedFixtures<'_> {
    assert!(
        fixtures.groups.len() >= 3,
        "paging fixtures need three groups"
    );
    assert!(
        fixtures.member_public_keys.len() >= 3,
        "paging fixtures need three public-key records"
    );
    assert!(
        has_member_identity_order_mismatch(&fixtures.member_public_keys),
        "paging fixtures need member identities whose segment and text ordering differ"
    );
    assert!(
        fixtures.pending_group_decisions.len() >= 2,
        "paging fixtures need two pending decisions"
    );
    assert!(
        fixtures.pending_group_activations.len() >= 2,
        "paging fixtures need two pending activations"
    );
    let evidence = fixtures
        .member_key_trust_evidence
        .first()
        .expect("paging fixtures need trust evidence");
    let tied_member = fixtures
        .member_public_keys
        .iter()
        .find_map(|candidate| {
            let count = fixtures
                .member_public_keys
                .iter()
                .filter(|record| record.key_id.member_id == candidate.key_id.member_id)
                .count();
            if count >= 2 {
                Some(&candidate.key_id.member_id)
            } else {
                None
            }
        })
        .expect("paging fixtures need two keys for one member");
    let tied_fingerprint = fixtures
        .member_public_keys
        .iter()
        .find_map(|candidate| {
            let count = fixtures
                .member_public_keys
                .iter()
                .filter(|record| record.key_id.fingerprint == candidate.key_id.fingerprint)
                .count();
            if count >= 2 {
                Some(candidate.key_id.fingerprint)
            } else {
                None
            }
        })
        .expect("paging fixtures need two members sharing one fingerprint");

    ValidatedFixtures {
        evidence,
        tied_member,
        tied_fingerprint,
    }
}

/// Exercise two interleaved cursors, rotated limits, and exact-boundary exhaustion.
async fn assert_interleaved_group_and_key_paging(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixtures: &MetadataPagingFixtures,
) {
    let mut groups_cursor = PageCursor::new(ReplicationGroupPredicate::All);
    let mut groups = Vec::new();
    let mut first_groups_batch = VecPageBatch::bounded(TWO_RECORD_PAGE);
    transaction
        .load_replication_groups_into(&mut groups_cursor, &mut first_groups_batch)
        .await
        .expect("first group page should load");
    assert_eq!(first_groups_batch.values().len(), 2);
    groups.extend(first_groups_batch.into_values());

    let expected_key_ids = fixtures
        .member_public_keys
        .iter()
        .map(|record| record.key_id.clone())
        .collect::<Vec<_>>();
    let mut key_ids_cursor = PageCursor::new(());
    let mut key_ids = Vec::new();
    let mut first_key_ids_batch = VecPageBatch::bounded(TWO_RECORD_PAGE);
    transaction
        .load_member_public_key_ids_into(&mut key_ids_cursor, &mut first_key_ids_batch)
        .await
        .expect("interleaved member-key page should load");
    assert_eq!(first_key_ids_batch.values().len(), 2);
    key_ids.extend(first_key_ids_batch.into_values());

    let mut final_group_page_len = usize::MAX;
    let mut group_page_calls = 1;
    while groups_cursor.has_more() {
        record_page_call(&mut group_page_calls);
        let mut batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
        transaction
            .load_replication_groups_into(&mut groups_cursor, &mut batch)
            .await
            .expect("continued group page should load");
        final_group_page_len = batch.values().len();
        groups.extend(batch.into_values());
    }
    assert_eq!(
        final_group_page_len, 0,
        "exact final group page needs an empty confirmation"
    );
    assert_unordered_eq(&groups, &fixtures.groups);

    let mut final_key_page_len = usize::MAX;
    let mut key_page_calls = 1;
    while key_ids_cursor.has_more() {
        record_page_call(&mut key_page_calls);
        let mut batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
        transaction
            .load_member_public_key_ids_into(&mut key_ids_cursor, &mut batch)
            .await
            .expect("continued member-key page should load");
        final_key_page_len = batch.values().len();
        key_ids.extend(batch.into_values());
    }
    assert_eq!(
        final_key_page_len, 0,
        "exact final key page needs an empty confirmation"
    );
    assert_unordered_eq(&key_ids, &expected_key_ids);

    let complete_groups = transaction
        .load_replication_groups()
        .await
        .expect("complete groups should load through unlimited paging");
    assert_unordered_eq(&complete_groups, &groups);
    let complete_key_ids = transaction
        .load_member_public_key_ids()
        .await
        .expect("complete key ids should load through unlimited paging");
    assert_unordered_eq(&complete_key_ids, &key_ids);
}

/// Exercise non-empty and empty replication-group predicates.
async fn assert_group_predicates(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixtures: &MetadataPagingFixtures,
) {
    let selected_group_ids = HashSet::from([
        fixtures.groups[0].group_id,
        fixtures.groups[2].group_id,
        fixtures.missing_group_id,
    ]);
    let expected = fixtures
        .groups
        .iter()
        .filter(|group| selected_group_ids.contains(&group.group_id))
        .cloned()
        .collect::<Vec<_>>();
    let predicate = ReplicationGroupPredicate::GroupIdIn(&selected_group_ids);
    let mut cursor = PageCursor::new(predicate);
    let mut selected = Vec::new();
    let mut page_calls = 0;
    while cursor.has_more() {
        record_page_call(&mut page_calls);
        let mut batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
        transaction
            .load_replication_groups_into(&mut cursor, &mut batch)
            .await
            .expect("selected group page should load");
        selected.extend(batch.into_values());
    }
    assert_unordered_eq(&selected, &expected);

    let complete = transaction
        .load_replication_groups_for_ids(&selected_group_ids)
        .await
        .expect("complete selected groups should load through unlimited paging");
    assert_unordered_eq(&complete, &selected);

    let empty_group_ids = HashSet::new();
    let predicate = ReplicationGroupPredicate::GroupIdIn(&empty_group_ids);
    let mut empty_cursor = PageCursor::new(predicate);
    let mut empty_batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
    transaction
        .load_replication_groups_into(&mut empty_cursor, &mut empty_batch)
        .await
        .expect("empty group predicate should load");
    assert!(empty_batch.values().is_empty());
    assert!(empty_cursor.is_exhausted());
}

/// Exercise writable-group paging and its complete-result convenience.
async fn assert_writable_group_paging(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixtures: &MetadataPagingFixtures,
) {
    let mut cursor = PageCursor::new(());
    let mut writable = Vec::new();
    let mut page_calls = 0;
    while cursor.has_more() {
        record_page_call(&mut page_calls);
        let mut batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
        transaction
            .load_writable_replication_group_versions_into(&mut cursor, &mut batch)
            .await
            .expect("writable-version page should load");
        writable.extend(batch.into_values());
    }
    assert_unordered_eq(&writable, &fixtures.writable_group_versions);

    let complete = transaction
        .load_writable_replication_group_versions()
        .await
        .expect("complete writable versions should load");
    assert_unordered_eq(&complete, &writable);
}

/// Exercise member and fingerprint public-key predicates.
async fn assert_public_key_predicates(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixtures: &MetadataPagingFixtures,
    validated: &ValidatedFixtures<'_>,
) {
    let expected_member_keys = fixtures
        .member_public_keys
        .iter()
        .filter(|record| &record.key_id.member_id == validated.tied_member)
        .cloned()
        .collect::<Vec<_>>();
    let predicate = MemberPublicKeyPredicate::MemberEq(validated.tied_member);
    let mut member_cursor = PageCursor::new(predicate);
    let member_keys = drain_public_key_pages(transaction, &mut member_cursor).await;
    assert_unordered_eq(&member_keys, &expected_member_keys);
    let complete_member_keys = transaction
        .load_member_public_keys_for_member(validated.tied_member)
        .await
        .expect("complete member public keys should load");
    assert_unordered_eq(&complete_member_keys, &member_keys);

    let expected_fingerprint_keys = fixtures
        .member_public_keys
        .iter()
        .filter(|record| record.key_id.fingerprint == validated.tied_fingerprint)
        .cloned()
        .collect::<Vec<_>>();
    let predicate = MemberPublicKeyPredicate::FingerprintEq(validated.tied_fingerprint);
    let mut fingerprint_cursor = PageCursor::new(predicate);
    let fingerprint_keys = drain_public_key_pages(transaction, &mut fingerprint_cursor).await;
    assert_unordered_eq(&fingerprint_keys, &expected_fingerprint_keys);
    let complete_fingerprint_keys = transaction
        .load_member_public_keys_for_fingerprint(&validated.tied_fingerprint)
        .await
        .expect("complete fingerprint public keys should load");
    assert_unordered_eq(&complete_fingerprint_keys, &fingerprint_keys);
}

/// Exercise exact-binding trust-evidence paging and exhaustion confirmation.
async fn assert_trust_evidence_paging(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    evidence: &MemberKeyTrustEvidenceRecord,
) {
    let mut cursor = PageCursor::new(&evidence.key_id);
    let mut batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
    transaction
        .load_member_key_trust_evidence_into(&mut cursor, &mut batch)
        .await
        .expect("trust-evidence page should load");
    assert_eq!(batch.values(), &[evidence.evidence_kind]);
    assert!(cursor.has_more());
    transaction
        .load_member_key_trust_evidence_into(&mut cursor, &mut batch)
        .await
        .expect("empty trust-evidence page should establish exhaustion");
    assert!(batch.values().is_empty());
    assert!(cursor.is_exhausted());

    let complete = transaction
        .load_member_key_trust_evidence(&evidence.key_id)
        .await
        .expect("complete trust evidence should load");
    assert!(complete.contains(evidence.evidence_kind));
}

/// Exercise pending decision and activation paging and complete conveniences.
async fn assert_pending_group_paging(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    fixtures: &MetadataPagingFixtures,
) {
    let decisions = drain_pending_decision_pages(transaction).await;
    assert_unordered_eq(&decisions, &fixtures.pending_group_decisions);
    let complete_decisions = transaction
        .load_pending_group_decisions()
        .await
        .expect("complete pending decisions should load through unlimited paging");
    assert_unordered_eq(&complete_decisions, &decisions);

    let activations = drain_pending_activation_pages(transaction).await;
    assert_unordered_eq(&activations, &fixtures.pending_group_activations);
    let complete_activations = transaction
        .load_pending_group_activations()
        .await
        .expect("complete pending activations should load through unlimited paging");
    assert_unordered_eq(&complete_activations, &activations);
}

/// Load every public-key page with a one-record maximum.
async fn drain_public_key_pages(
    transaction: &mut dyn ReplicationStoreReadTransaction,
    cursor: &mut PageCursor<MemberPublicKeyPredicate<'_>>,
) -> Vec<MemberPublicKeysRecord> {
    let mut records = Vec::new();
    let mut page_calls = 0;
    while cursor.has_more() {
        record_page_call(&mut page_calls);
        let mut batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
        transaction
            .load_member_public_keys_into(cursor, &mut batch)
            .await
            .expect("public-key page should load");
        records.extend(batch.into_values());
    }
    records
}

/// Load every pending-decision page with a one-record maximum.
async fn drain_pending_decision_pages(
    transaction: &mut dyn ReplicationStoreReadTransaction,
) -> Vec<PendingGroupDecisionRecord> {
    let mut cursor = PageCursor::new(());
    let mut records = Vec::new();
    let mut page_calls = 0;
    while cursor.has_more() {
        record_page_call(&mut page_calls);
        let mut batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
        transaction
            .load_pending_group_decisions_into(&mut cursor, &mut batch)
            .await
            .expect("pending-decision page should load");
        records.extend(batch.into_values());
    }
    records
}

/// Load every pending-activation page with a one-record maximum.
async fn drain_pending_activation_pages(
    transaction: &mut dyn ReplicationStoreReadTransaction,
) -> Vec<PendingGroupActivationRecord> {
    let mut cursor = PageCursor::new(());
    let mut records = Vec::new();
    let mut page_calls = 0;
    while cursor.has_more() {
        record_page_call(&mut page_calls);
        let mut batch = VecPageBatch::bounded(SINGLE_RECORD_PAGE);
        transaction
            .load_pending_group_activations_into(&mut cursor, &mut batch)
            .await
            .expect("pending-activation page should load");
        records.extend(batch.into_values());
    }
    records
}

/// Count one page call and fail promptly when a cursor cannot terminate.
fn record_page_call(page_calls: &mut usize) {
    *page_calls += 1;
    assert!(
        *page_calls <= MAX_PAGE_CALLS,
        "metadata paging did not terminate within {MAX_PAGE_CALLS} calls"
    );
}

/// Return whether the records exercise distinct structured and textual member ordering.
fn has_member_identity_order_mismatch(records: &[MemberPublicKeysRecord]) -> bool {
    records.iter().tuple_combinations().any(|(left, right)| {
        let structured_order = left.key_id.member_id.cmp(&right.key_id.member_id);
        let left_text = left.key_id.member_id.to_string();
        let right_text = right.key_id.member_id.to_string();
        let text_order = left_text.cmp(&right_text);
        structured_order != text_order
    })
}

//! Replication store records and transaction contracts.

use super::*;
use futures_util::FutureExt;
use snafu::{Snafu, ensure};

/// Borrowed group, dataset, and schema arguments for one schema-aware store operation.
#[derive(Clone, Copy, Debug)]
pub struct GroupDatasetSchemaRef<'a> {
    /// Replication group that owns the dataset.
    pub group_id: &'a GroupId,
    /// Dataset governed by `schema` within `group_id`.
    pub dataset_id: &'a DatasetId,
    /// Schema used to encode or decode the dataset rows.
    pub schema: &'a Schema,
}

/// One row-granular dataset view loaded for a single transaction.
///
/// If `dataset_exists` is `true`, the dataset entry already exists for
/// `(group_id, dataset_id)`, even when every requested row key maps to
/// `None`. If `dataset_exists` is `false`, the dataset itself has not been
/// initialised in the group yet, so every requested key is absent because the
/// dataset is absent. Callers can then decide whether to seed an empty
/// in-memory working set from the application schema.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRowStateSlice {
    /// Replication group that owns this dataset slice.
    pub group_id: GroupId,
    /// Dataset identifier within the replication group.
    pub dataset_id: DatasetId,
    /// Whether this dataset already exists in the store for `group_id`.
    pub dataset_exists: bool,
    /// Stored state for each requested row key.
    ///
    /// `None` means the requested row is absent. A present
    /// [`ReplicationRowStateRecord`] with `tombstoned = true` means the row is
    /// deleted for application visibility but retained so causally later CRDT
    /// operations can still target the row.
    pub rows: HashMap<RowKey, Option<ReplicationRowStateRecord>>,
}

/// Page metadata for one ordered dataset-row scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetRowScanPage {
    /// Replication group that owns the rows written to the scan output.
    pub group_id: GroupId,
    /// Dataset identifier within the replication group.
    pub dataset_id: DatasetId,
    /// Whether this dataset already exists for `group_id`.
    pub dataset_exists: bool,
    /// Row key to use as the exclusive lower bound for the next batch.
    ///
    /// `None` means the scan is exhausted. `Some` means callers should issue a
    /// follow-up scan when they need more rows; that follow-up may still return
    /// an empty batch if this batch ended exactly at the stored row count.
    pub next_after: Option<RowKey>,
}

impl DatasetRowScanPage {
    /// Build row transitions from two ordered pages for the same dataset reference.
    ///
    /// `self` and `previous_rows` describe the previous group;
    /// `current_page` and `current_rows` describe the current group. Both pages
    /// must reference the same dataset.
    ///
    /// The result contains at most `limit` transitions in ascending row-key order.
    /// Each key present in either input page is represented once, with the
    /// corresponding previous and current records populated when present.
    /// `next_after` contains the final emitted key when either input page may
    /// have more rows, and is `None` when both inputs are exhausted. When the
    /// combined page exceeds `limit`, input records after the final emitted key
    /// are consumed but omitted from the result. Store-backed callers resume
    /// their source scans after that key and may fetch those records again.
    ///
    /// # Panics
    ///
    /// Panics when the input pages reference different datasets.
    #[must_use]
    pub fn transition_with_limit(
        self,
        previous_rows: &ReplicationStateRowBatch,
        current_page: &Self,
        current_rows: &ReplicationStateRowBatch,
        limit: NonZeroUsize,
    ) -> DatasetRowStateTransitionBatch {
        assert_eq!(
            self.dataset_id, current_page.dataset_id,
            "row-state transition batches must reference the same dataset"
        );

        let previous_may_continue = self.next_after.is_some();
        let current_may_continue = current_page.next_after.is_some();
        let mut previous_rows = previous_rows
            .rows()
            .map(|row| replication_row_state_record(&row))
            .peekable();
        let mut current_rows = current_rows
            .rows()
            .map(|row| replication_row_state_record(&row))
            .peekable();
        let mut rows = Vec::with_capacity(limit.get());

        while rows.len() < limit.get() {
            let transition = match (previous_rows.peek(), current_rows.peek()) {
                (Some(previous), Some(current)) => match previous.row_id.cmp(&current.row_id) {
                    std::cmp::Ordering::Less => DatasetRowStateTransition {
                        row_key: previous.row_id,
                        previous: previous_rows.next(),
                        current: None,
                    },
                    std::cmp::Ordering::Equal => DatasetRowStateTransition {
                        row_key: previous.row_id,
                        previous: previous_rows.next(),
                        current: current_rows.next(),
                    },
                    std::cmp::Ordering::Greater => DatasetRowStateTransition {
                        row_key: current.row_id,
                        previous: None,
                        current: current_rows.next(),
                    },
                },
                (Some(previous), None) => DatasetRowStateTransition {
                    row_key: previous.row_id,
                    previous: previous_rows.next(),
                    current: None,
                },
                (None, Some(current)) => DatasetRowStateTransition {
                    row_key: current.row_id,
                    previous: None,
                    current: current_rows.next(),
                },
                (None, None) => break,
            };
            assert!(
                transition.previous.is_some() || transition.current.is_some(),
                "row-state transition must contain at least one record"
            );
            rows.push(transition);
        }

        let has_buffered_rows = previous_rows.peek().is_some() || current_rows.peek().is_some();
        let may_continue = has_buffered_rows || previous_may_continue || current_may_continue;
        let last_row_key = rows.last().map(|transition| transition.row_key);
        let next_after = option_when!(may_continue, last_row_key).flatten();

        DatasetRowStateTransitionBatch {
            previous_group_id: self.group_id,
            current_group_id: current_page.group_id,
            dataset_id: self.dataset_id,
            previous_dataset_exists: self.dataset_exists,
            current_dataset_exists: current_page.dataset_exists,
            rows,
            next_after,
        }
    }
}

/// One row-key-aligned transition between two group occurrences.
///
/// At least one record is present. Each present record has a `row_id` equal to
/// `row_key`; present records retain their stored active or tombstoned state.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRowStateTransition {
    /// Row key shared by both optional records.
    pub row_key: RowKey,
    /// Stored row from [`DatasetRowStateTransitionBatch::previous_group_id`], when present.
    pub previous: Option<ReplicationRowStateRecord>,
    /// Stored row from [`DatasetRowStateTransitionBatch::current_group_id`], when present.
    pub current: Option<ReplicationRowStateRecord>,
}

/// Storage result for one ordered batch of dataset row-state transitions.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRowStateTransitionBatch {
    /// Replication group owning the previous dataset occurrence.
    pub previous_group_id: GroupId,
    /// Replication group owning the current dataset occurrence.
    pub current_group_id: GroupId,
    /// Dataset scanned in both replication groups.
    pub dataset_id: DatasetId,
    /// Whether the previous dataset occurrence exists in storage.
    pub previous_dataset_exists: bool,
    /// Whether the current dataset occurrence exists in storage.
    pub current_dataset_exists: bool,
    /// Transitions in ascending order by keys present in either group occurrence.
    pub rows: Vec<DatasetRowStateTransition>,
    /// Exclusive lower bound for the next transition scan.
    ///
    /// `None` means both scans are exhausted. `Some` may lead to an empty
    /// follow-up batch when an underlying scan ended exactly at its row count.
    pub next_after: Option<RowKey>,
}

/// Complete row state snapshot used by replication storage.
pub type ReplicationRowStateSnapshot = RowStateSnapshot<'static, UpdateId>;

/// Metadata stored beside one positional replication row state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationRowMetadata {
    /// Stable row key in the dataset that owns this record.
    pub row_key: RowKey,
    /// Whether the row is deleted but retained for causal updates.
    pub tombstoned: bool,
    /// Update which first introduced this row key, when known.
    pub created_by: Option<UpdateId>,
    /// Causal version of the last update that changed this row image.
    pub last_changed_versions: VersionVector,
}

/// Reusable positional state rows returned by ordinary replication-store scans.
pub type ReplicationStateRowBatch = InMemoryStateRowBatch<ReplicationRowMetadata, UpdateId>;

/// Materialise one positional row view at a named-record compatibility boundary.
fn replication_row_state_record(
    row: &InMemoryStateRowView<'_, ReplicationRowMetadata, UpdateId>,
) -> ReplicationRowStateRecord {
    let metadata = row.metadata();
    ReplicationRowStateRecord {
        row_id: metadata.row_key,
        snapshot: row.snapshot().into_owned(),
        tombstoned: metadata.tombstoned,
        created_by: metadata.created_by,
        last_changed_versions: metadata.last_changed_versions.clone(),
    }
}

/// Stored progress for one writable replication group.
///
/// Storage queries may return these records in any order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritableReplicationGroupVersionRecord {
    /// Stable replication-group identifier.
    pub group_id: GroupId,
    /// Last applied version vector stored for the group.
    pub version_vector: VersionVector,
}

/// Row image loaded from or written to replication storage.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicationRowStateRecord {
    /// Stable row key in the dataset that owns this record.
    pub row_id: RowKey,
    /// Complete state snapshot for the row.
    pub snapshot: ReplicationRowStateSnapshot,
    /// Whether the row is deleted but still retained for causal updates.
    pub tombstoned: bool,
    /// Update which first introduced this row key in its replication group.
    ///
    /// This is unknown when storage first observes the row as a tombstone and
    /// therefore cannot prove which earlier update created it.
    pub created_by: Option<UpdateId>,
    /// Causal version of the last update that changed this row image.
    pub last_changed_versions: VersionVector,
}

/// One explicit transactional row patch for a dataset.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRowStatePatch {
    /// Replication group that owns this dataset patch.
    pub group_id: GroupId,
    /// Dataset identifier within the replication group.
    pub dataset_id: DatasetId,
    /// Ordered row-level writes to apply transactionally.
    pub actions: Vec<DatasetRowStateWrite>,
    /// Update whose operations produced every row write in this patch.
    pub change_id: UpdateId,
    /// Causal version to store as the last change for every row in `actions`.
    pub last_changed_versions: VersionVector,
}

/// One explicit storage action for a persisted dataset row.
#[derive(Clone, Debug, PartialEq)]
pub enum DatasetRowStateWrite {
    /// Ensure that `row_key` exists as an active application-visible row.
    UpsertActive {
        row_key: RowKey,
        snapshot: ReplicationRowStateSnapshot,
    },
    /// Ensure that `row_key` exists as a retained delete tombstone.
    UpsertTombstone {
        row_key: RowKey,
        snapshot: ReplicationRowStateSnapshot,
    },
}

/// Iterator used to stream requested row keys into one store transaction.
pub type RowKeyIterator<'a> = dyn Iterator<Item = &'a RowKey> + Send + 'a;

/// Read-only transaction over one replication store implementation.
///
/// Read transactions are release-on-drop. They are intended for consistent
/// snapshot streams and may be held by a provider across multiple `next_batch`
/// calls, so callers should drain or drop the provider promptly.
pub trait ReplicationStoreReadTransaction: Send {
    /// Load one persisted replication group by id.
    fn load_replication_group<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupRecord>, StoreError>>;

    /// Load all persisted replication groups currently known to the store.
    fn load_replication_groups(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<ReplicationGroupRecord>, StoreError>>;

    /// Load ids and stored progress for all currently writable replication groups.
    ///
    /// Results have no ordering guarantee and exclude non-writable lifecycle states.
    fn load_writable_replication_group_versions(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<WritableReplicationGroupVersionRecord>, StoreError>>;

    /// Load persisted replication groups whose ids are included in `group_ids`.
    ///
    /// Missing ids are omitted from the returned vector so callers can decide
    /// whether absence is expected or an error.
    fn load_replication_groups_for_ids<'a>(
        &'a mut self,
        group_ids: &'a HashSet<GroupId>,
    ) -> BoxFuture<'a, Result<Vec<ReplicationGroupRecord>, StoreError>>;

    /// Load one dataset schema stored for a specific replication group.
    fn load_group_dataset_schema<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
    ) -> BoxFuture<'a, Result<Option<SchemaSource>, StoreError>>;

    /// Load the unique local member identity represented by local-private key material.
    ///
    /// Returns `None` when the store has not been provisioned. Implementations must reject
    /// several distinct local member identities rather than selecting one arbitrarily.
    fn load_local_member_identity(
        &mut self,
    ) -> BoxFuture<'_, Result<Option<MemberIdentity>, StoreError>>;

    /// Load encrypted local-private key material for one member identity.
    fn load_local_member_private_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<LocalMemberPrivateKeysRecord>, StoreError>>;

    /// Load public key material for one exact member-key binding.
    fn load_member_public_keys<'a>(
        &'a mut self,
        key_id: &'a MemberKeyId,
    ) -> BoxFuture<'a, Result<Option<MemberPublicKeysRecord>, StoreError>>;

    /// Load every observed member-key identity without returning public key material.
    fn load_member_public_key_ids(&mut self)
    -> BoxFuture<'_, Result<Vec<MemberKeyId>, StoreError>>;

    /// Load every observed public key material record for one member identity.
    fn load_member_public_keys_for_member<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Vec<MemberPublicKeysRecord>, StoreError>>;

    /// Load every observed public key material record for one key fingerprint.
    fn load_member_public_keys_for_fingerprint<'a>(
        &'a mut self,
        fingerprint: &'a KeyFingerprint,
    ) -> BoxFuture<'a, Result<Vec<MemberPublicKeysRecord>, StoreError>>;

    /// Load trust evidence for one exact member-key binding.
    fn load_member_key_trust_evidence<'a>(
        &'a mut self,
        key_id: &'a MemberKeyId,
    ) -> BoxFuture<'a, Result<MemberKeyTrustEvidenceSet, StoreError>>;

    /// Return whether a fingerprint is globally blocked.
    fn is_key_fingerprint_blocked<'a>(
        &'a mut self,
        fingerprint: &'a KeyFingerprint,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;

    /// Load one persisted replication update by `(group_id, update_id)`.
    fn load_replication_update<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<Option<ReplicationUpdateRecord>, StoreError>>;

    /// Load persisted replication updates for one group using the given filter and optional limit.
    fn load_replication_updates<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
        limit: Option<NonZeroUsize>,
    ) -> BoxFuture<'a, Result<Vec<ReplicationUpdateRecord>, StoreError>>;

    /// Load only persisted replication update ids for one group.
    ///
    /// This is for availability/frontier checks that must not decode full
    /// update payloads. Returned ids follow the same ordering and filtering
    /// rules as [`Self::load_replication_updates`].
    fn load_replication_update_ids<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
        limit: Option<NonZeroUsize>,
    ) -> BoxFuture<'a, Result<Vec<UpdateId>, StoreError>>;

    /// Load the stored state for the requested dataset row keys.
    ///
    /// `dataset` must describe this group's authoritative schema for the requested dataset.
    /// Implementations must use its borrowed values only while executing the returned future and
    /// must not clone them into retained store state.
    ///
    /// Implementations must include every iterated `row_key` exactly once in
    /// `DatasetRowStateSlice.rows`.
    fn load_dataset_rows<'a>(
        &'a mut self,
        dataset: GroupDatasetSchemaRef<'a>,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowStateSlice, StoreError>>;

    /// Scan one ordered batch of stored dataset rows.
    ///
    /// `dataset` must describe this group's authoritative schema for the requested dataset.
    /// Implementations must use its borrowed values only while executing the returned future and
    /// must not clone them into retained store state.
    ///
    /// `after` is an exclusive lower bound over row keys. `None` starts before
    /// the first row. Implementations must reset `output`, prepare it for
    /// `dataset.schema`, and append at most `limit` rows ordered by row key.
    /// `next_after` is the last emitted row key when another scan may be needed,
    /// and `None` when this dataset scan is known to be exhausted.
    fn scan_dataset_row_batch<'a>(
        &'a mut self,
        dataset: GroupDatasetSchemaRef<'a>,
        after: Option<RowKey>,
        limit: NonZeroUsize,
        output: &'a mut ReplicationStateRowBatch,
    ) -> BoxFuture<'a, Result<DatasetRowScanPage, StoreError>>;

    /// Scan one ordered transition batch of a dataset across two replication groups.
    ///
    /// `previous_group` and `current_group` supply the owning group, equal
    /// dataset references, and schema for each occurrence. `after` is an
    /// exclusive lower bound over row keys; `None` starts before the first key.
    ///
    /// The result contains at most `limit` transitions in ascending row-key order.
    /// Every key greater than `after` which is stored in either group is
    /// represented once until the limit is reached. Each transition contains the
    /// stored previous and current records when present, including tombstones.
    /// The two dataset-existence flags describe whether the dataset is stored
    /// in each group even when that occurrence contributes no rows.
    /// `next_after` is the final emitted key when another scan may be needed,
    /// and `None` when both occurrences are known to be exhausted.
    ///
    /// # Default implementation
    ///
    /// The default performs two ordinary scans and joins their results in
    /// memory. Store engines should override it when they can align the two
    /// row-key sets more efficiently within storage.
    fn scan_dataset_row_transition_batch<'a>(
        &'a mut self,
        previous_group: GroupDatasetSchemaRef<'a>,
        current_group: GroupDatasetSchemaRef<'a>,
        after: Option<RowKey>,
        limit: NonZeroUsize,
    ) -> BoxFuture<'a, Result<DatasetRowStateTransitionBatch, StoreError>> {
        async move {
            ensure_matching_transition_dataset_references(previous_group, current_group)
                .map_err(StoreError::from_classification_source)?;
            let mut previous_rows = ReplicationStateRowBatch::new(previous_group.schema);
            let previous_batch = self
                .scan_dataset_row_batch(previous_group, after, limit, &mut previous_rows)
                .await?;
            let mut current_rows = ReplicationStateRowBatch::new(current_group.schema);
            let current_batch = self
                .scan_dataset_row_batch(current_group, after, limit, &mut current_rows)
                .await?;
            Ok(previous_batch.transition_with_limit(
                &previous_rows,
                &current_batch,
                &current_rows,
                limit,
            ))
        }
        .boxed()
    }

    /// Load all unresolved listener-mediated group decisions.
    fn load_pending_group_decisions(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<PendingGroupDecisionRecord>, StoreError>>;

    /// Load the unresolved decision for one target group, if present.
    fn load_pending_group_decision<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<PendingGroupDecisionRecord>, StoreError>>;

    /// Load all accepted group activations that are not externally active yet.
    fn load_pending_group_activations(
        &mut self,
    ) -> BoxFuture<'_, Result<Vec<PendingGroupActivationRecord>, StoreError>>;

    /// Load accepted activation work targeting one group, if present.
    fn load_pending_group_activation<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<PendingGroupActivationRecord>, StoreError>>;

    /// Load group material regardless of whether the group is active yet.
    fn load_replication_group_material<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupMaterialRecord>, StoreError>>;

    /// Explicitly release the read transaction.
    ///
    /// Callers may skip this and simply drop the transaction instead, but an
    /// explicit release allows store implementations to release resources
    /// promptly and surface release failures directly.
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>>;
}

/// One dataset-scoped batch inside a persisted replication update.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetUpdateRecord {
    /// Dataset targeted by this batch of schema operations.
    pub dataset_id: DatasetId,
    /// Ordered schema operations for `dataset_id` within one replication update.
    pub operations: Vec<flotsync_messages::datamodel::SchemaOperation>,
}

/// One persisted replication update recorded by the runtime.
///
/// Stores must preserve at most one record for each
/// `(group_id, update_id)` pair. The `applied_locally` flag distinguishes
/// updates that are already reflected in stored dataset snapshots from updates
/// that are still only present in the append-only update log.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicationUpdateRecord {
    /// Group that this update belongs to.
    pub group_id: GroupId,
    /// Stable replication update identifier within `group_id`.
    pub update_id: UpdateId,
    /// Logical sender of the update.
    pub sender: MemberIdentity,
    /// Sender read-version snapshot carried with this update.
    pub read_versions: VersionVector,
    /// Per-dataset schema operations in transport order.
    pub dataset_updates: Vec<DatasetUpdateRecord>,
    /// Whether this update is already reflected in stored local dataset state.
    pub applied_locally: bool,
}

/// Which replication updates should be returned by one transaction query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReplicationUpdateFilter {
    /// Return every persisted update for the group.
    All,
    /// Return only updates that are not yet reflected in stored local state.
    PendingApply,
    /// Return only updates that are already reflected in stored local state.
    Applied,
    /// Return persisted updates for one producer and inclusive version range.
    ProducerRange {
        producer_index: MemberIndex,
        start_version: u64,
        end_version: u64,
    },
}

/// Mutable transaction over one replication store implementation.
///
/// Implementations must provide read-your-own-writes semantics within the same
/// transaction object so the runtime can interleave async validation and
/// mutation steps without reconstructing temporary whole-runtime state.
///
/// Transactions are rollback-by-default. Dropping an uncommitted transaction
/// must discard all uncommitted writes as if `rollback` had been called.
/// `rollback` remains part of the API so callers can release store resources
/// early and observe rollback failures explicitly when the backend can report
/// them.
///
/// Mutable transactions inherit the read API from
/// [`ReplicationStoreReadTransaction`]. The inherited `release` operation is a
/// rollback-style release path for mutable transactions; write callers should
/// still use [`Self::commit`] or [`Self::rollback`] to make intent explicit.
pub trait ReplicationStoreTransaction: ReplicationStoreReadTransaction {
    /// Insert one new persisted replication group.
    fn insert_replication_group(
        &mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Store group material or refresh metadata on a compatible existing record.
    ///
    /// Compatibility requires the same group definition and security material;
    /// name metadata may differ and is replaced when it does.
    fn ensure_replication_group_material(
        &mut self,
        material: ReplicationGroupMaterialRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Mark stored group material active at the supplied initial progress.
    fn activate_replication_group(
        &mut self,
        group_id: GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Insert encrypted local-private key material or confirm it is already stored unchanged.
    fn ensure_local_member_private_keys(
        &mut self,
        record: LocalMemberPrivateKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Insert public key material or confirm it is already stored unchanged.
    fn ensure_member_public_keys(
        &mut self,
        record: MemberPublicKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Insert trust evidence or confirm it is already present.
    fn ensure_member_key_trust_evidence(
        &mut self,
        record: MemberKeyTrustEvidenceRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Insert a globally blocked fingerprint or confirm it is already present.
    fn ensure_blocked_key_fingerprint(
        &mut self,
        fingerprint: KeyFingerprint,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Advance the stored applied version vector for one existing replication group.
    fn update_replication_group_version_vector<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Replace the application-access lifecycle for one hosted group.
    fn update_replication_group_lifecycle<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        lifecycle: ReplicationGroupLifecycle,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Apply one explicit set of row-level dataset storage actions.
    ///
    /// `dataset` must carry the same group and dataset identifiers as `patch` and the authoritative
    /// schema for that group dataset. Implementations must return an error when either identifier
    /// differs. They must use the borrowed context only while executing the returned future and
    /// must not clone it into retained store state.
    fn apply_dataset_row_patch<'a>(
        &'a mut self,
        dataset: GroupDatasetSchemaRef<'a>,
        patch: &'a DatasetRowStatePatch,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Append one new persisted replication update record.
    ///
    /// Implementations must preserve the uniqueness of `(group_id, update_id)`
    /// and reject attempts to overwrite an existing stored update blob.
    fn append_replication_update(
        &mut self,
        update: ReplicationUpdateRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Mark one persisted replication update as already applied locally.
    fn mark_replication_update_applied<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Insert or replace one unresolved listener-mediated group decision.
    fn upsert_pending_group_decision(
        &mut self,
        record: PendingGroupDecisionRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Remove one unresolved group decision.
    ///
    /// The returned boolean is `true` when a pending decision with `key`
    /// existed and was removed. It is `false` when the decision had already
    /// been resolved or never existed.
    fn remove_pending_group_decision(
        &mut self,
        key: PendingGroupWorkKey,
    ) -> BoxFuture<'_, Result<bool, StoreError>>;

    /// Insert or replace one accepted group activation that is not externally active yet.
    fn upsert_pending_group_activation(
        &mut self,
        record: PendingGroupActivationRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Remove one accepted group activation.
    ///
    /// The returned boolean is `true` when a pending activation with `key`
    /// existed and was removed. It is `false` when the activation had already
    /// completed or never existed.
    fn remove_pending_group_activation(
        &mut self,
        key: PendingGroupWorkKey,
    ) -> BoxFuture<'_, Result<bool, StoreError>>;

    /// Remove inactive material after its pending work is rejected.
    ///
    /// Returns `true` when inactive material existed and was removed. Active
    /// group material is never removed by this operation.
    fn remove_inactive_replication_group_material(
        &mut self,
        group_id: GroupId,
    ) -> BoxFuture<'_, Result<bool, StoreError>>;

    /// Commit all writes performed in this transaction.
    fn commit(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>>;

    /// Explicitly roll back all writes performed in this transaction.
    ///
    /// Callers may skip this and simply drop the transaction instead, but an
    /// explicit rollback allows store implementations to release resources
    /// promptly and surface rollback failures directly.
    fn rollback(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>>;
}

/// Storage capabilities available before a local replication identity has been provisioned.
///
/// Provisioning stores deliberately do not implement [`ReplicationStore`]: a runtime store must
/// already have one authoritative local identity, while this interface also represents an empty
/// store. Applications should provision identity material and then activate the backend-specific
/// replication-store type.
pub trait LocalIdentityProvisioningStore: Send + Sync {
    /// Load the identity already represented by local-private key material, when present.
    ///
    /// Implementations must reject several distinct local member identities.
    fn local_member_identity(&self) -> BoxFuture<'_, Result<Option<MemberIdentity>, StoreError>>;

    /// Begin the mutable transaction used to establish identity and key material atomically.
    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>>;
}

/// Persistence extension point.
pub trait ReplicationStore:
    crate::delivery::contracts::ReliableDeliveryStore + Send + Sync
{
    /// Return the member identity hosted by this replication runtime instance.
    fn local_member_identity(&self) -> BoxFuture<'_, Result<MemberIdentity, StoreError>>;

    /// Begin one mutable transaction over the replication state store.
    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>>;

    /// Begin one read-only transaction over the replication state store.
    fn begin_read_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreReadTransaction>, StoreError>>;
}

/// Validate that two group-dataset references describe one dataset transition.
pub(crate) fn ensure_matching_transition_dataset_references(
    previous_group: GroupDatasetSchemaRef<'_>,
    current_group: GroupDatasetSchemaRef<'_>,
) -> Result<(), DatasetTransitionReferenceMismatchError> {
    ensure!(
        previous_group.dataset_id == current_group.dataset_id,
        DatasetTransitionReferenceMismatchSnafu {
            previous_dataset_id: previous_group.dataset_id.clone(),
            current_dataset_id: current_group.dataset_id.clone(),
        }
    );
    Ok(())
}

/// Two sides of a requested row transition referenced different datasets.
#[derive(Debug, Snafu)]
#[snafu(display(
    "Dataset row transition referenced previous dataset '{previous_dataset_id}' and current dataset '{current_dataset_id}'."
))]
pub(crate) struct DatasetTransitionReferenceMismatchError {
    /// Dataset reference supplied for the previous group occurrence.
    previous_dataset_id: DatasetId,
    /// Dataset reference supplied for the current group occurrence.
    current_dataset_id: DatasetId,
}

impl StoreErrorClassificationSource for DatasetTransitionReferenceMismatchError {
    fn store_error_classification(&self) -> Option<StoreErrorClassification> {
        Some(
            StoreErrorClassification::UNKNOWN
                .with_scope(StoreErrorScope::Operation)
                .with_class(StoreErrorClass::Contract)
                .with_resolution(StoreErrorResolution::FixBug),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn row(row_key: u128) -> ReplicationRowStateRecord {
        ReplicationRowStateRecord {
            row_id: RowKey(Uuid::from_u128(row_key)),
            snapshot: ReplicationRowStateSnapshot::from_owned_fields(Vec::new()),
            tombstoned: false,
            created_by: Some(UpdateId::INITIAL_STATE_ORIGIN),
            last_changed_versions: VersionVector::initial(
                NonZeroUsize::new(1).expect("test member count is non-zero"),
            ),
        }
    }

    fn batch(
        group_id: u128,
        dataset_id: &'static str,
        rows: impl IntoIterator<Item = u128>,
        next_after: Option<u128>,
    ) -> (DatasetRowScanPage, ReplicationStateRowBatch) {
        let schema = Schema::empty();
        let mut state_rows = ReplicationStateRowBatch::new(&schema);
        for row in rows.into_iter().map(row) {
            let encoded =
                flotsync_messages::codecs::datamodel::encode_row_snapshot(&row.snapshot, &schema)
                    .expect("test row must encode against the empty schema");
            let mut decoder =
                flotsync_messages::snapshots::datamodel::ProtoSchemaSnapshotDecoder::new(encoded)
                    .expect("test row must create a snapshot decoder");
            state_rows
                .push_decoded_row(
                    ReplicationRowMetadata {
                        row_key: row.row_id,
                        tombstoned: row.tombstoned,
                        created_by: row.created_by,
                        last_changed_versions: row.last_changed_versions,
                    },
                    &mut decoder,
                )
                .expect("test row must decode into the state batch");
        }
        let page = DatasetRowScanPage {
            group_id: GroupId(Uuid::from_u128(group_id)),
            dataset_id: DatasetId::try_from_static(dataset_id).expect("test dataset id is valid"),
            dataset_exists: true,
            next_after: next_after.map(|row_key| RowKey(Uuid::from_u128(row_key))),
        };
        (page, state_rows)
    }

    #[test]
    fn transition_batch_aligns_the_row_key_union() {
        let (previous_page, previous_rows) = batch(1, "shared", [1, 3], None);
        let (current_page, current_rows) = batch(2, "shared", [2, 3], None);
        let limit = NonZeroUsize::new(4).expect("test limit is non-zero");
        let merged = previous_page.transition_with_limit(
            &previous_rows,
            &current_page,
            &current_rows,
            limit,
        );

        let row_presence = merged
            .rows
            .iter()
            .map(|transition| {
                (
                    transition.row_key.0.as_u128(),
                    transition.previous.is_some(),
                    transition.current.is_some(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            row_presence,
            vec![(1, true, false), (2, false, true), (3, true, true)]
        );
        assert_eq!(merged.next_after, None);
    }

    #[test]
    fn transition_batch_pages_the_union_without_losing_buffered_keys() {
        let (previous_page, previous_rows) = batch(1, "shared", [1, 4], None);
        let (current_page, current_rows) = batch(2, "shared", [2, 3], None);
        let limit = NonZeroUsize::new(2).expect("test limit is non-zero");
        let merged = previous_page.transition_with_limit(
            &previous_rows,
            &current_page,
            &current_rows,
            limit,
        );

        let row_keys = merged
            .rows
            .iter()
            .map(|transition| transition.row_key.0.as_u128())
            .collect::<Vec<_>>();

        assert_eq!(row_keys, vec![1, 2]);
        assert_eq!(merged.next_after, Some(RowKey(Uuid::from_u128(2))));
    }

    #[test]
    fn transition_batch_preserves_underlying_exact_limit_continuation() {
        let (previous_page, previous_rows) = batch(1, "shared", [1], Some(1));
        let (current_page, current_rows) = batch(2, "shared", [1], Some(1));
        let limit = NonZeroUsize::new(1).expect("test limit is non-zero");
        let merged = previous_page.transition_with_limit(
            &previous_rows,
            &current_page,
            &current_rows,
            limit,
        );

        assert_eq!(merged.next_after, Some(RowKey(Uuid::from_u128(1))));
    }
}

//! Replication store records and transaction contracts.

use super::*;

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

/// Storage-extension result for one ordered batch of rows scanned from a dataset.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRowStateBatch {
    /// Replication group that owns this batch.
    pub group_id: GroupId,
    /// Dataset identifier within the replication group.
    pub dataset_id: DatasetId,
    /// Whether this dataset already exists for `group_id`.
    pub dataset_exists: bool,
    /// Row records ordered by row key.
    pub rows: Vec<ReplicationRowStateRecord>,
    /// Row key to use as the exclusive lower bound for the next batch.
    ///
    /// `None` means the scan is exhausted. `Some` means callers should issue a
    /// follow-up scan when they need more rows; that follow-up may still return
    /// an empty batch if this batch ended exactly at the stored row count.
    pub next_after: Option<RowKey>,
}

/// Complete row state snapshot used by replication storage.
pub type ReplicationRowStateSnapshot = RowStateSnapshot<'static, UpdateId>;

/// Row image loaded from or written to replication storage.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicationRowStateRecord {
    /// Stable row key in the dataset that owns this record.
    pub row_id: RowKey,
    /// Complete state snapshot for the row.
    pub snapshot: ReplicationRowStateSnapshot,
    /// Whether the row is deleted but still retained for causal updates.
    pub tombstoned: bool,
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
    /// Implementations must include every iterated `row_key` exactly once in
    /// `DatasetRowStateSlice.rows`.
    fn load_dataset_rows<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowStateSlice, StoreError>>;

    /// Scan one ordered batch of stored dataset rows.
    ///
    /// `after` is an exclusive lower bound over row keys. `None` starts before
    /// the first row. Implementations must return at most `limit` rows ordered
    /// by row key. `next_after` is the last emitted row key when another scan
    /// may be needed, and `None` when this dataset scan is known to be
    /// exhausted.
    fn scan_dataset_row_batch<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        after: Option<RowKey>,
        limit: NonZeroUsize,
    ) -> BoxFuture<'a, Result<DatasetRowStateBatch, StoreError>>;

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

    /// Store group material or confirm an identical record already exists.
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
    fn apply_dataset_row_patch(
        &mut self,
        patch: DatasetRowStatePatch,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

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

/// Persistence extension point.
pub trait ReplicationStore: Send + Sync {
    /// Return the member identity hosted by this replication runtime instance.
    fn local_member_identity(&self) -> BoxFuture<'_, Result<MemberIdentity, StoreError>>;

    /// Load one locally available dataset schema.
    fn load_dataset_schema(
        &self,
        dataset_id: &DatasetId,
    ) -> BoxFuture<'_, Result<Option<SchemaSource>, StoreError>>;

    /// Begin one mutable transaction over the replication state store.
    fn begin_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreTransaction>, StoreError>>;

    /// Begin one read-only transaction over the replication state store.
    fn begin_read_transaction(
        &self,
    ) -> BoxFuture<'_, Result<Box<dyn ReplicationStoreReadTransaction>, StoreError>>;
}

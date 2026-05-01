use flotsync_core::versions::{UpdateId, VersionVector};
use flotsync_data_types::schema::datamodel::{NullableBasicValue, RowRecord, RowSnapshot};
use flotsync_utils::BoxFuture;
use smallvec::SmallVec;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
};

mod errors;
mod ids;
pub mod providers;

pub use errors::*;
pub use flotsync_data_types::{
    Decode,
    DecodeValueError,
    InMemoryFieldValue,
    RowOperations,
    RowRead,
    schema::datamodel::SchemaSource,
};
pub use ids::*;

/// Write-only row payload submitted by applications.
///
/// For a new row this must contain the initial values required by the dataset
/// schema. For an existing row this is a sparse field patch: fields omitted from
/// `fields` are intentionally left unchanged by `publish_changes`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MutableRow {
    /// Desired nullable field values keyed by field name.
    pub fields: HashMap<String, NullableBasicValue>,
}

impl MutableRow {
    pub fn new(fields: HashMap<String, NullableBasicValue>) -> Self {
        Self { fields }
    }
}

/// Convenience macro to build a [`MutableRow`] inline.
#[macro_export]
macro_rules! row_values {
    ($($field:expr => $value:expr),* $(,)?) => {{
        let mut fields: ::std::collections::HashMap<
            ::std::string::String,
            ::flotsync_data_types::schema::datamodel::NullableBasicValue,
        > = ::std::collections::HashMap::new();
        $(
            fields.insert(($field).to_string(), ($value).into());
        )*
        $crate::api::MutableRow::new(fields)
    }};
}

/// A row-level mutation submitted by an application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowMutation {
    /// Insert a new row or update the provided fields of an existing row.
    Upsert { row_id: RowId, row: MutableRow },
    /// Tombstone an existing row.
    Delete { row_id: RowId },
}

impl RowMutation {
    pub fn row_id(&self) -> &RowId {
        match self {
            RowMutation::Upsert { row_id, .. } | RowMutation::Delete { row_id } => row_id,
        }
    }
}

/// Row-level change emitted by the framework to an application listener.
pub enum RowChange {
    Upsert {
        row_id: RowId,
        row: Arc<dyn RowRead<UpdateId> + Send + Sync>,
    },
    Delete {
        row_id: RowId,
    },
}

impl RowChange {
    pub fn row_id(&self) -> &RowId {
        match self {
            RowChange::Upsert { row_id, .. } | RowChange::Delete { row_id } => row_id,
        }
    }

    pub fn row(&self) -> Option<&(dyn RowRead<UpdateId> + Send + Sync)> {
        match self {
            RowChange::Upsert { row, .. } => Some(row.as_ref()),
            RowChange::Delete { .. } => None,
        }
    }
}

/// Batch of row changes emitted by a [`RowProvider`].
///
/// An empty batch marks end-of-stream.
pub type RowChangeBatch = SmallVec<[RowChange; 4]>;

/// Source for batched row-level changes carried in `ReplicationEvent::DataChanged`.
///
/// Returning an empty batch signals end-of-stream.
pub trait RowProvider: Send {
    fn next_batch<'a>(&'a mut self) -> BoxFuture<'a, Result<RowChangeBatch, RowProviderError>>;
}

/// Request a full snapshot of all rows in the latest state of the replication
/// group with `group_id` for the given `datasets`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRowsRequest {
    /// Id of the replication group this request targets.
    pub group_id: GroupId,
    /// Application datasets to include in the snapshot.
    pub datasets: HashSet<DatasetId>,
    /// Maximum number of stored rows the provider should read in one batch.
    pub max_rows_per_batch: NonZeroUsize,
    /// Whether retained delete tombstones should be emitted with
    /// [`SnapshotRow::deleted`] set.
    ///
    /// Application reload paths normally leave this disabled so they only
    /// rebuild visible rows.
    pub include_tombstones: bool,
}

/// Row snapshot stream returned by [`ReplicationApi::snapshot_rows`].
pub struct SnapshotRows {
    pub group_id: GroupId,
    pub rows: Box<dyn SnapshotRowProvider>,
}

impl std::fmt::Debug for SnapshotRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotRows")
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}

/// One row in a snapshot stream.
pub struct SnapshotRow {
    pub row_id: RowId,
    pub row: Arc<dyn RowRead<UpdateId> + Send + Sync>,
    /// Whether this row is a retained delete tombstone instead of an
    /// application-visible row.
    pub deleted: bool,
}

/// Batch of rows emitted by a [`SnapshotRowProvider`].
pub type SnapshotRowBatch = SmallVec<[SnapshotRow; 16]>;

/// Source for batched snapshot rows.
///
/// Returning `None` signals end-of-stream. Providers may hold a store read
/// transaction internally, so callers should drain or drop them promptly.
pub trait SnapshotRowProvider: Send {
    fn next_batch<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<Option<SnapshotRowBatch>, RowProviderError>>;
}

/// One row entry in an initial dataset state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialRowState {
    pub row_key: RowKey,
    pub row: MutableRow,
}

/// Batch type used by [`InitialRowProvider`].
///
/// An empty batch marks end-of-stream.
pub type InitialRowBatch = Vec<InitialRowState>;

/// Source for one dataset's initial rows in `InitialGroupState`.
///
/// `reuse` is owned and returned so callers can keep allocation capacity across calls.
/// Returning an empty batch signals end-of-stream.
pub trait InitialRowProvider: Send {
    fn next_batch<'a>(
        &'a mut self,
        reuse: InitialRowBatch,
        max_rows: NonZeroUsize,
    ) -> BoxFuture<'a, Result<InitialRowBatch, RowProviderError>>;
}

/// Initial state rows for one dataset.
pub struct InitialDatasetState {
    pub dataset_id: DatasetId,
    pub rows: Box<dyn InitialRowProvider>,
}

/// Initial group state grouped by dataset.
#[derive(Default)]
pub struct InitialGroupState {
    pub datasets: Vec<InitialDatasetState>,
}

/// How to handle a particular group migration change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupMigrationPolicyBehaviour {
    /// Accept incoming invitations automatically.
    AutoAccept,
    /// Forward invitations to the application listener for an explicit decision.
    ViaListener,
    /// Reject incoming invitations automatically.
    AutoReject,
}

/// Policy for handling membership migration invitations.
///
/// If more than one behaviour applies to a change, the most restrictive behaviour is used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GroupMigrationPolicy {
    /// Applied if the migration contains only an epoch change, so that the group does not run out
    /// of change versions.
    pub epoch_change: GroupMigrationPolicyBehaviour,
    /// Applied when a new top-level member was added to the group.
    pub member_added: GroupMigrationPolicyBehaviour,
    /// Applied when a member added a new device to the group.
    pub member_device_added: GroupMigrationPolicyBehaviour,
    /// Applied when a top-level member is removed from the group.
    pub member_removed: GroupMigrationPolicyBehaviour,
    /// Applied when a member removes one of their devices from the group.
    pub member_device_removed: GroupMigrationPolicyBehaviour,
}

impl Default for GroupMigrationPolicy {
    fn default() -> Self {
        Self {
            epoch_change: GroupMigrationPolicyBehaviour::AutoAccept,
            member_added: GroupMigrationPolicyBehaviour::ViaListener,
            member_device_added: GroupMigrationPolicyBehaviour::AutoAccept,
            member_removed: GroupMigrationPolicyBehaviour::ViaListener,
            member_device_removed: GroupMigrationPolicyBehaviour::AutoAccept,
        }
    }
}

/// Runtime configuration passed during `load`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicationConfig {
    pub group_migration_policy: GroupMigrationPolicy,
}

/// Request to create a new replication group.
pub struct CreateGroupRequest {
    pub members: Vec<MemberIdentity>,
    pub initial_state: Option<InitialGroupState>,
}

impl std::fmt::Debug for CreateGroupRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateGroupRequest")
            .field("members", &self.members)
            .field("has_initial_state", &self.initial_state.is_some())
            .finish()
    }
}

/// Request to change membership of an existing group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeGroupMembershipRequest {
    pub group_id: GroupId,
    pub add_members: Vec<MemberIdentity>,
    pub remove_members: Vec<MemberIdentity>,
}

/// Receipt returned by `publish_changes`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PublishReceipt {
    pub update_id: UpdateId,
}

/// Invitation details surfaced when migration policy requires listener mediation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupInvitation {
    pub migration: GroupMigration,
    pub proposed_members: Vec<MemberIdentity>,
    pub group_name: Option<String>,
    pub message: Option<String>,
}

/// Listener-visible replication events.
pub enum ReplicationEvent {
    DataChanged {
        rows: Box<dyn RowProvider>,
    },
    GroupInvitation {
        invitation: GroupInvitation,
        respond: Box<dyn GroupInvitationResponder>,
    },
}

/// Callback for applications to accept or reject one migration invitation.
///
/// The callback is one-shot by construction: calling either method consumes the responder.
pub trait GroupInvitationResponder: Send {
    /// Join the new group and consider the old group (if any) closed.
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>>;

    /// Refuse to join the new group, ignore all its messages.
    fn reject(self: Box<Self>, reason: RejectionReason)
    -> BoxFuture<'static, Result<(), ApiError>>;
}

/// Optional rejection reasons reported back to the framework.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RejectionReason {
    PolicyDenied,
    UserDenied,
    UnsupportedSchemaEpoch,
    IncompatibleMembership,
    Other(String),
}

/// Application listener callback interface.
pub trait ReplicationEventListener: Send + Sync {
    fn on_event(&self, event: ReplicationEvent) -> BoxFuture<'_, Result<(), ListenerError>>;
}

/// Application-facing replication control surface.
pub trait ReplicationApi: Send + Sync {
    fn publish_changes(
        &self,
        changes: Vec<RowMutation>,
    ) -> BoxFuture<'_, Result<PublishReceipt, ApiError>>;

    fn snapshot_rows(
        &self,
        request: SnapshotRowsRequest,
    ) -> BoxFuture<'_, Result<SnapshotRows, ApiError>>;

    fn create_group(&self, req: CreateGroupRequest) -> BoxFuture<'_, Result<GroupId, ApiError>>;

    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<GroupMigration, ApiError>>;
}

/// One persisted replication group together with its local progress.
///
/// This is the group-level record that anchors dataset snapshots and persisted
/// replication updates in the store. The canonical member order is significant
/// because it defines the stable `MemberIndex` values used in version vectors
/// and `UpdateId.node_index`.
#[derive(Clone)]
pub struct ReplicationGroupRecord {
    /// Stable replication-group identifier.
    pub group_id: GroupId,
    /// Canonical member order for the group.
    pub members: Vec<MemberIdentity>,
    /// Position of the local member within `members`.
    pub local_member_index: MemberIndex,
    /// Last locally durable version vector for this group.
    pub version_vector: VersionVector,
}

impl ReplicationGroupRecord {
    /// Return the number of members encoded in this record.
    pub fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.members.len())
            .expect("replication group records must not contain an empty member set")
    }

    /// Return the local member identity referenced by `local_member_index`.
    ///
    /// Store implementations must preserve the invariant that
    /// `local_member_index < members.len()`.
    pub fn local_member(&self) -> &MemberIdentity {
        &self.members[self.local_member_index.as_u32() as usize]
    }
}

impl std::fmt::Debug for ReplicationGroupRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationGroupRecord")
            .field("group_id", &self.group_id)
            .field("members", &self.members)
            .field("local_member_index", &self.local_member_index)
            .field("version_vector", &self.version_vector)
            .finish()
    }
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
pub struct DatasetRowSlice {
    /// Replication group that owns this dataset slice.
    pub group_id: GroupId,
    /// Dataset identifier within the replication group.
    pub dataset_id: DatasetId,
    /// Whether this dataset already exists durably for `group_id`.
    pub dataset_exists: bool,
    /// Durable state for each requested row key.
    ///
    /// `None` means the requested row is absent. A present
    /// [`ReplicationRowRecord`] with `tombstoned = true` means the row is
    /// deleted for application visibility but retained so causally later CRDT
    /// operations can still target the row.
    pub rows: HashMap<RowKey, Option<ReplicationRowRecord>>,
}

/// Storage-extension result for one ordered batch of rows scanned from a dataset.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRowsBatch {
    /// Replication group that owns this batch.
    pub group_id: GroupId,
    /// Dataset identifier within the replication group.
    pub dataset_id: DatasetId,
    /// Whether this dataset already exists for `group_id`.
    pub dataset_exists: bool,
    /// Row records ordered by row key.
    pub rows: Vec<ReplicationRowRecord>,
    /// Row key to use as the exclusive lower bound for the next batch.
    ///
    /// `None` means the scan is exhausted. `Some` means callers should issue a
    /// follow-up scan when they need more rows; that follow-up may still return
    /// an empty batch if this batch ended exactly at the stored row count.
    pub next_after: Option<RowKey>,
}

/// Complete row snapshot used by replication storage.
pub type ReplicationRowSnapshot = RowSnapshot<'static, UpdateId>;

/// Durable row image loaded from or written to replication storage.
pub type ReplicationRowRecord = RowRecord<'static, RowKey, UpdateId>;

/// One explicit transactional row patch for a dataset.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRowPatch {
    /// Replication group that owns this dataset patch.
    pub group_id: GroupId,
    /// Dataset identifier within the replication group.
    pub dataset_id: DatasetId,
    /// Ordered row-level writes to apply transactionally.
    pub actions: Vec<DatasetRowWrite>,
}

/// One explicit storage action for a persisted dataset row.
#[derive(Clone, Debug, PartialEq)]
pub enum DatasetRowWrite {
    /// Ensure that `row_key` exists as an active application-visible row.
    UpsertActive {
        row_key: RowKey,
        snapshot: ReplicationRowSnapshot,
    },
    /// Ensure that `row_key` exists as a retained delete tombstone.
    UpsertTombstone {
        row_key: RowKey,
        snapshot: ReplicationRowSnapshot,
    },
}

/// Iterator used to stream requested row keys into one store transaction.
pub type RowKeyIterator<'a> = dyn Iterator<Item = &'a RowKey> + Send + 'a;

/// Read-only transaction over one replication store implementation.
///
/// Read transactions are rollback-by-default. They are intended for consistent
/// snapshot streams and may be held by a provider across multiple `next_batch`
/// calls, so callers should drain or drop the provider promptly.
pub trait ReplicationStoreReadTransaction: Send {
    /// Load one persisted replication group by id.
    fn load_replication_group<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupRecord>, StoreError>>;

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
    ) -> BoxFuture<'a, Result<DatasetRowsBatch, StoreError>>;

    /// Explicitly release the read transaction.
    ///
    /// Callers may skip this and simply drop the transaction instead, but an
    /// explicit rollback allows store implementations to release resources
    /// promptly and surface rollback failures directly.
    fn rollback(self: Box<Self>) -> BoxFuture<'static, Result<(), StoreError>>;
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
/// Stores must preserve at most one durable record for each
/// `(group_id, update_id)` pair. The `applied_locally` flag distinguishes
/// updates that are already reflected in durable dataset snapshots from updates
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
    /// Whether this update is already reflected in durable local dataset state.
    pub applied_locally: bool,
}

/// Which replication updates should be returned by one transaction query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReplicationUpdateFilter {
    /// Return every persisted update for the group.
    All,
    /// Return only updates that are not yet reflected in durable local state.
    PendingApply,
    /// Return only updates that are already reflected in durable local state.
    Applied,
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
pub trait ReplicationStoreTransaction: Send {
    /// Load one persisted replication group by id.
    fn load_replication_group<'a>(
        &'a mut self,
        group_id: &'a GroupId,
    ) -> BoxFuture<'a, Result<Option<ReplicationGroupRecord>, StoreError>>;

    /// Load all persisted replication groups currently known to the store.
    fn load_replication_groups<'a>(
        &'a mut self,
    ) -> BoxFuture<'a, Result<Vec<ReplicationGroupRecord>, StoreError>>;

    /// Insert one new persisted replication group.
    fn insert_replication_group<'a>(
        &'a mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Advance the durable version vector for one existing replication group.
    fn update_replication_group_version_vector<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Load the durable state for the requested dataset row keys.
    ///
    /// Implementations must include every iterated `row_key` exactly once in
    /// `DatasetRowSlice.rows`.
    fn load_dataset_rows<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowSlice, StoreError>>;

    /// Apply one explicit set of durable row-level dataset storage actions.
    fn apply_dataset_row_patch<'a>(
        &'a mut self,
        patch: DatasetRowPatch,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Load one persisted replication update by `(group_id, update_id)`.
    fn load_replication_update<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<Option<ReplicationUpdateRecord>, StoreError>>;

    /// Load persisted replication updates for one group using the given filter.
    fn load_replication_updates<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        filter: ReplicationUpdateFilter,
    ) -> BoxFuture<'a, Result<Vec<ReplicationUpdateRecord>, StoreError>>;

    /// Append one new persisted replication update record.
    ///
    /// Implementations must preserve the uniqueness of `(group_id, update_id)`
    /// and reject attempts to overwrite an existing durable update blob.
    fn append_replication_update<'a>(
        &'a mut self,
        update: ReplicationUpdateRecord,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Mark one persisted replication update as already applied locally.
    fn mark_replication_update_applied<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        update_id: UpdateId,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Durably commit all writes performed in this transaction.
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

    /// Load one application-defined dataset schema when available locally.
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

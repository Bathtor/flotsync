use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use enumset::{EnumSet, EnumSetType};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    member::{Identifier, TrieMap},
    membership::{GroupMembers, GroupMembersError},
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::schema::{
    Schema,
    datamodel::{NullableBasicValue, RowStateSnapshot},
};
use flotsync_security::{
    KeyFingerprint,
    PublicKeyBundle,
    PublicMemberKeys,
    StoreSecretKey,
    load_local_store_secret,
    load_or_create_local_store_secret,
};
use flotsync_utils::{BoxFuture, option_when};
use smallvec::{Array, SmallVec, smallvec};
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt,
    num::NonZeroUsize,
    str::FromStr,
    sync::Arc,
};

mod errors;
mod ids;
pub mod providers;
pub mod security;

pub use errors::*;
pub use flotsync_data_types::{
    Decode,
    DecodeValueError,
    InMemoryFieldState,
    InMemoryValueData,
    InMemoryValueDataRowRef,
    RowOperations,
    RowValueRead,
    RowValues,
    schema::datamodel::SchemaSource,
};
pub use flotsync_security::{LocalStoreSecretProfile, StoreSecretKeyId};
pub use ids::*;

/// Write-only row payload submitted by applications.
///
/// For a new row this must contain the initial values required by the dataset
/// schema. For an existing row this is a sparse field patch: fields omitted from
/// `fields` are intentionally left unchanged by `publish_changes`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RowValuesPatch {
    /// Desired nullable field values keyed by field name.
    pub fields: HashMap<String, NullableBasicValue>,
}

impl RowValuesPatch {
    #[must_use]
    pub fn new(fields: HashMap<String, NullableBasicValue>) -> Self {
        Self { fields }
    }
}

/// Convenience macro to build a [`RowValuesPatch`] inline.
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
        $crate::api::RowValuesPatch::new(fields)
    }};
}

/// A row-level mutation submitted by an application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowMutation {
    /// Insert a new row or update the provided fields of an existing row.
    Upsert { row_id: RowId, row: RowValuesPatch },
    /// Tombstone an existing row.
    Delete { row_id: RowId },
}

impl RowMutation {
    #[must_use]
    pub fn row_id(&self) -> &RowId {
        match self {
            RowMutation::Upsert { row_id, .. } | RowMutation::Delete { row_id } => row_id,
        }
    }
}

/// Opaque read-position token returned by the replication runtime.
///
/// Applications should store this value alongside their in-memory application
/// state and pass it back to [`ReplicationApi::publish_changes`].
#[derive(Clone, PartialEq, Eq)]
pub struct ReadToken {
    // Developer note: the token intentionally hides its group-scoped version
    // vectors from applications. Runtime internals remain responsible for
    // validating group compatibility and advancing the right producer position.
    versions: Arc<ReadTokenVersions>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadTokenVersions {
    pub(crate) groups: HashMap<GroupId, VersionVector>,
}

impl ReadToken {
    pub(crate) fn from_group_versions(groups: HashMap<GroupId, VersionVector>) -> Self {
        Self {
            versions: Arc::new(ReadTokenVersions { groups }),
        }
    }

    pub(crate) fn group_version(&self, group_id: &GroupId) -> Option<&VersionVector> {
        self.versions.groups.get(group_id)
    }

    pub(crate) fn group_count(&self) -> usize {
        self.versions.groups.len()
    }

    pub(crate) fn with_group_version(
        &self,
        group_id: GroupId,
        version_vector: VersionVector,
    ) -> Self {
        let mut groups = self.versions.groups.clone();
        groups.insert(group_id, version_vector);
        Self::from_group_versions(groups)
    }

    pub(crate) fn with_update_applied(&self, group_id: GroupId, update_id: UpdateId) -> Self {
        let Some(group_versions) = self.group_version(&group_id) else {
            return self.clone();
        };
        self.with_group_version(group_id, group_versions.with_update_applied(update_id))
    }

    /// Merge an event or snapshot token into this application read position.
    ///
    /// This is safe for applications that publish locally while listener
    /// events are still queued: the merge keeps the furthest-known version for
    /// every group instead of replacing newer local progress with an older
    /// event token.
    ///
    /// # Panics
    ///
    /// Panics if both tokens contain the same group with incompatible member
    /// counts.
    pub fn merge_applied(&mut self, applied: &ReadToken) {
        let versions = Arc::make_mut(&mut self.versions);
        for (group_id, applied_versions) in &applied.versions.groups {
            let merged_versions = match versions.groups.get(group_id) {
                Some(existing_versions) => existing_versions.least_upper_bound(applied_versions),
                None => applied_versions.clone(),
            };
            versions.groups.insert(*group_id, merged_versions);
        }
    }
}

impl std::fmt::Debug for ReadToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            return f
                .debug_struct("ReadToken")
                .field("groups", &self.versions.groups)
                .finish();
        }
        f.debug_struct("ReadToken")
            .field("group_count", &self.group_count())
            .finish_non_exhaustive()
    }
}

/// Request to publish one local set of row mutations from a known read token.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishChangesRequest {
    /// Opaque read position for the application state this change was based on.
    pub read_token: ReadToken,
    /// Row mutations to publish.
    pub changes: Vec<RowMutation>,
}

/// Row-level change emitted by the framework to an application listener.
pub enum RowChange {
    Upsert {
        row_id: RowId,
        row: Arc<dyn RowValueRead + Send + Sync>,
    },
    Delete {
        row_id: RowId,
    },
}

impl RowChange {
    #[must_use]
    pub fn row_id(&self) -> &RowId {
        match self {
            RowChange::Upsert { row_id, .. } | RowChange::Delete { row_id } => row_id,
        }
    }

    #[must_use]
    pub fn row(&self) -> Option<&(dyn RowValueRead + Send + Sync)> {
        match self {
            RowChange::Upsert { row, .. } => Some(row.as_ref()),
            RowChange::Delete { .. } => None,
        }
    }
}

/// One owned batch emitted by a [`BatchProvider`].
pub trait ProviderBatch: Send + 'static {
    /// Remove previously emitted rows before reusing this allocation.
    fn clear(&mut self);
    /// Return whether the batch currently contains no rows.
    fn is_empty(&self) -> bool;
}

impl<T> ProviderBatch for Vec<T>
where
    T: Send + 'static,
{
    fn clear(&mut self) {
        Vec::clear(self);
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }
}

impl<A> ProviderBatch for SmallVec<A>
where
    A: Array + Send + 'static,
    A::Item: Send + 'static,
{
    fn clear(&mut self) {
        SmallVec::clear(self);
    }

    fn is_empty(&self) -> bool {
        SmallVec::is_empty(self)
    }
}

/// Source for owned batches with explicit end-of-stream signalling.
///
/// `Some(batch)` means at least one row was emitted. `None` means the provider
/// is exhausted. `fill_batch` accepts the previous batch by value so
/// implementations may reuse, move, or hand the allocation to another thread
/// while preparing the next batch.
pub trait BatchProvider: Send {
    type Batch: ProviderBatch;

    /// Allocate a fresh empty batch using this provider's batching policy.
    fn new_batch(&self) -> Self::Batch;

    fn fill_batch(
        &mut self,
        reuse: Self::Batch,
    ) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>>;

    fn next_batch(&mut self) -> BoxFuture<'_, Result<Option<Self::Batch>, RowProviderError>> {
        let reuse = self.new_batch();
        self.fill_batch(reuse)
    }
}

/// Process all batches from `provider`, reusing each emitted batch allocation.
///
/// `process` receives the batch mutably so it can use concrete batch APIs such
/// as `drain` before the allocation is handed back to the provider.
///
/// # Errors
///
/// Returns provider errors from fetching a batch or processing errors returned
/// by `process`.
pub async fn process_batches<B>(
    provider: &mut dyn BatchProvider<Batch = B>,
    mut process: impl FnMut(&mut B) -> Result<(), RowProviderError>,
) -> Result<(), RowProviderError>
where
    B: ProviderBatch,
{
    let mut batch = provider.new_batch();
    while let Some(mut filled_batch) = provider.fill_batch(batch).await? {
        process(&mut filled_batch)?;
        batch = filled_batch;
    }
    Ok(())
}

/// Batch of row changes emitted by a [`RowProvider`].
pub type RowChangeBatch = SmallVec<[RowChange; 4]>;

/// Source for batched row-level changes carried in `ReplicationEvent::DataChanged`.
pub type RowProvider = dyn BatchProvider<Batch = RowChangeBatch>;

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
    /// Whether retained delete tombstones should be emitted with the snapshot
    /// row tombstone flag set.
    ///
    /// Application reload paths normally leave this disabled so they only
    /// rebuild visible rows.
    pub include_tombstones: bool,
}

/// Projected row-value snapshot stream returned by [`ReplicationApi::snapshot_rows`].
pub struct SnapshotValueRows {
    pub group_id: GroupId,
    pub read_token: ReadToken,
    pub rows: Box<SnapshotValueRowProvider>,
}

impl std::fmt::Debug for SnapshotValueRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotValueRows")
            .field("group_id", &self.group_id)
            .field("read_token", &self.read_token)
            .finish_non_exhaustive()
    }
}

/// One borrowed row view in a snapshot stream.
pub type SnapshotValueRow<'a> = InMemoryValueDataRowRef<'a, RowId>;

/// Compact batch of projected snapshot rows for one dataset schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotValueRowBatch {
    /// Compact value storage for this batch.
    rows: InMemoryValueData<RowId>,
}

impl SnapshotValueRowBatch {
    /// Create an empty reusable batch allocation.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            rows: InMemoryValueData::new(Schema::empty()),
        }
    }

    /// Create a batch from compact projected row storage.
    #[must_use]
    pub fn from_rows(rows: InMemoryValueData<RowId>) -> Self {
        Self { rows }
    }

    /// Prepare this batch to receive rows for `schema`, preserving allocation
    /// when the existing compact storage can be reused.
    pub fn prepare(
        &mut self,
        schema: impl Into<SchemaSource>,
        row_capacity: usize,
    ) -> &mut InMemoryValueData<RowId> {
        let schema = schema.into();
        if self.rows.schema() == schema.as_schema() {
            self.rows.clear_rows();
            self.rows.reserve_rows(row_capacity);
        } else {
            self.rows = InMemoryValueData::with_row_capacity(schema, row_capacity);
        }
        &mut self.rows
    }

    /// Return the compact projected row storage, if this batch is non-empty.
    #[must_use]
    pub fn data(&self) -> Option<&InMemoryValueData<RowId>> {
        option_when!(!self.rows.is_empty(), &self.rows)
    }

    /// Return the number of rows in this batch.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.row_count()
    }

    /// Remove all rows from this reusable batch.
    pub fn clear(&mut self) {
        self.rows.clear_rows();
    }

    /// Return whether this batch contains no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_count() == 0
    }

    /// Iterate the rows in this batch.
    pub fn rows(&self) -> impl Iterator<Item = SnapshotValueRow<'_>> {
        self.rows.rows()
    }
}

impl Default for SnapshotValueRowBatch {
    fn default() -> Self {
        Self::empty()
    }
}

impl ProviderBatch for SnapshotValueRowBatch {
    fn clear(&mut self) {
        SnapshotValueRowBatch::clear(self);
    }

    fn is_empty(&self) -> bool {
        SnapshotValueRowBatch::is_empty(self)
    }
}

/// Source for batched snapshot rows.
///
/// Snapshot providers may hold a store read transaction internally, so callers
/// should drain or drop them promptly.
pub type SnapshotValueRowProvider = dyn BatchProvider<Batch = SnapshotValueRowBatch>;

/// Request one peer's current version vector for a group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryRequest {
    /// Replication group whose progress should be reported.
    pub group_id: GroupId,
    /// Group member that should answer the request.
    pub target: MemberIdentity,
}

/// One peer's reported replication progress for a group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Summary {
    /// Replication group described by this summary.
    pub group_id: GroupId,
    /// Member that produced this summary.
    pub responder: MemberIdentity,
    /// Group version vector currently known to `responder`.
    pub has_versions: VersionVector,
}

/// One row entry in an initial dataset's value rows.
#[derive(Clone, PartialEq, Eq)]
pub struct InitialValueRow {
    pub row_key: RowKey,
    pub row: RowValues,
}

impl fmt::Debug for InitialValueRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitialValueRow")
            .field("row_key", &self.row_key)
            .field("field_count", &self.row.field_count())
            .finish()
    }
}

/// Initial value rows for one dataset.
#[derive(Clone, PartialEq, Eq)]
pub struct InitialDatasetValueRows {
    pub dataset_id: DatasetId,
    pub rows: Vec<InitialValueRow>,
}

impl fmt::Debug for InitialDatasetValueRows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitialDatasetValueRows")
            .field("dataset_id", &self.dataset_id)
            .field("row_count", &self.rows.len())
            .finish()
    }
}

/// Initial group value rows grouped by dataset.
//
// TODO(flotsync-git-d3w): Ensure all nodes deterministically derive equivalent
// initial CRDT state from these values during group creation/migration. If data
// types such as LinearString would diverge on generated ids, redesign this to
// carry state instead of values.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct InitialGroupValueRows {
    pub datasets: Vec<InitialDatasetValueRows>,
}

impl InitialGroupValueRows {
    fn row_count(&self) -> usize {
        self.datasets.iter().map(|dataset| dataset.rows.len()).sum()
    }
}

impl fmt::Debug for InitialGroupValueRows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitialGroupValueRows")
            .field("dataset_count", &self.datasets.len())
            .field("row_count", &self.row_count())
            .finish()
    }
}

/// Fixed schema for one dataset in a replication group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetSchema {
    /// Dataset governed by this schema.
    pub dataset_id: DatasetId,
    /// Schema fixed for `dataset_id` for the lifetime of its group.
    pub schema: SchemaSource,
}

/// Dataset schemas fixed for the lifetime of a replication group.
///
/// Schema changes require migrating to a new group.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct GroupSchema {
    /// Schemas keyed by dataset.
    datasets: HashMap<DatasetId, SchemaSource>,
}

impl GroupSchema {
    /// Build a group schema from per-dataset schemas keyed by dataset id.
    #[must_use]
    pub fn new(datasets: HashMap<DatasetId, SchemaSource>) -> Self {
        Self { datasets }
    }

    /// Insert a repeated schema entry while rejecting duplicate dataset ids.
    ///
    /// This is intended for codecs that decode repeated protobuf schema entries.
    /// Ordinary callers should construct [`GroupSchema`] from a map instead.
    ///
    /// # Errors
    ///
    /// Returns [`GroupSchemaError::DuplicateDataset`] when this schema already
    /// contains an entry for `dataset_schema.dataset_id`.
    pub fn insert_checked(
        &mut self,
        dataset_schema: DatasetSchema,
    ) -> Result<(), GroupSchemaError> {
        match self.datasets.entry(dataset_schema.dataset_id) {
            Entry::Vacant(entry) => {
                entry.insert(dataset_schema.schema);
                Ok(())
            }
            Entry::Occupied(entry) => {
                let dataset_id = entry.key().clone();
                group_schema::DuplicateDatasetSnafu { dataset_id }.fail()
            }
        }
    }

    /// Return the per-dataset schema entries in deterministic dataset-id order.
    #[must_use]
    pub fn datasets(&self) -> Vec<DatasetSchema> {
        let mut datasets = self
            .datasets
            .iter()
            .map(|(dataset_id, schema)| DatasetSchema {
                dataset_id: dataset_id.clone(),
                schema: schema.clone(),
            })
            .collect::<Vec<_>>();
        datasets.sort_by(|left, right| left.dataset_id.cmp(&right.dataset_id));
        datasets
    }

    /// Return the schema for `dataset_id`, if this group declares it.
    #[must_use]
    pub fn schema(&self, dataset_id: &DatasetId) -> Option<&SchemaSource> {
        self.datasets.get(dataset_id)
    }

    /// Return the number of dataset schemas in this group schema.
    #[must_use]
    pub fn len(&self) -> usize {
        self.datasets.len()
    }

    /// Return whether this group schema has no dataset schemas.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.datasets.is_empty()
    }
}

impl fmt::Debug for GroupSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alternate = f.alternate();
        let mut debug = f.debug_struct("GroupSchema");
        if alternate {
            debug.field("datasets", &self.datasets());
        } else {
            debug.field("dataset_count", &self.datasets.len());
        }
        debug.finish()
    }
}

/// Failed construction of a group schema from repeated entries.
#[derive(Debug, Snafu)]
#[snafu(module(group_schema))]
pub enum GroupSchemaError {
    /// The repeated schema entries listed the same dataset more than once.
    #[snafu(display("Group schema contained duplicate dataset id '{dataset_id}'."))]
    DuplicateDataset { dataset_id: DatasetId },
}

/// Group/version reference for one materialised snapshot.
///
/// Multiple references may identify the same logical snapshot. During
/// migration, the old group's `final_versions` and the new group's zero vector
/// are expected to be equivalent references to the initial target-group state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRef {
    /// Group whose version vector names the snapshot state.
    pub group_id: GroupId,
    /// Version vector that identifies the materialised state in `group_id`.
    pub versions: VersionVector,
}

/// Initial dataset contents required before a joined or migrated group becomes
/// externally active.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum InitialSnapshot {
    /// The initial dataset contents are empty.
    ///
    /// This does not imply that the group was newly created.
    #[default]
    Empty,
    /// The initial value rows are carried directly in the invitation or proposal.
    Inline(InitialGroupValueRows),
    /// The initial contents must be resolved from metadata before activation.
    Metadata(InitialSnapshotMetadata),
}

impl fmt::Debug for InitialSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("InitialSnapshot::Empty"),
            Self::Inline(state) => f
                .debug_struct("InitialSnapshot::Inline")
                .field("dataset_count", &state.datasets.len())
                .field("row_count", &state.row_count())
                .finish(),
            Self::Metadata(metadata) => f
                .debug_tuple("InitialSnapshot::Metadata")
                .field(metadata)
                .finish(),
        }
    }
}

impl InitialSnapshot {
    /// Return whether activation needs snapshot data that is not carried inline.
    #[must_use]
    pub fn requires_snapshot_fetch(&self) -> bool {
        matches!(self, Self::Metadata(_))
    }
}

/// Metadata for an initial snapshot that is not carried inline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialSnapshotMetadata {
    /// Primary reference the recipient should use when reasoning about this snapshot.
    pub primary_ref: SnapshotRef,
    /// Other group/version references that identify the same logical snapshot.
    ///
    /// Not required to be exhaustive.
    pub equivalent_refs: SmallVec<[SnapshotRef; 1]>,
    /// Optional number of row records expected in the snapshot.
    pub record_count: Option<u64>,
}

/// Policy decision for one invitation or migration classification.
///
/// The enum order is the restrictiveness order: automatic acceptance is the
/// least restrictive outcome, and automatic rejection is the most restrictive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolicyDecision {
    /// Accept automatically after validation succeeds.
    AutoAccept,
    /// Forward to the application listener for an explicit decision.
    AskListener,
    /// Reject automatically after validation succeeds.
    AutoReject,
}

impl PolicyDecision {
    /// Return the more restrictive of two policy decisions.
    #[must_use]
    pub fn most_restrictive(self, other: Self) -> Self {
        self.max(other)
    }
}

/// Policy for handling group invitations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupInvitationPolicy {
    /// Applied to ordinary group invitations.
    pub creation: PolicyDecision,
    /// Applied when this member is added through a migration and receives the
    /// new group as an invitation rather than as an old-group proposal.
    pub migration_added_member: PolicyDecision,
}

impl Default for GroupInvitationPolicy {
    fn default() -> Self {
        Self {
            creation: PolicyDecision::AskListener,
            migration_added_member: PolicyDecision::AskListener,
        }
    }
}

/// Policy for handling membership migration proposals.
///
/// The current inline migration flow classifies top-level member additions and
/// removals. Device-level and local-member-removal classifications are reserved
/// for later identity and local-removal flows.
///
/// If more than one supported behaviour applies to a change, the most
/// restrictive behaviour is used.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMigrationPolicy {
    /// Applied if the migration contains only an epoch change, so that the group does not run out
    /// of change versions.
    pub epoch_change: PolicyDecision,
    /// Applied when a new top-level member was added to the group.
    pub member_added: PolicyDecision,
    /// Reserved for classifying a new device under an existing top-level member.
    pub member_device_added: PolicyDecision,
    /// Applied when a top-level member is removed from the group.
    pub member_removed: PolicyDecision,
    /// Reserved for classifying removal of a device under an existing member.
    pub member_device_removed: PolicyDecision,
    /// Reserved for a future flow that informs a removed local member.
    ///
    /// A migration proposal delivered to this runtime must include its local
    /// member in the proposed group.
    pub local_member_removed: PolicyDecision,
}

impl Default for GroupMigrationPolicy {
    fn default() -> Self {
        Self {
            epoch_change: PolicyDecision::AutoAccept,
            member_added: PolicyDecision::AskListener,
            member_device_added: PolicyDecision::AutoAccept,
            member_removed: PolicyDecision::AskListener,
            member_device_removed: PolicyDecision::AutoAccept,
            local_member_removed: PolicyDecision::AskListener,
        }
    }
}

/// Local access policy after a future standalone group-close signal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum GroupClosePolicy {
    /// Keep local external access unchanged and only record/observe the signal.
    ObserveOnly,
    /// Reject new external writes while keeping external reads available.
    #[default]
    CloseLocalWrites,
    /// Reject external reads and writes.
    ///
    /// Runtime internals may still read stored updates or snapshots for
    /// activation, catch-up, or diagnostics.
    CloseReadsAndWrites,
}

/// Runtime configuration passed during `load`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// Policy used to derive runtime permissions from stored trust evidence.
    pub trust_policy: TrustPolicy,
    /// Policy for handling group invitations.
    pub group_invitation_policy: GroupInvitationPolicy,
    /// Policy for handling group membership migration proposals.
    pub group_migration_policy: GroupMigrationPolicy,
    /// Local access policy reserved for the future standalone group-close flow.
    pub group_close_policy: GroupClosePolicy,
}

/// Device-local security input required while loading one replication runtime.
///
/// This value is intentionally separate from [`ReplicationConfig`]: it carries
/// secret-bearing key material for the current process/device, while
/// `ReplicationConfig` contains cloneable policy knobs that are safe to compare
/// and log normally.
#[derive(Clone, Debug)]
pub struct ReplicationSecuritySecrets {
    store_secret_key_id: StoreSecretKeyId,
    store_secret_key: Arc<StoreSecretKey>,
}

impl ReplicationSecuritySecrets {
    /// Load the existing device-local store secret for an application/profile.
    ///
    /// The profile is scoped by application id, not by member identity. This
    /// keeps the API ready for stores that host multiple local identities under
    /// the same encrypted-store secret.
    ///
    /// # Errors
    ///
    /// Returns [`LoadSecurityError::LocalStoreSecret`] if the local secret slot
    /// is missing, inaccessible, or malformed.
    pub fn load_local(
        application_id: &Identifier,
        profile: &LocalStoreSecretProfile,
    ) -> Result<Self, LoadSecurityError> {
        let secret =
            load_local_store_secret(application_id, profile).context(LocalStoreSecretSnafu)?;
        Ok(Self::from_loaded_local_store_secret(secret))
    }

    /// Load or first-run initialise the device-local store secret.
    ///
    /// # Errors
    ///
    /// Returns [`LoadSecurityError::LocalStoreSecret`] if the local secret slot
    /// cannot be loaded or created.
    pub fn load_or_create_local(
        application_id: &Identifier,
        profile: &LocalStoreSecretProfile,
    ) -> Result<Self, LoadSecurityError> {
        let secret = load_or_create_local_store_secret(application_id, profile)
            .context(LocalStoreSecretSnafu)?;
        Ok(Self::from_loaded_local_store_secret(secret))
    }

    /// Build runtime security input from caller-managed store-secret material.
    ///
    /// This bypasses the platform local-secret store. Prefer
    /// [`Self::load_or_create_local`] unless the caller intentionally manages
    /// the local store secret through another mechanism.
    ///
    /// TODO(flotsync-lsi8): Re-evaluate or remove this escape hatch once
    /// headless local store-secret backends can provide managed secrets.
    #[doc(hidden)]
    #[must_use]
    pub fn from_unmanaged_store_secret(
        store_secret_key_id: StoreSecretKeyId,
        store_secret_key: StoreSecretKey,
    ) -> Self {
        Self {
            store_secret_key_id,
            store_secret_key: Arc::new(store_secret_key),
        }
    }

    /// Build runtime security input from a shared store-secret key handle.
    ///
    /// This is intentionally test-support only: some security validation tests
    /// need to construct deliberately mismatched or malformed key inputs.
    #[must_use]
    #[cfg(any(test, feature = "test-support"))]
    pub fn new(
        store_secret_key_id: StoreSecretKeyId,
        store_secret_key: Arc<StoreSecretKey>,
    ) -> Self {
        Self {
            store_secret_key_id,
            store_secret_key,
        }
    }

    fn from_loaded_local_store_secret(secret: flotsync_security::LoadedLocalStoreSecret) -> Self {
        let (key_id, store_secret_key) = secret.into_parts();
        Self {
            store_secret_key_id: key_id,
            store_secret_key: Arc::new(store_secret_key),
        }
    }

    /// Return the generated store-secret key id expected by encrypted cells.
    #[must_use]
    pub fn store_secret_key_id(&self) -> &StoreSecretKeyId {
        &self.store_secret_key_id
    }

    /// Return the shared store-secret key handle for internal runtime security loading.
    pub(crate) fn store_secret_key(&self) -> &Arc<StoreSecretKey> {
        &self.store_secret_key
    }
}

/// Request to create a new replication group.
#[derive(Clone, PartialEq, Eq)]
pub struct CreateGroupRequest {
    pub members: Vec<MemberIdentity>,
    /// Dataset schemas fixed for the lifetime of the new group.
    pub group_schema: GroupSchema,
}

impl fmt::Debug for CreateGroupRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateGroupRequest")
            .field("members", &self.members)
            .field("group_schema", &self.group_schema)
            .finish()
    }
}

/// Request to change membership of an existing group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeGroupMembershipRequest {
    pub group_id: GroupId,
    pub add_members: HashSet<MemberIdentity>,
    pub remove_members: HashSet<MemberIdentity>,
    pub group_name: Option<String>,
    pub message: Option<String>,
}

/// Receipt returned by `publish_changes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishReceipt {
    pub update_id: UpdateId,
    pub read_token: ReadToken,
}

/// Source that explains why a group invitation was received.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupInvitationSource {
    /// Ordinary group creation or invitation into an existing group.
    Creation,
    /// The local member is joining the new group as part of a migration.
    Migration { migration_id: MigrationId },
}

/// Invitation details surfaced when group-invitation policy requires listener mediation.
#[derive(Clone, PartialEq, Eq)]
pub struct GroupInvitation {
    /// Group the recipient is being invited to activate.
    pub group_id: GroupId,
    /// Local perspective for this invitation.
    pub source: GroupInvitationSource,
    /// Proposed canonical member order for the invited group.
    pub proposed_members: Vec<MemberIdentity>,
    /// Dataset schemas fixed for the lifetime of the invited group.
    pub group_schema: GroupSchema,
    /// Initial dataset state required before the invited group becomes active.
    pub initial_snapshot: InitialSnapshot,
    /// Optional display name supplied by the proposer.
    pub group_name: Option<String>,
    /// Optional user-facing note supplied by the proposer.
    pub message: Option<String>,
}

impl fmt::Debug for GroupInvitation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupInvitation")
            .field("group_id", &self.group_id)
            .field("source", &self.source)
            .field("proposed_member_count", &self.proposed_members.len())
            .field("group_schema", &self.group_schema)
            .field("initial_snapshot", &self.initial_snapshot)
            .field("has_group_name", &self.group_name.is_some())
            .field("has_message", &self.message.is_some())
            .finish()
    }
}

/// Failed construction of a group invitation.
#[derive(Debug, Snafu)]
#[snafu(module(group_invitation))]
pub enum GroupInvitationError {
    /// A migration invitation named a target group different from its migration id.
    #[snafu(display(
        "Migration invitation group id {group_id} did not match migration target group {new_group_id}."
    ))]
    GroupMismatch {
        group_id: GroupId,
        new_group_id: GroupId,
    },
}

impl GroupInvitation {
    /// Build an ordinary group invitation.
    #[must_use]
    pub fn new_creation(
        group_id: GroupId,
        proposed_members: Vec<MemberIdentity>,
        group_schema: GroupSchema,
        initial_snapshot: InitialSnapshot,
        group_name: Option<String>,
        message: Option<String>,
    ) -> Self {
        Self {
            group_id,
            source: GroupInvitationSource::Creation,
            proposed_members,
            group_schema,
            initial_snapshot,
            group_name,
            message,
        }
    }

    /// Build a migration-sourced group invitation.
    ///
    /// The invitation's `group_id` is derived from `migration_id.new_group_id` so
    /// the duplicated target-group identity cannot diverge.
    #[must_use]
    pub fn new_migration(
        migration_id: MigrationId,
        proposed_members: Vec<MemberIdentity>,
        group_schema: GroupSchema,
        initial_snapshot: InitialSnapshot,
        group_name: Option<String>,
        message: Option<String>,
    ) -> Self {
        Self {
            group_id: migration_id.new_group_id,
            source: GroupInvitationSource::Migration { migration_id },
            proposed_members,
            group_schema,
            initial_snapshot,
            group_name,
            message,
        }
    }

    /// Build a group invitation from explicit API parts.
    ///
    /// This validates that migration-sourced invitations name the migration's
    /// target group as `group_id`.
    ///
    /// # Errors
    ///
    /// Returns [`GroupInvitationError::GroupMismatch`] when `source` is
    /// migration-sourced and `group_id` differs from `source.migration_id.new_group_id`.
    pub fn try_new(
        group_id: GroupId,
        source: GroupInvitationSource,
        proposed_members: Vec<MemberIdentity>,
        group_schema: GroupSchema,
        initial_snapshot: InitialSnapshot,
        group_name: Option<String>,
        message: Option<String>,
    ) -> Result<Self, GroupInvitationError> {
        match source {
            GroupInvitationSource::Creation => Ok(Self::new_creation(
                group_id,
                proposed_members,
                group_schema,
                initial_snapshot,
                group_name,
                message,
            )),
            GroupInvitationSource::Migration { migration_id } => {
                ensure!(
                    group_id == migration_id.new_group_id,
                    group_invitation::GroupMismatchSnafu {
                        group_id,
                        new_group_id: migration_id.new_group_id,
                    }
                );
                Ok(Self::new_migration(
                    migration_id,
                    proposed_members,
                    group_schema,
                    initial_snapshot,
                    group_name,
                    message,
                ))
            }
        }
    }
}

/// Old-group member proposal surfaced when migration policy requires listener mediation.
#[derive(Clone, PartialEq, Eq)]
pub struct MigrationProposal {
    /// Old-to-new group migration being proposed.
    pub migration_id: MigrationId,
    /// Old-group final externally writable version vector for this proposal.
    pub final_versions: VersionVector,
    /// Proposed canonical member order for the new group.
    pub proposed_members: Vec<MemberIdentity>,
    /// Dataset schemas fixed for the lifetime of the new group.
    pub group_schema: GroupSchema,
    /// Initial dataset state required before the new group becomes active.
    pub initial_snapshot: InitialSnapshot,
    /// Optional display name supplied by the proposer.
    pub group_name: Option<String>,
    /// Optional user-facing note supplied by the proposer.
    pub message: Option<String>,
}

impl fmt::Debug for MigrationProposal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationProposal")
            .field("migration_id", &self.migration_id)
            .field("final_versions", &self.final_versions)
            .field("proposed_member_count", &self.proposed_members.len())
            .field("group_schema", &self.group_schema)
            .field("initial_snapshot", &self.initial_snapshot)
            .field("has_group_name", &self.group_name.is_some())
            .field("has_message", &self.message.is_some())
            .finish()
    }
}

/// Stable identity for pending group decision or activation work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PendingGroupWorkKey {
    /// Decision for a group invitation.
    GroupInvitation {
        /// Group being invited or activated.
        group_id: GroupId,
        /// Local source perspective for the invitation.
        source: GroupInvitationSource,
    },
    /// Decision for an old-group migration proposal.
    MigrationProposal {
        /// Old-to-new group migration being proposed.
        migration_id: MigrationId,
    },
}

impl PendingGroupWorkKey {
    /// Return the target group for the pending work.
    #[must_use]
    pub const fn group_id(self) -> GroupId {
        match self {
            Self::GroupInvitation { group_id, .. } => group_id,
            Self::MigrationProposal { migration_id } => migration_id.new_group_id,
        }
    }
}

/// Store-owned unresolved listener-mediated group decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingGroupDecisionRecord {
    /// Pending group invitation decision.
    GroupInvitation(GroupInvitation),
    /// Pending migration proposal decision.
    MigrationProposal(MigrationProposal),
}

impl PendingGroupDecisionRecord {
    /// Return the stable idempotence key for this pending decision.
    #[must_use]
    pub const fn key(&self) -> PendingGroupWorkKey {
        match self {
            Self::GroupInvitation(invitation) => PendingGroupWorkKey::GroupInvitation {
                group_id: invitation.group_id,
                source: invitation.source,
            },
            Self::MigrationProposal(proposal) => PendingGroupWorkKey::MigrationProposal {
                migration_id: proposal.migration_id,
            },
        }
    }

    /// Return the target group for this pending decision.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.key().group_id()
    }

    /// Return whether accepting this decision requires fetching snapshot data first.
    #[must_use]
    pub fn requires_snapshot_fetch(&self) -> bool {
        match self {
            Self::GroupInvitation(invitation) => {
                invitation.initial_snapshot.requires_snapshot_fetch()
            }
            Self::MigrationProposal(proposal) => {
                proposal.initial_snapshot.requires_snapshot_fetch()
            }
        }
    }

    /// Return proposed target-group members in canonical order.
    #[must_use]
    pub fn proposed_members(&self) -> &[MemberIdentity] {
        match self {
            Self::GroupInvitation(invitation) => &invitation.proposed_members,
            Self::MigrationProposal(proposal) => &proposal.proposed_members,
        }
    }

    /// Return the target-group schema carried by this work.
    #[must_use]
    pub const fn group_schema(&self) -> &GroupSchema {
        match self {
            Self::GroupInvitation(invitation) => &invitation.group_schema,
            Self::MigrationProposal(proposal) => &proposal.group_schema,
        }
    }

    /// Convert this pending decision into the listener event that asks for the decision.
    #[must_use]
    pub fn to_event<R>(self, responder: R) -> ReplicationEvent
    where
        R: GroupInvitationResponder + MigrationProposalResponder + 'static,
    {
        match self {
            Self::GroupInvitation(invitation) => ReplicationEvent::GroupInvitation {
                invitation,
                respond: Box::new(responder),
            },
            Self::MigrationProposal(proposal) => ReplicationEvent::MigrationProposals {
                proposals: smallvec![MigrationCandidateProposal {
                    proposal,
                    respond: Box::new(responder),
                }],
            },
        }
    }

    /// Convert an accepted pending decision into accepted activation work.
    #[must_use]
    pub fn into_activation(self) -> PendingGroupActivationRecord {
        match self {
            Self::GroupInvitation(invitation) => {
                PendingGroupActivationRecord::GroupInvitation(invitation)
            }
            Self::MigrationProposal(proposal) => {
                PendingGroupActivationRecord::MigrationProposal(proposal)
            }
        }
    }
}

/// Store-owned accepted group work that is not externally active yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingGroupActivationRecord {
    /// Accepted group invitation activation work.
    GroupInvitation(GroupInvitation),
    /// Accepted migration proposal activation work.
    MigrationProposal(MigrationProposal),
}

impl PendingGroupActivationRecord {
    /// Return the stable idempotence key for this pending activation.
    #[must_use]
    pub const fn key(&self) -> PendingGroupWorkKey {
        match self {
            Self::GroupInvitation(invitation) => PendingGroupWorkKey::GroupInvitation {
                group_id: invitation.group_id,
                source: invitation.source,
            },
            Self::MigrationProposal(proposal) => PendingGroupWorkKey::MigrationProposal {
                migration_id: proposal.migration_id,
            },
        }
    }

    /// Return the new group activated by this record.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.key().group_id()
    }

    /// Return whether activation requires fetching snapshot data first.
    #[must_use]
    pub fn requires_snapshot_fetch(&self) -> bool {
        match self {
            Self::GroupInvitation(invitation) => {
                invitation.initial_snapshot.requires_snapshot_fetch()
            }
            Self::MigrationProposal(proposal) => {
                proposal.initial_snapshot.requires_snapshot_fetch()
            }
        }
    }

    /// Split this record into the fields required by the activation pipeline.
    #[must_use]
    pub fn into_activation_record(self) -> AcceptedGroupActivationRecord {
        let key = self.key();
        match self {
            Self::GroupInvitation(invitation) => AcceptedGroupActivationRecord {
                key,
                group_id: invitation.group_id,
                migration_cutover: None,
                proposed_members: invitation.proposed_members,
                group_schema: invitation.group_schema,
                initial_snapshot: invitation.initial_snapshot,
            },
            Self::MigrationProposal(proposal) => AcceptedGroupActivationRecord {
                key,
                group_id: proposal.migration_id.new_group_id,
                migration_cutover: Some(AcceptedMigrationCutover {
                    old_group_id: proposal.migration_id.old_group_id,
                    final_versions: proposal.final_versions,
                }),
                proposed_members: proposal.proposed_members,
                group_schema: proposal.group_schema,
                initial_snapshot: proposal.initial_snapshot,
            },
        }
    }
}

/// Decomposed accepted group work ready for activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedGroupActivationRecord {
    /// Stable store key for this accepted activation.
    pub key: PendingGroupWorkKey,
    /// New group that becomes externally active after activation succeeds.
    pub group_id: GroupId,
    /// Old-group cutover performed after target activation, when applicable.
    pub migration_cutover: Option<AcceptedMigrationCutover>,
    /// Proposed canonical member order for the activated group.
    pub proposed_members: Vec<MemberIdentity>,
    /// Dataset schemas fixed for the activated group.
    pub group_schema: GroupSchema,
    /// Initial dataset state required before the group becomes active.
    pub initial_snapshot: InitialSnapshot,
}

/// Immutable old-group closure context retained by accepted migration work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedMigrationCutover {
    /// Existing group that becomes closed after target activation.
    pub old_group_id: GroupId,
    /// Replay cut fixed by the accepted migration proposal.
    pub final_versions: VersionVector,
}

/// Listener-visible replication events.
#[allow(
    clippy::large_enum_variant,
    reason = "Listener events are infrequent, and keeping invitation values direct avoids public API indirection."
)]
pub enum ReplicationEvent {
    DataChanged {
        /// Read position reached by the row changes in this event.
        ///
        /// Applications that keep local mutable state should merge this into
        /// their stored [`ReadToken`] after applying all rows from the event.
        /// This avoids replacing newer local publish progress with an older
        /// listener token when local and inbound events are consumed out of
        /// order.
        read_token: ReadToken,
        rows: Box<RowProvider>,
    },
    GroupInvitation {
        invitation: GroupInvitation,
        respond: Box<dyn GroupInvitationResponder>,
    },
    /// Current undecided migration candidates for one old group.
    MigrationProposals {
        /// Complete candidate set known when this event was emitted.
        proposals: SmallVec<[MigrationCandidateProposal; 1]>,
    },
}

/// One listener-mediated candidate within a grouped migration proposal event.
pub struct MigrationCandidateProposal {
    /// Proposed target group and immutable old-group replay cut.
    pub proposal: MigrationProposal,
    /// One-shot response for this specific candidate.
    pub respond: Box<dyn MigrationProposalResponder>,
}

/// Callback for applications to accept or reject one group invitation.
///
/// The callback is one-shot by construction: calling either method consumes the responder.
pub trait GroupInvitationResponder: Send {
    /// Accept the invitation into local activation state.
    ///
    /// Empty and inline initial snapshots activate before this call succeeds.
    /// Metadata-only snapshots are not fetchable by the current runtime and
    /// therefore cannot be accepted yet.
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>>;

    /// Refuse to join the invited group.
    fn reject(self: Box<Self>, reason: RejectionReason)
    -> BoxFuture<'static, Result<(), ApiError>>;
}

/// Callback for applications to accept or reject one migration proposal.
///
/// The callback is one-shot by construction: calling either method consumes the responder.
pub trait MigrationProposalResponder: Send {
    /// Accept the proposal into local migration activation state.
    ///
    /// Empty and inline initial snapshots activate the new group before this
    /// call succeeds. Metadata-only snapshots are not fetchable by the current
    /// runtime and therefore cannot be accepted yet.
    fn accept(self: Box<Self>) -> BoxFuture<'static, Result<(), ApiError>>;

    /// Refuse the proposed migration.
    fn reject(self: Box<Self>, reason: RejectionReason)
    -> BoxFuture<'static, Result<(), ApiError>>;
}

/// Optional rejection reasons reported back to the framework.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RejectionReason {
    PolicyDenied,
    UserDenied,
    UnsupportedInitialSnapshot,
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
    /// Shut this runtime down gracefully.
    ///
    /// Shutdown stops the live replication components in dependency order and
    /// then shuts down the underlying runtime system. After shutdown starts,
    /// other API calls on any clone of this runtime handle report
    /// [`ApiError::RuntimeUnavailable`]. Calling shutdown again after a
    /// previous shutdown completed is a no-op.
    fn shutdown(&self) -> BoxFuture<'_, Result<(), ApiError>>;

    /// Return the local member's shareable identity-free public key bundle.
    ///
    /// Applications can encode this bundle for transfer to another user or
    /// device. The returned key material is public; it does not include member
    /// identity, local trust evidence, private keys, or policy state.
    fn local_public_key_bundle(&self) -> BoxFuture<'_, Result<PublicKeyBundle, ApiError>>;

    /// Assess one decoded public key bundle against the running system's local security state.
    ///
    /// Read-only assessment does not store observed key material, record trust
    /// evidence, or change blocked-key state. Requests may instead ask the
    /// running system to store candidate member bindings before reporting, still
    /// without adding trust evidence. Applications use the returned report to
    /// present fingerprint, known identity bindings, local trust, blocked status,
    /// and authority decisions before recording user feedback.
    fn assess_public_key_bundle(
        &self,
        request: security::AssessPublicKeyBundleRequest,
    ) -> BoxFuture<'_, Result<security::PublicKeyBundleReport, ApiError>>;

    /// Record user/application feedback for one decoded public key bundle.
    ///
    /// Trust feedback records local trust for an explicit member identity plus
    /// the bundle fingerprint. Block feedback records the fingerprint as
    /// globally blocked.
    fn record_public_key_bundle_feedback(
        &self,
        request: security::RecordPublicKeyBundleFeedbackRequest,
    ) -> BoxFuture<'_, Result<(), ApiError>>;

    /// Publish one local set of row mutations from a known read token.
    ///
    /// The request token is the read position of the application state used to
    /// decide the mutation list. Mutations are interpreted as sparse field
    /// patches for each [`RowMutation::Upsert`]:
    /// fields omitted from the submitted [`RowValuesPatch`] are intentionally left
    /// unchanged in the current local state. [`RowMutation::Delete`] records a
    /// replicated row tombstone rather than physically removing the row from the
    /// store.
    ///
    /// On success, the runtime has applied the mutations transactionally to the
    /// local store, persisted the corresponding replication update, notified the
    /// local [`ReplicationEventListener`] with the locally applied row changes,
    /// and submitted a best-effort live update broadcast to currently configured
    /// peers. The returned [`PublishReceipt`] identifies the local update that
    /// was recorded in the store.
    ///
    /// The method returns [`ApiError`] if validation, local apply, store
    /// access, listener notification, or runtime availability fails. Failed calls
    /// must not be treated as partially published by applications; callers should
    /// keep their own pending changes until a receipt is returned.
    fn publish_changes(
        &self,
        request: PublishChangesRequest,
    ) -> BoxFuture<'_, Result<PublishReceipt, ApiError>>;

    /// Open a batched stream over the latest locally stored projected row values for selected datasets.
    ///
    /// The request is scoped to one replication group and an explicit set of
    /// application datasets. The stream reflects the latest state known to the
    /// local store when the snapshot is opened; it does not wait for remote peers
    /// and it does not perform catch-up. When `include_tombstones` is false, the
    /// provider should emit only application-visible rows. When it is true,
    /// retained delete tombstones are emitted as [`SnapshotValueRow`] views with
    /// [`SnapshotValueRow::is_tombstoned`] set.
    ///
    /// The returned [`SnapshotValueRows`] provider may hold a store read transaction
    /// while it is alive, so callers should drain or drop it promptly. Batches
    /// are bounded by [`SnapshotRowsRequest::max_rows_per_batch`] and are emitted
    /// through the same [`BatchProvider`] end-of-stream contract as listener row
    /// providers.
    ///
    /// The method returns [`ApiError`] when the group is unknown, the request is
    /// invalid, the runtime is unavailable, or the store cannot open the
    /// snapshot.
    fn snapshot_rows(
        &self,
        request: SnapshotRowsRequest,
    ) -> BoxFuture<'_, Result<SnapshotValueRows, ApiError>>;

    /// Ask one group member for its current group version vector.
    fn request_summary(&self, request: SummaryRequest) -> BoxFuture<'_, Result<Summary, ApiError>>;

    /// Create one new fixed-membership replication group rooted at this member.
    ///
    /// `req.members` defines the canonical member order for the new group and
    /// must include the local member. On success, the runtime stores the
    /// group record, installs it in the live runtime view, broadcasts bootstrap
    /// messages to the configured remote members, and returns the newly allocated
    /// [`GroupId`]. New groups are created empty; callers publish initial
    /// dataset contents through ordinary [`Self::publish_changes`] updates.
    ///
    /// The method returns [`ApiError`] when membership validation fails, the
    /// runtime is unavailable, or group storage fails.
    fn create_group(&self, req: CreateGroupRequest) -> BoxFuture<'_, Result<GroupId, ApiError>>;

    /// Request a membership migration for an existing replication group.
    ///
    /// The runtime derives the migration state from `req.group_id`, creates and
    /// locally activates a new group for the requested member set, and sends
    /// recipient-protected setup messages. Applications do not supply the
    /// initial snapshot.
    ///
    /// Continuing remote members receive old-group-scoped migration proposals;
    /// newly added members receive new-group-scoped invitations with migration
    /// source context.
    ///
    /// Receiver-side mediation is governed by [`GroupInvitationPolicy`] and
    /// [`GroupMigrationPolicy`].
    ///
    /// The old group becomes read-only when the migration is accepted and
    /// becomes closed when local target-group activation commits. Closed groups
    /// remain available to the replication protocol for bounded catch-up, but
    /// no longer interact with the application.
    ///
    /// # Errors
    ///
    /// Returns [`ApiError`] when the old group is unknown or invalid, the
    /// requested members exclude the local member, migration preparation or
    /// activation fails, listener notification fails, or the runtime is
    /// unavailable.
    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<MigrationId, ApiError>>;
}

/// Current encrypted-store-secret setup for the active security-storage slice.
///
/// Version `1` denotes XChaCha20-Poly1305 with random nonces and authenticated
/// data chosen by the encryption boundary. Store implementations persist the
/// numeric value opaquely and must not derive cipher behaviour from it.
pub(crate) const STORE_SECRET_CRYPTO_V1: StoreSecretCryptoVersion =
    StoreSecretCryptoVersion::new(1);

/// Nonce width used by [`STORE_SECRET_CRYPTO_V1`] implementation.
const STORE_SECRET_CRYPTO_NONCE_LENGTH_V1: usize = 24;

/// Canonical ordered member-key set for one persisted replication group.
#[derive(Clone)]
pub struct GroupMemberKeys {
    /// Exact member-key bindings in canonical group order.
    ordered_member_keys: Vec<MemberKeyId>,
    /// Member identity to canonical index lookup derived from `ordered_member_keys`.
    member_indices: TrieMap<MemberIndex>,
}

impl GroupMemberKeys {
    /// Build one indexed member-key set from canonical group member-key order.
    ///
    /// # Errors
    ///
    /// Returns [`GroupMembersError`] when the member-key list is empty, contains
    /// duplicate member identities, or exceeds the supported member count.
    ///
    /// # Panics
    ///
    /// Panics only if a member index cannot be found after the same member keys
    /// were already accepted by [`GroupMembers`].
    pub fn from_ordered_member_keys(
        ordered_member_keys: impl IntoIterator<Item = MemberKeyId>,
    ) -> Result<Self, GroupMembersError> {
        let ordered_member_keys: Vec<_> = ordered_member_keys.into_iter().collect();
        let members = GroupMembers::from_ordered_members(
            ordered_member_keys
                .iter()
                .map(|member_key| member_key.member_id.clone()),
        )?;
        let mut member_indices = TrieMap::new();
        for member_key in &ordered_member_keys {
            let member_index = members
                .member_index(&member_key.member_id)
                .expect("member index exists after building members from the same keys");
            member_indices.insert(member_key.member_id.clone(), member_index);
        }
        Ok(Self {
            ordered_member_keys,
            member_indices,
        })
    }

    /// Return the exact member-key bindings in canonical group order.
    #[must_use]
    pub fn ordered_member_keys(&self) -> &[MemberKeyId] {
        &self.ordered_member_keys
    }

    /// Return the member identities in canonical group order.
    #[must_use]
    pub fn member_ids(&self) -> impl ExactSizeIterator<Item = &MemberIdentity> + '_ {
        self.ordered_member_keys
            .iter()
            .map(|member_key| &member_key.member_id)
    }

    /// Convert this exact key set into an identity-only indexed group view.
    ///
    /// # Errors
    ///
    /// See [`GroupMembersError`] for failure conditions. Construction should
    /// only fail if this value was built from inconsistent internal state.
    pub fn to_group_members(&self) -> Result<GroupMembers, GroupMembersError> {
        GroupMembers::from_ordered_members(self.member_ids().cloned())
    }

    /// Return whether this group contains `member_id`.
    #[must_use]
    pub fn contains_member(&self, member_id: &MemberIdentity) -> bool {
        self.member_indices.get(member_id).is_some()
    }

    /// Return the canonical member index assigned to `member_id`, if present.
    #[must_use]
    pub fn member_index(&self, member_id: &MemberIdentity) -> Option<MemberIndex> {
        self.member_indices.get(member_id).copied()
    }

    /// Return the exact key binding for `member_id`, if present.
    #[must_use]
    pub fn member_key(&self, member_id: &MemberIdentity) -> Option<&MemberKeyId> {
        let member_index = self.member_index(member_id)?;
        self.member_key_at_index(member_index)
    }

    /// Return the exact key binding assigned to one canonical group index.
    #[must_use]
    pub fn member_key_at_index(&self, index: MemberIndex) -> Option<&MemberKeyId> {
        self.ordered_member_keys.get(index.as_u32() as usize)
    }

    /// Return whether this member-key set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ordered_member_keys.is_empty()
    }

    /// Return the number of exact member-key bindings in this group.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ordered_member_keys.len()
    }
}

impl fmt::Debug for GroupMemberKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupMemberKeys")
            .field("ordered_member_keys", &self.ordered_member_keys)
            .finish_non_exhaustive()
    }
}

impl PartialEq for GroupMemberKeys {
    fn eq(&self, other: &Self) -> bool {
        self.ordered_member_keys == other.ordered_member_keys
    }
}

impl Eq for GroupMemberKeys {}

/// Application-access lifecycle for one hosted replication group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationGroupLifecycle {
    /// The group accepts application reads, writes, and membership changes.
    Open,
    /// Local writes are disabled while accepted migration activation completes.
    ReadOnly {
        /// Accepted target group used to identify exact proposal replays.
        successor_group_id: GroupId,
        /// Immutable old-group replay cut carried by the accepted proposal.
        final_versions: VersionVector,
    },
    /// The group remains available only for bounded replication and catch-up.
    Closed {
        /// Accepted target group used to identify exact proposal replays.
        successor_group_id: GroupId,
        /// Immutable old-group replay cut carried by the accepted proposal.
        final_versions: VersionVector,
    },
}

impl ReplicationGroupLifecycle {
    /// Return whether application code may publish or change membership.
    #[must_use]
    pub const fn is_writable(&self) -> bool {
        matches!(self, Self::Open)
    }

    /// Return whether application code may read rows or request summaries.
    #[must_use]
    pub const fn is_readable(&self) -> bool {
        !matches!(self, Self::Closed { .. })
    }

    /// Return whether newly applied remote rows should reach the listener.
    #[must_use]
    pub const fn emits_data_changes(&self) -> bool {
        self.is_readable()
    }

    /// Return the immutable migration cut for a non-open group.
    #[must_use]
    pub const fn final_versions(&self) -> Option<&VersionVector> {
        match self {
            Self::Open => None,
            Self::ReadOnly { final_versions, .. } | Self::Closed { final_versions, .. } => {
                Some(final_versions)
            }
        }
    }

    /// Return the accepted successor for a non-open group.
    #[must_use]
    pub const fn successor_group_id(&self) -> Option<GroupId> {
        match self {
            Self::Open => None,
            Self::ReadOnly {
                successor_group_id, ..
            }
            | Self::Closed {
                successor_group_id, ..
            } => Some(*successor_group_id),
        }
    }

    /// Bound one replication frontier by this lifecycle's migration cut.
    #[must_use]
    pub fn bound_versions(&self, versions: &VersionVector) -> VersionVector {
        match self.final_versions() {
            Some(final_versions) => versions.greatest_lower_bound(final_versions),
            None => versions.clone(),
        }
    }
}

/// One persisted replication group together with its local progress.
///
/// This is the group-level record that anchors dataset snapshots and persisted
/// replication updates in the store. The canonical member order is significant
/// because it defines the stable `MemberIndex` values used in version vectors
/// and `UpdateId.node_index`. Sensitive group-security material is already
/// encrypted by the setup/runtime boundary before it enters the store.
#[derive(Clone, PartialEq, Eq)]
pub struct ReplicationGroupRecord {
    /// Stable replication-group identifier.
    pub group_id: GroupId,
    /// Canonical exact member-key order for the group.
    pub member_keys: GroupMemberKeys,
    /// Position of the local member within `member_keys`.
    pub local_member_index: MemberIndex,
    /// Dataset schemas fixed for the lifetime of this group.
    pub group_schema: GroupSchema,
    /// Last applied version vector stored for this group.
    pub version_vector: VersionVector,
    /// Current application-access and replication lifecycle.
    pub lifecycle: ReplicationGroupLifecycle,
    /// Already-encrypted group-security material needed by runtime operation.
    pub security_material: EncryptedGroupSecurityMaterial,
}

impl ReplicationGroupRecord {
    /// Split active progress from group material shared with pending setup.
    #[must_use]
    pub fn into_parts(self) -> (ReplicationGroupMaterialRecord, ReplicationGroupActiveState) {
        let material = ReplicationGroupMaterialRecord {
            group_id: self.group_id,
            member_keys: self.member_keys,
            local_member_index: self.local_member_index,
            group_schema: self.group_schema,
            security_material: self.security_material,
        };
        let active_state = ReplicationGroupActiveState {
            version_vector: self.version_vector,
            lifecycle: self.lifecycle,
        };
        (material, active_state)
    }

    /// Return the number of members encoded in this record.
    ///
    /// # Panics
    ///
    /// Panics if the record contains an empty member set.
    #[must_use]
    pub fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.member_keys.len())
            .expect("replication group records must not contain an empty member set")
    }

    /// Return member identities in canonical group order.
    #[must_use]
    pub fn member_ids(&self) -> impl ExactSizeIterator<Item = &MemberIdentity> + '_ {
        self.member_keys.member_ids()
    }

    /// Return the local member identity referenced by `local_member_index`.
    ///
    /// Store implementations must preserve the invariant that `local_member_index
    /// < member_keys.len()`.
    ///
    /// # Panics
    ///
    /// Panics if the stored local member index is outside the stored member-key
    /// order.
    #[must_use]
    pub fn local_member(&self) -> &MemberIdentity {
        &self
            .member_keys
            .member_key_at_index(self.local_member_index)
            .expect("replication group local member index must be in bounds")
            .member_id
    }

    /// Return whether another active record has the same group definition.
    ///
    /// Active progress and encrypted security material are intentionally not
    /// part of the definition comparison.
    #[must_use]
    pub fn matches_definition(&self, other: &Self) -> bool {
        self.group_id == other.group_id
            && self.member_keys == other.member_keys
            && self.local_member_index == other.local_member_index
            && self.group_schema == other.group_schema
    }

    /// Return whether this active group is compatible with stored material
    /// for the same group id.
    ///
    /// This intentionally ignores `version_vector`: active groups may already
    /// have applied local progress beyond the initial bootstrap state. All
    /// identity, schema, and security material must still match exactly.
    #[must_use]
    pub fn matches_group_material(&self, material: &ReplicationGroupMaterialRecord) -> bool {
        material.matches_definition(
            self.group_id,
            &self.member_keys,
            self.local_member_index,
            &self.group_schema,
        ) && self.security_material == material.security_material
    }
}

/// Persisted state that distinguishes active group material from inactive setup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationGroupActiveState {
    /// Last applied version vector stored for the active group.
    pub version_vector: VersionVector,
    /// Current application-access and replication lifecycle.
    pub lifecycle: ReplicationGroupLifecycle,
}

/// Stored group definition and local security material, independent of activation.
///
/// Material may exist before the group becomes externally active. Dataset rows,
/// update logs, and active progress remain anchored to [`ReplicationGroupRecord`]
/// through the store's separate active-group marker.
#[derive(Clone, PartialEq, Eq)]
pub struct ReplicationGroupMaterialRecord {
    /// Stable replication-group identifier.
    pub group_id: GroupId,
    /// Canonical exact member-key order for the group.
    pub member_keys: GroupMemberKeys,
    /// Position of the local member within `member_keys`.
    pub local_member_index: MemberIndex,
    /// Dataset schemas fixed for the lifetime of this group.
    pub group_schema: GroupSchema,
    /// Already-encrypted group-security material needed by runtime operation.
    pub security_material: EncryptedGroupSecurityMaterial,
}

impl ReplicationGroupMaterialRecord {
    /// Return whether this material has the supplied group definition.
    ///
    /// Encrypted security material is intentionally excluded so callers can
    /// reject structural conflicts before performing cryptographic checks.
    #[must_use]
    pub fn matches_definition(
        &self,
        group_id: GroupId,
        member_keys: &GroupMemberKeys,
        local_member_index: MemberIndex,
        group_schema: &GroupSchema,
    ) -> bool {
        self.group_id == group_id
            && self.member_keys == *member_keys
            && self.local_member_index == local_member_index
            && self.group_schema == *group_schema
    }

    /// Return the number of members encoded in this material.
    ///
    /// # Panics
    ///
    /// Panics if an invalid material record was constructed with no member keys.
    #[must_use]
    pub fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.member_keys.len())
            .expect("replication group material must not contain an empty member set")
    }

    /// Combine this material with active progress.
    #[must_use]
    pub fn activate(self, version_vector: VersionVector) -> ReplicationGroupRecord {
        ReplicationGroupRecord {
            group_id: self.group_id,
            member_keys: self.member_keys,
            local_member_index: self.local_member_index,
            group_schema: self.group_schema,
            version_vector,
            lifecycle: ReplicationGroupLifecycle::Open,
            security_material: self.security_material,
        }
    }
}

impl fmt::Debug for ReplicationGroupMaterialRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplicationGroupMaterialRecord")
            .field("group_id", &self.group_id)
            .field("member_keys", &self.member_keys)
            .field("local_member_index", &self.local_member_index)
            .field("group_schema", &self.group_schema)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ReplicationGroupRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationGroupRecord")
            .field("group_id", &self.group_id)
            .field("member_keys", &self.member_keys)
            .field("local_member_index", &self.local_member_index)
            .field("group_schema", &self.group_schema)
            .field("version_vector", &self.version_vector)
            .field("lifecycle", &self.lifecycle)
            .field("security_material", &self.security_material)
            .finish()
    }
}

/// Cryptographic setup version for one encrypted store secret.
///
/// Store implementations persist and compare the value opaquely. The
/// encryption boundary owns the mapping from version numbers to concrete
/// cryptographic setups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StoreSecretCryptoVersion(u16);

impl StoreSecretCryptoVersion {
    /// Build a store-secret crypto version from its wire/storage value.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Return the integer value stored in backend metadata columns.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

/// One already-encrypted secret cell stored by replication storage.
///
/// `key_id` identifies the device-local database secret used by the caller,
/// `crypto_version` identifies the encryption setup, `nonce` is opaque nonce
/// material for this cell, and `ciphertext` contains the encrypted payload
/// including the AEAD authentication tag.
#[derive(Clone, PartialEq, Eq)]
pub struct EncryptedStoreSecret {
    /// Cryptographic setup version for this encrypted cell.
    pub crypto_version: StoreSecretCryptoVersion,
    /// Generated id of the device-local database secret.
    pub key_id: StoreSecretKeyId,
    /// Opaque nonce bytes supplied by the encryption boundary.
    pub nonce: Box<[u8]>,
    /// Encrypted bytes including the AEAD authentication tag.
    pub ciphertext: Box<[u8]>,
}

impl fmt::Debug for EncryptedStoreSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedStoreSecret")
            .field("crypto_version", &self.crypto_version)
            .field("key_id", &self.key_id)
            .field("nonce_len", &self.nonce.len())
            .field("ciphertext_len", &self.ciphertext.len())
            .finish()
    }
}

impl EncryptedStoreSecret {
    /// Convert security-crate sealed bytes into the store's opaque encrypted-cell record.
    #[must_use]
    pub(crate) fn from_store_secret_ciphertext(
        key_id: StoreSecretKeyId,
        sealed: flotsync_security::StoreSecretCiphertext,
    ) -> Self {
        Self {
            crypto_version: STORE_SECRET_CRYPTO_V1,
            key_id,
            nonce: sealed.nonce.into(),
            ciphertext: sealed.ciphertext.into_boxed_slice(),
        }
    }
}

/// Build placeholder group-security material for the current security-storage slice.
///
/// This is a temporary bridge for `flotsync-sec.10`: group records need
/// encrypted security columns before replicated checklist setup provisions real
/// encrypted values from configuration. The returned bytes are not decryptable
/// production security material and this helper must be removed with that task.
#[doc(hidden)]
#[must_use]
pub fn current_slice_placeholder_group_security_material(
    group_id: GroupId,
) -> EncryptedGroupSecurityMaterial {
    current_slice_placeholder_group_security_material_with_key_id(
        group_id,
        StoreSecretKeyId::placeholder_for_current_slice(),
    )
}

/// Build placeholder group-security material with a caller-selected key id.
///
/// This exists only so temporary static group setup can satisfy loader metadata
/// validation until real group-secret provisioning lands.
#[doc(hidden)]
#[must_use]
pub fn current_slice_placeholder_group_security_material_with_key_id(
    group_id: GroupId,
    key_id: StoreSecretKeyId,
) -> EncryptedGroupSecurityMaterial {
    let seed = group_id.0.as_u128().to_le_bytes()[0];
    EncryptedGroupSecurityMaterial {
        encrypted_group_secret: EncryptedStoreSecret {
            crypto_version: STORE_SECRET_CRYPTO_V1,
            key_id,
            nonce: vec![seed; STORE_SECRET_CRYPTO_NONCE_LENGTH_V1].into_boxed_slice(),
            ciphertext: vec![seed, seed.wrapping_add(1), seed.wrapping_add(2)].into_boxed_slice(),
        },
    }
}

/// Encrypted group-security material persisted with one replication group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedGroupSecurityMaterial {
    /// Encrypted group secret payload for the replication group.
    pub encrypted_group_secret: EncryptedStoreSecret,
}

/// Encrypted local member private key material.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedLocalMemberPrivateKeys {
    /// Encrypted private key bundle for the local member.
    pub secret: EncryptedStoreSecret,
}

/// One stored encrypted local-private key bundle for a member identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalMemberPrivateKeysRecord {
    /// Member identity these private keys belong to.
    pub member_id: MemberIdentity,
    /// Already-encrypted local private key material.
    pub private_keys: EncryptedLocalMemberPrivateKeys,
}

/// Authority scope requested by one permission check.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AuthorityScope {
    /// Use member key material for active replication runtime traffic.
    ReplicationRuntime,
    /// Accept a bootstrap message from this member key for local group activation.
    BootstrapActivation,
    /// Publish a discovered route candidate for this member identity.
    MemberRoutePublication,
}

impl AuthorityScope {
    /// Authority scopes included in public key bundle assessment reports.
    pub const VALUES: [Self; 3] = [
        Self::ReplicationRuntime,
        Self::BootstrapActivation,
        Self::MemberRoutePublication,
    ];
}

impl fmt::Display for AuthorityScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplicationRuntime => f.write_str("replication runtime"),
            Self::BootstrapActivation => f.write_str("bootstrap activation"),
            Self::MemberRoutePublication => f.write_str("member route publication"),
        }
    }
}

/// Runtime policy that maps stored trust evidence to permission decisions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustPolicy {
    /// Evidence requirement for active replication runtime traffic.
    pub replication_runtime: MemberKeyTrustRequirement,
    /// Evidence requirement for accepting a bootstrap activation from a sender key.
    pub bootstrap_activation: MemberKeyTrustRequirement,
    /// Evidence requirement for publishing discovered routes for a member identity.
    pub member_route_publication: MemberKeyTrustRequirement,
}

impl Default for TrustPolicy {
    fn default() -> Self {
        Self {
            replication_runtime: MemberKeyTrustRequirement::LocalExplicitTrust,
            bootstrap_activation: MemberKeyTrustRequirement::LocalExplicitTrust,
            member_route_publication: MemberKeyTrustRequirement::StoredPublicKeyMaterial,
        }
    }
}

/// Evidence requirement for one member-key authority scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MemberKeyTrustRequirement {
    /// Require locally recorded explicit trust for the exact member key.
    LocalExplicitTrust,
    /// Require only that public key material is stored for the exact member key.
    StoredPublicKeyMaterial,
    /// Deny every permission request for this authority scope.
    DenyAll,
}

/// Result of evaluating whether one member key has a requested permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PermissionDecision {
    /// The key has the requested permission.
    Permit,
    /// The key does not have the requested permission.
    Deny(PermissionDenialReason),
}

impl PermissionDecision {
    /// Convert this permission decision into a plain result.
    ///
    /// Use this when the caller already knows the requested member key and authority scope and
    /// only needs to branch on permit versus denial.
    ///
    /// # Errors
    ///
    /// Returns the denial reason when this decision is [`Self::Deny`].
    pub const fn ok(self) -> Result<(), PermissionDenialReason> {
        match self {
            Self::Permit => Ok(()),
            Self::Deny(reason) => Err(reason),
        }
    }
}

impl fmt::Display for PermissionDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Permit => f.write_str("permit"),
            Self::Deny(reason) => write!(f, "deny ({reason})"),
        }
    }
}

/// Reason one member key permission request was denied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PermissionDenialReason {
    /// No stored public key material exists for the exact member key.
    MissingKeyMaterial,
    /// Stored evidence does not satisfy the policy for the requested authority.
    MissingTrustEvidence,
    /// The fingerprint is globally blocked.
    FingerprintBlocked,
    /// The active policy denies the requested authority scope.
    PolicyDenied,
}

impl fmt::Display for PermissionDenialReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingKeyMaterial => f.write_str("missing key material"),
            Self::MissingTrustEvidence => f.write_str("missing trust evidence"),
            Self::FingerprintBlocked => f.write_str("fingerprint blocked"),
            Self::PolicyDenied => f.write_str("policy denied"),
        }
    }
}

/// Identity of one exact member-key binding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemberKeyId {
    /// Member identity bound to the key material.
    pub member_id: MemberIdentity,
    /// Fingerprint derived from the bound public key material.
    pub fingerprint: KeyFingerprint,
}

/// Public key material observed for one exact member-key binding.
#[derive(Clone, PartialEq, Eq)]
pub struct MemberPublicKeysRecord {
    /// Exact member-key binding for this material.
    pub key_id: MemberKeyId,
    /// Opaque signing public key bytes.
    pub signing_public_key: Box<[u8]>,
    /// Opaque encryption public key bytes.
    pub encryption_public_key: Box<[u8]>,
}

impl MemberPublicKeysRecord {
    /// Build a store record from typed public member keys.
    #[must_use]
    pub fn from_public_keys(public_keys: &PublicMemberKeys) -> Self {
        Self {
            key_id: MemberKeyId {
                member_id: public_keys.member_id().clone(),
                fingerprint: public_keys.fingerprint(),
            },
            signing_public_key: public_keys.signing_key_bytes().into(),
            encryption_public_key: public_keys.encryption_key_bytes().into(),
        }
    }
}

impl fmt::Debug for MemberPublicKeysRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alternate = f.alternate();
        let mut debug = f.debug_struct("MemberPublicKeysRecord");
        debug.field("key_id", &self.key_id);
        if alternate {
            let signing_public_key = URL_SAFE_NO_PAD.encode(&self.signing_public_key);
            let encryption_public_key = URL_SAFE_NO_PAD.encode(&self.encryption_public_key);
            debug
                .field("signing_public_key", &signing_public_key)
                .field("encryption_public_key", &encryption_public_key)
                .finish()
        } else {
            debug
                .field("signing_public_key_len", &self.signing_public_key.len())
                .field(
                    "encryption_public_key_len",
                    &self.encryption_public_key.len(),
                )
                .finish()
        }
    }
}

/// Trust evidence recorded for one exact member-key binding.
#[derive(Debug, EnumSetType, Hash)]
pub enum MemberKeyTrustEvidenceKind {
    /// This store locally trusts the exact member-key binding.
    LocalExplicitTrust,
}

impl MemberKeyTrustEvidenceKind {
    /// Store representation for this evidence kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalExplicitTrust => "local_explicit_trust",
        }
    }
}

impl FromStr for MemberKeyTrustEvidenceKind {
    type Err = MemberKeyTrustEvidenceKindParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "local_explicit_trust" => Ok(Self::LocalExplicitTrust),
            _ => Err(MemberKeyTrustEvidenceKindParseError {
                value: input.to_owned(),
            }),
        }
    }
}

/// Error raised while parsing a stored member-key trust evidence kind.
#[derive(Clone, Debug, PartialEq, Eq, Snafu)]
#[snafu(display("member-key trust evidence kind '{value}' is invalid"))]
pub struct MemberKeyTrustEvidenceKindParseError {
    /// Raw evidence-kind value read from storage.
    value: String,
}

/// One stored evidence record for a member-key binding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemberKeyTrustEvidenceRecord {
    /// Exact member-key binding the evidence describes.
    pub key_id: MemberKeyId,
    /// Kind of evidence recorded for that binding.
    pub evidence_kind: MemberKeyTrustEvidenceKind,
}

/// Evidence set loaded for one exact member-key binding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MemberKeyTrustEvidenceSet {
    /// In-memory finite-domain set of evidence kinds observed for one exact binding.
    evidence_kinds: EnumSet<MemberKeyTrustEvidenceKind>,
}

impl MemberKeyTrustEvidenceSet {
    /// Build an empty evidence set.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            evidence_kinds: EnumSet::new(),
        }
    }

    /// Add one evidence kind to the set.
    pub fn insert(&mut self, evidence_kind: MemberKeyTrustEvidenceKind) {
        self.evidence_kinds.insert(evidence_kind);
    }

    /// Return whether the set contains the requested evidence kind.
    #[must_use]
    pub fn contains(self, evidence_kind: MemberKeyTrustEvidenceKind) -> bool {
        self.evidence_kinds.contains(evidence_kind)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::docs_group_schema;

    fn member_key_id<const N: usize>(segments: [&str; N], fingerprint_seed: u8) -> MemberKeyId {
        MemberKeyId {
            member_id: MemberIdentity::from_array(segments),
            fingerprint: KeyFingerprint::from_bytes([fingerprint_seed; 32]),
        }
    }

    fn member_public_keys_record() -> MemberPublicKeysRecord {
        MemberPublicKeysRecord {
            key_id: MemberKeyId {
                member_id: MemberIdentity::from_array(["debug", "alice"]),
                fingerprint: KeyFingerprint::from_bytes([9_u8; 32]),
            },
            signing_public_key: Box::from([1_u8, 2, 3]),
            encryption_public_key: Box::from([4_u8, 5, 6]),
        }
    }

    #[test]
    fn policy_decision_order_matches_restrictiveness() {
        let decisions = [
            PolicyDecision::AutoAccept,
            PolicyDecision::AskListener,
            PolicyDecision::AutoReject,
        ];
        for expected_index in 0..decisions.len() {
            let expected = decisions[expected_index];
            for other in decisions.iter().copied().take(expected_index + 1) {
                assert_eq!(expected.most_restrictive(other), expected);
                assert_eq!(other.most_restrictive(expected), expected);
            }
        }
    }

    #[test]
    fn group_policy_defaults_require_mediation_for_top_level_membership_changes() {
        assert_eq!(
            GroupInvitationPolicy::default(),
            GroupInvitationPolicy {
                creation: PolicyDecision::AskListener,
                migration_added_member: PolicyDecision::AskListener,
            }
        );
        assert_eq!(
            GroupMigrationPolicy::default(),
            GroupMigrationPolicy {
                epoch_change: PolicyDecision::AutoAccept,
                member_added: PolicyDecision::AskListener,
                member_device_added: PolicyDecision::AutoAccept,
                member_removed: PolicyDecision::AskListener,
                member_device_removed: PolicyDecision::AutoAccept,
                local_member_removed: PolicyDecision::AskListener,
            }
        );
    }

    #[test]
    fn group_member_keys_preserve_order_indices_and_exact_keys() {
        let alice_key = member_key_id(["debug", "alice"], 1);
        let bob_key = member_key_id(["debug", "bob"], 2);

        let member_keys =
            GroupMemberKeys::from_ordered_member_keys([alice_key.clone(), bob_key.clone()])
                .expect("group member keys should build");

        assert_eq!(
            member_keys.member_ids().cloned().collect::<Vec<_>>(),
            vec![alice_key.member_id.clone(), bob_key.member_id.clone()]
        );
        assert_eq!(
            member_keys.member_index(&alice_key.member_id),
            Some(MemberIndex::new(0))
        );
        assert_eq!(
            member_keys.member_index(&bob_key.member_id),
            Some(MemberIndex::new(1))
        );
        assert_eq!(
            member_keys.member_key(&alice_key.member_id),
            Some(&alice_key)
        );
        assert_eq!(
            member_keys.member_key_at_index(MemberIndex::new(1)),
            Some(&bob_key)
        );
        assert_eq!(
            member_keys
                .to_group_members()
                .expect("identity group view should build")
                .ordered_members(),
            vec![alice_key.member_id, bob_key.member_id]
        );
    }

    #[test]
    fn group_member_keys_reject_duplicate_member_identities() {
        let first_key = member_key_id(["debug", "alice"], 1);
        let second_key = member_key_id(["debug", "alice"], 2);

        let error = GroupMemberKeys::from_ordered_member_keys([first_key, second_key])
            .expect_err("duplicate member identity should be rejected");

        assert!(matches!(error, GroupMembersError::DuplicateMember { .. }));
    }

    #[test]
    fn group_material_definition_matching_excludes_security_material() {
        let group_id = GroupId(uuid::Uuid::from_u128(91_000));
        let member_keys = GroupMemberKeys::from_ordered_member_keys([
            member_key_id(["debug", "alice"], 1),
            member_key_id(["debug", "bob"], 2),
        ])
        .expect("group member keys should build");
        let group_schema = docs_group_schema();
        let material = ReplicationGroupMaterialRecord {
            group_id,
            member_keys: member_keys.clone(),
            local_member_index: MemberIndex::new(0),
            group_schema: group_schema.clone(),
            security_material: current_slice_placeholder_group_security_material(group_id),
        };
        let member_count = NonZeroUsize::new(2).expect("test group has members");
        let active = material
            .clone()
            .activate(VersionVector::initial(member_count));
        let mut different_security = material.clone();
        different_security.security_material = current_slice_placeholder_group_security_material(
            GroupId(uuid::Uuid::from_u128(91_098)),
        );
        let different_security_active = different_security
            .clone()
            .activate(VersionVector::initial(member_count));

        assert!(material.matches_definition(
            group_id,
            &member_keys,
            MemberIndex::new(0),
            &group_schema,
        ));
        assert!(active.matches_definition(&different_security_active));
        assert!(!active.matches_group_material(&different_security));
        assert!(!material.matches_definition(
            GroupId(uuid::Uuid::from_u128(91_099)),
            &member_keys,
            MemberIndex::new(0),
            &group_schema,
        ));
    }

    #[test]
    fn active_group_decomposition_preserves_progress_and_lifecycle() {
        let group_id = GroupId(uuid::Uuid::from_u128(91_100));
        let successor_group_id = GroupId(uuid::Uuid::from_u128(91_101));
        let member_count = NonZeroUsize::new(1).expect("test group has one member");
        let mut versions = VersionVector::initial(member_count);
        versions.increment_at(0);
        let lifecycle = ReplicationGroupLifecycle::ReadOnly {
            successor_group_id,
            final_versions: versions.clone(),
        };
        let group = ReplicationGroupRecord {
            group_id,
            member_keys: GroupMemberKeys::from_ordered_member_keys([member_key_id(
                ["active-state", "alice"],
                1,
            )])
            .expect("test group member keys should build"),
            local_member_index: MemberIndex::new(0),
            group_schema: GroupSchema::default(),
            version_vector: versions.clone(),
            lifecycle: lifecycle.clone(),
            security_material: current_slice_placeholder_group_security_material(group_id),
        };

        let (_, active_state) = group.into_parts();

        assert_eq!(active_state.version_vector, versions);
        assert_eq!(active_state.lifecycle, lifecycle);
    }

    #[test]
    fn group_invitation_rejects_mismatched_migration_group_id() {
        let old_group_id = GroupId(uuid::Uuid::from_u128(91_001));
        let new_group_id = GroupId(uuid::Uuid::from_u128(91_002));
        let wrong_group_id = GroupId(uuid::Uuid::from_u128(91_003));

        let error = GroupInvitation::try_new(
            wrong_group_id,
            GroupInvitationSource::Migration {
                migration_id: MigrationId {
                    old_group_id,
                    new_group_id,
                },
            },
            Vec::new(),
            GroupSchema::default(),
            InitialSnapshot::Empty,
            None,
            None,
        )
        .expect_err("mismatched migration invitation group id should be rejected");

        assert!(matches!(
            error,
            GroupInvitationError::GroupMismatch {
                group_id,
                new_group_id: actual_new_group_id,
            } if group_id == wrong_group_id && actual_new_group_id == new_group_id
        ));
    }

    #[test]
    fn group_schema_alternate_debug_lists_datasets() {
        let group_schema = docs_group_schema();

        let default_output = format!("{group_schema:?}");
        let alternate_output = format!("{group_schema:#?}");

        assert!(default_output.contains("dataset_count"));
        assert!(!default_output.contains("DatasetSchema"));
        assert!(alternate_output.contains("DatasetSchema"));
        assert!(alternate_output.contains("docs"));
    }

    #[test]
    fn member_public_keys_debug_prints_lengths_by_default() {
        let output = format!("{:?}", member_public_keys_record());

        assert_eq!(
            output,
            r#"MemberPublicKeysRecord { key_id: MemberKeyId { member_id: Identifier(i"debug", i"alice"), fingerprint: KeyFingerprint("CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQk") }, signing_public_key_len: 3, encryption_public_key_len: 3 }"#,
        );
    }

    #[test]
    fn member_public_keys_alternate_debug_prints_base64url() {
        let output = format!("{:#?}", member_public_keys_record());

        assert_eq!(
            output,
            r#"MemberPublicKeysRecord {
    key_id: MemberKeyId {
        member_id: Identifier(i"debug", i"alice"),
        fingerprint: KeyFingerprint(
            "CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQkJ-CQk",
        ),
    },
    signing_public_key: "AQID",
    encryption_public_key: "BAUG",
}"#,
        );
    }
}

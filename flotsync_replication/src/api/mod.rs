use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use flotsync_core::{
    GroupId,
    MemberIdentity,
    MemberIndex,
    member::Identifier,
    versions::{UpdateId, VersionVector},
};
use flotsync_data_types::schema::datamodel::{NullableBasicValue, RowSnapshot};
use flotsync_security::{
    StoreSecretKey,
    load_local_store_secret,
    load_or_create_local_store_secret,
};
use flotsync_utils::BoxFuture;
use smallvec::{Array, SmallVec};
use snafu::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    fmt,
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
pub use flotsync_security::{LocalStoreSecretProfile, StoreSecretKeyId};
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
    #[must_use]
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
        row: Arc<dyn RowRead<UpdateId> + Send + Sync>,
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
    pub fn row(&self) -> Option<&(dyn RowRead<UpdateId> + Send + Sync)> {
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
    pub read_token: ReadToken,
    pub rows: Box<SnapshotRowProvider>,
}

impl std::fmt::Debug for SnapshotRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotRows")
            .field("group_id", &self.group_id)
            .field("read_token", &self.read_token)
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
/// Snapshot providers may hold a store read transaction internally, so callers
/// should drain or drop them promptly.
pub type SnapshotRowProvider = dyn BatchProvider<Batch = SnapshotRowBatch>;

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

/// One row entry in an initial dataset state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialRowState {
    pub row_key: RowKey,
    pub row: MutableRow,
}

/// Batch type used by [`InitialRowProvider`].
pub type InitialRowBatch = Vec<InitialRowState>;

/// Source for one dataset's initial rows in `InitialGroupState`.
pub type InitialRowProvider = dyn BatchProvider<Batch = InitialRowBatch>;

/// Initial state rows for one dataset.
pub struct InitialDatasetState {
    pub dataset_id: DatasetId,
    pub rows: Box<InitialRowProvider>,
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishReceipt {
    pub update_id: UpdateId,
    pub read_token: ReadToken,
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
    /// Publish one local set of row mutations from a known read token.
    ///
    /// The request token is the read position of the application state used to
    /// decide the mutation list. Mutations are interpreted as sparse field
    /// patches for each [`RowMutation::Upsert`]:
    /// fields omitted from the submitted [`MutableRow`] are intentionally left
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

    /// Open a batched stream over the latest locally stored rows for selected datasets.
    ///
    /// The request is scoped to one replication group and an explicit set of
    /// application datasets. The stream reflects the latest state known to the
    /// local store when the snapshot is opened; it does not wait for remote peers
    /// and it does not perform catch-up. When `include_tombstones` is false, the
    /// provider should emit only application-visible rows. When it is true,
    /// retained delete tombstones are emitted as [`SnapshotRow`] values with
    /// [`SnapshotRow::deleted`] set.
    ///
    /// The returned [`SnapshotRows`] provider may hold a store read transaction
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
    ) -> BoxFuture<'_, Result<SnapshotRows, ApiError>>;

    /// Ask one group member for its current group version vector.
    fn request_summary(&self, request: SummaryRequest) -> BoxFuture<'_, Result<Summary, ApiError>>;

    /// Create one new fixed-membership replication group rooted at this member.
    ///
    /// `req.members` defines the canonical member order for the new group and
    /// must include the local member. On success, the runtime stores the
    /// group record, installs it in the live runtime view, broadcasts bootstrap
    /// messages to the configured remote members, and returns the newly allocated
    /// [`GroupId`].
    ///
    /// Initial dataset state in [`CreateGroupRequest::initial_state`] is part of
    /// the public request shape but is not implemented in the current manual
    /// replication slice; requests that include it fail with [`ApiError`].
    ///
    /// The method returns [`ApiError`] when membership validation fails, the
    /// runtime is unavailable, group storage fails, or unsupported
    /// request features are used.
    fn create_group(&self, req: CreateGroupRequest) -> BoxFuture<'_, Result<GroupId, ApiError>>;

    /// Request a membership migration for an existing replication group.
    ///
    /// The intended contract is to propose adding and removing members for
    /// `req.group_id`, persist the accepted migration, and surface any
    /// application-mediated invitations through [`ReplicationEvent::GroupInvitation`].
    /// Ordering and compatibility rules are governed by [`ReplicationConfig`]
    /// and [`GroupMigrationPolicy`].
    ///
    /// Membership migration is not implemented in the current manual replication
    /// slice. Current implementations return [`ApiError::UnsupportedOperation`]
    /// without mutating group state.
    fn change_group_membership(
        &self,
        req: ChangeGroupMembershipRequest,
    ) -> BoxFuture<'_, Result<GroupMigration, ApiError>>;
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

/// One persisted replication group together with its local progress.
///
/// This is the group-level record that anchors dataset snapshots and persisted
/// replication updates in the store. The canonical member order is significant
/// because it defines the stable `MemberIndex` values used in version vectors
/// and `UpdateId.node_index`. Sensitive group-security material is already
/// encrypted by the setup/runtime boundary before it enters the store.
#[derive(Clone)]
pub struct ReplicationGroupRecord {
    /// Stable replication-group identifier.
    pub group_id: GroupId,
    /// Canonical member order for the group.
    pub members: Vec<MemberIdentity>,
    /// Position of the local member within `members`.
    pub local_member_index: MemberIndex,
    /// Last applied version vector stored for this group.
    pub version_vector: VersionVector,
    /// Already-encrypted group-security material needed by runtime operation.
    pub security_material: EncryptedGroupSecurityMaterial,
}

impl ReplicationGroupRecord {
    /// Return the number of members encoded in this record.
    ///
    /// # Panics
    ///
    /// Panics if the record contains an empty member set.
    #[must_use]
    pub fn member_count(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.members.len())
            .expect("replication group records must not contain an empty member set")
    }

    /// Return the local member identity referenced by `local_member_index`.
    ///
    /// Store implementations must preserve the invariant that
    /// `local_member_index < members.len()`.
    #[must_use]
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

/// Trusted public key material for one member identity.
#[derive(Clone, PartialEq, Eq)]
pub struct TrustedMemberPublicKeysRecord {
    /// Member identity these public keys belong to.
    pub member_id: MemberIdentity,
    /// Opaque signing public key bytes.
    pub signing_public_key: Box<[u8]>,
    /// Opaque encryption public key bytes.
    pub encryption_public_key: Box<[u8]>,
}

impl fmt::Debug for TrustedMemberPublicKeysRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let alternate = f.alternate();
        let mut debug = f.debug_struct("TrustedMemberPublicKeysRecord");
        debug.field("member_id", &self.member_id);
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
    /// Whether this dataset already exists in the store for `group_id`.
    pub dataset_exists: bool,
    /// Stored state for each requested row key.
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

/// Row image loaded from or written to replication storage.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplicationRowRecord {
    /// Stable row key in the dataset that owns this record.
    pub row_id: RowKey,
    /// Complete value snapshot for the row.
    pub snapshot: ReplicationRowSnapshot,
    /// Whether the row is deleted but still retained for causal updates.
    pub tombstoned: bool,
    /// Causal version of the last update that changed this row image.
    pub last_changed_versions: VersionVector,
}

/// One explicit transactional row patch for a dataset.
#[derive(Clone, Debug, PartialEq)]
pub struct DatasetRowPatch {
    /// Replication group that owns this dataset patch.
    pub group_id: GroupId,
    /// Dataset identifier within the replication group.
    pub dataset_id: DatasetId,
    /// Ordered row-level writes to apply transactionally.
    pub actions: Vec<DatasetRowWrite>,
    /// Causal version to store as the last change for every row in `actions`.
    pub last_changed_versions: VersionVector,
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

    /// Load encrypted local-private key material for one member identity.
    fn load_local_member_private_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<LocalMemberPrivateKeysRecord>, StoreError>>;

    /// Load trusted public key material for one member identity.
    fn load_trusted_member_public_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<TrustedMemberPublicKeysRecord>, StoreError>>;

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
pub trait ReplicationStoreTransaction: Send {
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

    /// Insert one new persisted replication group.
    fn insert_replication_group(
        &mut self,
        group: ReplicationGroupRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Load encrypted local-private key material for one member identity.
    fn load_local_member_private_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<LocalMemberPrivateKeysRecord>, StoreError>>;

    /// Insert encrypted local-private key material or confirm it is already stored unchanged.
    fn ensure_local_member_private_keys(
        &mut self,
        record: LocalMemberPrivateKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Load trusted public key material for one member identity.
    fn load_trusted_member_public_keys<'a>(
        &'a mut self,
        member_id: &'a MemberIdentity,
    ) -> BoxFuture<'a, Result<Option<TrustedMemberPublicKeysRecord>, StoreError>>;

    /// Insert trusted public key material or confirm it is already stored unchanged.
    fn ensure_trusted_member_public_keys(
        &mut self,
        record: TrustedMemberPublicKeysRecord,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Advance the stored applied version vector for one existing replication group.
    fn update_replication_group_version_vector<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        version_vector: VersionVector,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Load the stored state for the requested dataset row keys.
    ///
    /// Implementations must include every iterated `row_key` exactly once in
    /// `DatasetRowSlice.rows`.
    fn load_dataset_rows<'a>(
        &'a mut self,
        group_id: &'a GroupId,
        dataset_id: &'a DatasetId,
        row_keys: &'a mut RowKeyIterator<'a>,
    ) -> BoxFuture<'a, Result<DatasetRowSlice, StoreError>>;

    /// Apply one explicit set of row-level dataset storage actions.
    fn apply_dataset_row_patch(
        &mut self,
        patch: DatasetRowPatch,
    ) -> BoxFuture<'_, Result<(), StoreError>>;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn trusted_public_keys_record() -> TrustedMemberPublicKeysRecord {
        TrustedMemberPublicKeysRecord {
            member_id: MemberIdentity::from_array(["debug", "alice"]),
            signing_public_key: Box::from([1_u8, 2, 3]),
            encryption_public_key: Box::from([4_u8, 5, 6]),
        }
    }

    #[test]
    fn trusted_public_keys_debug_prints_lengths_by_default() {
        let output = format!("{:?}", trusted_public_keys_record());

        assert_eq!(
            output,
            r#"TrustedMemberPublicKeysRecord { member_id: Identifier(i"debug", i"alice"), signing_public_key_len: 3, encryption_public_key_len: 3 }"#,
        );
    }

    #[test]
    fn trusted_public_keys_alternate_debug_prints_base64url() {
        let output = format!("{:#?}", trusted_public_keys_record());

        assert_eq!(
            output,
            r#"TrustedMemberPublicKeysRecord {
    member_id: Identifier(i"debug", i"alice"),
    signing_public_key: "AQID",
    encryption_public_key: "BAUG",
}"#,
        );
    }
}
